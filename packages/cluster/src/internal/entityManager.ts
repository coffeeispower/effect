import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Mailbox from "effect/Mailbox"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import { hasProperty } from "effect/Predicate"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as ClusterSchema from "../ClusterSchema.js"
import { type Entity, makeEnvelopeSchema } from "../Entity.js"
import type { EntityAddress } from "../EntityAddress.js"
import type { Envelope } from "../Envelope.js"
import type { Sharding } from "../Sharding.js"
import type { ShardingConfig } from "../ShardingConfig.js"
import { EntityNotManagedByPod, MalformedMessage } from "../ShardingError.js"
import * as InternalMetrics from "./metrics.js"
import * as InternalShardingCircular from "./sharding/circular.js"
import * as InternalShardingConfig from "./shardingConfig.js"

/** @internal */
export interface EntityManager {
  readonly send: <Version extends string, Msg extends Envelope.AnyMessage>(
    envelope: Envelope<Version, Msg>
  ) => Effect.Effect<Exit.Exit<Schema.WithResult.Success<Msg>, Schema.WithResult.Failure<Msg>>, EntityNotManagedByPod>

  readonly sendPartial: (
    envelope: Envelope.PartialEncoded
  ) => Effect.Effect<Schema.ExitEncoded<unknown, unknown, unknown>, EntityNotManagedByPod | MalformedMessage>
}

/** @internal */
export const make: <Protocol extends Entity.AnyProtocol>(
  entity: Entity<Protocol>,
  behavior: Entity.Behavior<Protocol>,
  options?: Sharding.RegistrationOptions
) => Effect.Effect<
  EntityManager,
  never,
  | Scope.Scope
  | Entity.ProtocolContext<Protocol>
  | Sharding
  | ShardingConfig
> = Effect.fnUntraced(function*<Protocol extends Entity.AnyProtocol>(
  entity: Entity<Protocol>,
  behavior: Entity.Behavior<Protocol>,
  options?: Sharding.RegistrationOptions
) {
  const config = yield* InternalShardingConfig.Tag
  const sharding = yield* InternalShardingCircular.Tag
  const context = yield* Effect.context<Entity.ProtocolContext<Protocol>>()
  const gauge = InternalMetrics.entities.pipe(Metric.tagged("type", entity.type))

  // Represents the entities managed by this entity manager
  const entities: RcMap.RcMap<
    EntityAddress,
    Mailbox.Mailbox<Entity.ProtocolEnvelope<Protocol>>,
    EntityNotManagedByPod
  > = yield* RcMap.make({
    idleTimeToLive: options?.maxIdleTime ?? config.entityMaxIdleTime,
    lookup: Effect.fnUntraced(function*(address) {
      if (yield* sharding.isShutdown) {
        return yield* new EntityNotManagedByPod({ address })
      }
      const scope = yield* Effect.scope

      // Create the mailbox for the entity
      const mailbox = yield* Mailbox.make<Entity.ProtocolEnvelope<Protocol>>()

      // Initiate the behavior for the entity
      const fiber = yield* behavior(mailbox, replier).pipe(
        // ensure that the rcmap scope is not leaked to the behavior
        Effect.mapInputContext<Scope.Scope, never>(Context.omit(Scope.Scope)),
        Effect.ensuring(RcMap.invalidate(entities, address)),
        Effect.forkDaemon
      )

      // During shutdown, signal that no more messages will be processed
      // and wait for the fiber to complete.
      //
      // If the termination timeout is reached, completely **DESTROY** the behavior.
      yield* Scope.addFinalizer(
        scope,
        mailbox.end.pipe(
          Effect.andThen(Fiber.await(fiber)),
          Effect.timeoutOption(config.entityTerminationTimeout),
          Effect.flatMap(Option.match({
            onNone: () => Fiber.interrupt(fiber),
            onSome: () => Effect.void
          }))
        )
      )

      // Perform metric bookkeeping
      yield* Metric.increment(gauge)
      yield* Scope.addFinalizer(scope, Metric.incrementBy(gauge, BigInt(-1)))

      return mailbox
    })
  })

  const messageToEnvelope = new WeakMap<Envelope.AnyMessage, {
    readonly envelope: Envelope.Any
    readonly resume: (result: Effect.Effect<Exit.Exit<any, any>>) => void
  }>()

  function replyDone<Msg extends Envelope.AnyMessage>(
    message: Msg,
    result: Exit.Exit<
      Schema.WithResult.Success<Msg>,
      Schema.WithResult.Failure<Msg>
    >
  ) {
    return Effect.suspend(() => {
      const entry = messageToEnvelope.get(message)
      if (entry === undefined) return Effect.void
      entry.resume(Effect.succeed(result))
      messageToEnvelope.delete(message)
      return RcMap.touch(entities, entry.envelope.address)
    })
  }

  const replier: Entity.Replier = {
    succeed: (message, value) => replyDone(message, Exit.succeed(value)),
    fail: (message, error) => replyDone(message, Exit.fail(error)),
    failCause: (message, cause) => replyDone(message, Exit.failCause(cause)),
    done: replyDone
  }

  // TODO: For stream messages, keep the address scope open until the stream
  // is done. Add Stream.ensuring to the stream?
  function send<Version extends string, Msg extends Envelope.AnyMessage>(
    envelope: Envelope<Version, Msg>
  ): Effect.Effect<Exit.Exit<Schema.WithResult.Success<Msg>, Schema.WithResult.Failure<Msg>>, EntityNotManagedByPod> {
    const isStream = ClusterSchema.isStreamSerializable(envelope.message)

    return RcMap.get(entities, envelope.address).pipe(
      Effect.flatMap((mailbox) =>
        Effect.async<Exit.Exit<Schema.WithResult.Success<Msg>, Schema.WithResult.Failure<Msg>>>((resume) => {
          messageToEnvelope.set(envelope.message, { envelope, resume })
          mailbox.unsafeOffer(envelope as any)
          return Effect.sync(() => {
            messageToEnvelope.delete(envelope.message)
          })
        })
      ),
      Effect.scoped
    )
  }

  const envelopeSchema = makeEnvelopeSchema(entity)
  const decodeEnvelope = Schema.decode(envelopeSchema)
  const sendPartial: EntityManager["sendPartial"] = (envelope) =>
    decodeEnvelope(envelope).pipe(
      Effect.provide(context),
      Effect.mapError((cause) => new MalformedMessage({ cause })),
      Effect.flatMap(send)
    )

  return { send, sendPartial } as const
})
