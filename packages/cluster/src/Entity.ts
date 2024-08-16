/**
 * @since 1.0.0
 */
import type { Cause } from "effect/Cause"
import type { Effect } from "effect/Effect"
import * as Equal from "effect/Equal"
import type { Exit } from "effect/Exit"
import { identity } from "effect/Function"
import * as Hash from "effect/Hash"
import type { ReadonlyMailbox } from "effect/Mailbox"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import { EntityType } from "./EntityType.js"
import * as Envelope from "./Envelope.js"

const SymbolKey = "@effect/cluster/Entity"

/**
 * @since 1.0.0
 * @category type ids
 */
export const TypeId: unique symbol = Symbol.for(SymbolKey)

/**
 * @since 1.0.0
 * @category type ids
 */
export type TypeId = typeof TypeId

/**
 * @since 1.0.0
 * @category models
 */
export interface Entity<Protocol extends Entity.AnyProtocol> extends Equal.Equal {
  readonly [TypeId]: TypeId
  /**
   * The name of the entity type.
   */
  readonly type: EntityType
  /**
   * A schema definition for messages which represents the messaging protocol
   * that the entity is capable of processing.
   */
  readonly protocol: Protocol
}

/**
 * @since 1.0.0
 */
export declare namespace Entity {
  /**
   * @since 1.0.0
   * @category models
   */
  export type Any = Entity<AnyProtocol>

  /**
   * @since 1.0.0
   * @category models
   */
  export type AnyProtocol = Record<string, ReadonlyArray<Envelope.Envelope.AnyMessageSchema>>

  /**
   * @since 1.0.0
   * @category models
   */
  export type Envelopes<Entity extends Any> = keyof Entity["protocol"] extends infer Version ?
    Version extends keyof Entity["protocol"] & string ?
      Envelope.Envelope<Version, Entity["protocol"][Version][number]["Type"]>
    : never :
    never

  /**
   * @since 1.0.0
   * @category models
   */
  export type ProtocolEnvelope<Protocol extends AnyProtocol> = keyof Protocol extends infer Version ?
    Version extends (keyof Protocol) & string ? Envelope.Envelope<Version, Protocol[Version][number]["Type"]>
    : never :
    never

  /**
   * @since 1.0.0
   * @category models
   */
  export type ProtocolContext<Protocol extends AnyProtocol> = Protocol[keyof Protocol][number] extends
    Schema.Schema<infer _A, infer _I, infer _R> ? _R : never

  /**
   * @since 1.0.0
   * @category models
   */
  export type GetBehavior<E extends Any> = E extends Entity<infer P> ? Behavior<P> : never

  /**
   * @since 1.0.0
   * @category models
   */
  export type Behavior<Protocol extends AnyProtocol> = (
    mailbox: ReadonlyMailbox<ProtocolEnvelope<Protocol>>,
    replier: Replier
  ) => Effect<void>

  /**
   * @since 1.0.0
   * @category models
   */
  export interface Replier {
    /**
     * Completes specified message with the provided value.
     */
    readonly succeed: <Msg extends Envelope.Envelope.AnyMessage>(
      message: Msg,
      value: Schema.WithResult.Success<Msg>
    ) => Effect<void, never, Schema.WithResult.Context<Msg>>
    /**
     * Completes specified message with the provided error.
     */
    readonly fail: <Msg extends Envelope.Envelope.AnyMessage>(
      message: Msg,
      error: Schema.WithResult.Failure<Msg>
    ) => Effect<void, never, Schema.WithResult.Context<Msg>>
    /**
     * Completes specified message with the provided `Cause`.
     */
    readonly failCause: <Msg extends Envelope.Envelope.AnyMessage>(
      message: Msg,
      cause: Cause<Schema.WithResult.Failure<Msg>>
    ) => Effect<void, never, Schema.WithResult.Context<Msg>>
    /**
     * Completes specified message with the provided `Exit`.
     */
    readonly done: <Msg extends Envelope.Envelope.AnyMessage>(
      message: Msg,
      result: Exit<Schema.WithResult.Success<Msg>, Schema.WithResult.Failure<Msg>>
    ) => Effect<void, never, Schema.WithResult.Context<Msg>>
  }
}

/**
 * @since 1.0.0
 * @category refinements
 */
export const isEntity = (u: unknown): u is Entity.Any => Predicate.hasProperty(u, TypeId)

const Proto = {
  [TypeId]: TypeId,
  [Hash.symbol](this: Entity<any>): number {
    return Hash.structure({ type: this.type })
  },
  [Equal.symbol](this: Entity<any>, that: Equal.Equal): boolean {
    return isEntity(that) && this.type === that.type
  }
}

/**
 * Creates a new `Entity` of the specified `type` which will accept messages
 * that adhere to the provided `schema`.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = <const Protocol extends Entity.AnyProtocol>(
  /**
   * The entity type name.
   */
  type: string,
  /**
   * The schema definition for messages that the entity is capable of
   * processing.
   */
  protocol: Protocol
): Entity<Protocol> => {
  const self = Object.create(Proto)
  self.type = EntityType.make(type)
  self.protocol = protocol
  return self
}

/**
 * @since 1.0.0
 * @category Schemas
 */
export const makeEnvelopeSchema = <Protocol extends Entity.AnyProtocol>(entity: Entity<Protocol>): Schema.Schema<
  Entity.ProtocolEnvelope<Protocol>,
  Envelope.Envelope.PartialEncoded,
  Entity.ProtocolContext<Protocol>
> =>
  Schema.transform(
    Schema.Union(
      ...Object.entries(entity.protocol).map(([version, messages]) =>
        Schema.Struct({
          ...Envelope.PartialEncodedFromSelf.fields,
          version: Schema.Literal(version),
          message: Schema.Union(...messages as any)
        })
      )
    ),
    Envelope.EnvelopeFromSelf,
    {
      decode(envelope) {
        return Envelope.make(envelope as any)
      },
      encode: identity
    }
  ) as any
