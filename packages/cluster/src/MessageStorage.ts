/**
 * @since 1.0.0
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type { WithResult } from "effect/Schema"
import type { Envelope, EnvelopeWithContext } from "./Envelope.js"
import type { MessageState } from "./MessageState.js"
import type { MessagePersistenceError } from "./ShardingError.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface Service {
  /**
   * Save the provided message and its associated metadata.
   */
  readonly save: <Version extends string, Msg extends Envelope.AnyMessage>(
    envelope: EnvelopeWithContext<Version, Msg>
  ) => Effect.Effect<void, MessagePersistenceError>

  /**
   * Updates the specified message using the provided `MessageState`.
   */
  readonly update: <Version extends string, Msg extends Envelope.AnyMessage>(
    envelope: Envelope<Version, Msg>,
    state: MessageState<WithResult.Success<Msg>, WithResult.Failure<Msg>>
  ) => Effect.Effect<void>

  /**
   * Retrieves the unprocessed messages for the specified entity and shard.
   */
  // readonly unprocessed: <Msg extends Envelope.AnyMessage>(
  //   entity: Entity<Msg>,
  //   shardId: ShardId
  // ) => Effect.Effect<Array<Envelope<Msg>>>
}

/**
 * @since 1.0.0
 * @category context
 */
export class MessageStorage extends Context.Tag("@effect/cluster/MessageStorage")<MessageStorage, Service>() {}

/**
 * @since 1.0.0
 * @category layers
 */
export const layerNoop: Layer.Layer<MessageStorage> = Layer.succeed(
  MessageStorage,
  MessageStorage.of({
    save: () => Effect.void,
    update: () => Effect.void
  })
)
