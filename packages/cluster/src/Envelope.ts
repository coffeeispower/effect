/**
 * @since 1.0.0
 */
import type { Context } from "effect/Context"
import * as Effect from "effect/Effect"
import { globalValue } from "effect/GlobalValue"
import type { ParseError } from "effect/ParseResult"
import * as Predicate from "effect/Predicate"
import type * as Request from "effect/Request"
import * as Schema from "effect/Schema"
import { EntityAddress, EntityAddressFromSelf } from "./EntityAddress.js"
import { type Snowflake, SnowflakeFromBigInt } from "./Snowflake.js"

const SymbolKey = "@effect/cluster/Envelope"

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
export interface Envelope<out Version extends string, in out Msg extends Envelope.AnyMessage> {
  readonly [TypeId]: TypeId
  readonly id: Snowflake
  readonly address: EntityAddress
  readonly version: Version
  readonly message: Msg
}

/**
 * @since 1.0.0
 * @category models
 */
export interface EnvelopeWithContext<out Version extends string, in out Msg extends Envelope.AnyMessage>
  extends Envelope<Version, Msg>
{
  readonly context: Context<Schema.Serializable.Context<Msg>>
  encodedCache?: Envelope.Encoded
}

/**
 * @since 1.0.0
 */
export declare namespace Envelope {
  /**
   * @since 1.0.0
   * @category models
   */
  export type Any = Envelope<string, any>

  /**
   * @since 1.0.0
   * @category models
   */
  export interface AnyMessage extends Schema.TaggedRequest<string, any, any, any, any, any, any, any, unknown> {
    [Request.RequestTypeId]: any
    [Schema.symbolSerializable]: any
    [Schema.symbolWithResult]: any
  }

  /**
   * @since 1.0.0
   * @category models
   */
  export interface AnyMessageSchema {
    readonly [Schema.TypeId]: any
    readonly Type: AnyMessage
  }

  /**
   * @since 1.0.0
   * @category models
   */
  export interface Encoded {
    readonly id: bigint
    readonly address: typeof EntityAddress.Encoded
    readonly version: string
    readonly message: unknown
  }

  /**
   * @since 1.0.0
   * @category models
   */
  export interface PartialEncoded {
    readonly id: Snowflake
    readonly address: EntityAddress
    readonly version: string
    readonly message: unknown
  }
}

const Proto = {
  [TypeId]: TypeId,
  address: undefined,
  message: undefined,
  version: undefined,
  context: undefined
}

/**
 * @since 1.0.0
 * @category refinements
 */
export const isEnvelope = (u: unknown): u is Envelope<string, Envelope.AnyMessage> => Predicate.hasProperty(u, TypeId)

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = <const Version extends string, Msg extends Envelope.AnyMessage>(
  options: {
    readonly id: Snowflake
    readonly address: EntityAddress
    readonly version: Version
    readonly message: Msg
  }
): Envelope<Version, Msg> => {
  const self = Object.create(Proto)
  self.id = options.id
  self.address = options.address
  self.version = options.version
  self.message = options.message
  return self
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const makeWithContext = <const Version extends string, Msg extends Envelope.AnyMessage>(
  options: {
    readonly id: Snowflake
    readonly address: EntityAddress
    readonly version: Version
    readonly message: Msg
    readonly context: Context<Schema.Serializable.Context<Msg>>
  }
): EnvelopeWithContext<Version, Msg> => {
  const self = make(options) as any
  self.context = options.context
  Object.defineProperty(self, "encodeCache", {
    enumerable: false,
    writable: true,
    value: undefined
  })
  return self
}

const encodeCache = globalValue(
  "@effect/cluster/Envelope/encodeCache",
  () => new WeakMap<Schema.Schema.Any, (u: Envelope<any, any>) => Effect.Effect<Envelope.Encoded, ParseError>>()
)

const getEncode = <Msg extends Envelope.AnyMessage>(
  message: Msg
): <Version extends string>(
  u: EnvelopeWithContext<Version, Msg>
) => Effect.Effect<Envelope.Encoded, ParseError, Schema.Serializable.Context<Msg>> => {
  const schema = Schema.serializableSchema(message)
  let encode = encodeCache.get(schema)
  if (encode !== undefined) {
    return encode
  }
  encode = Schema.encode(Schema.Struct({
    id: SnowflakeFromBigInt,
    address: EntityAddress,
    version: Schema.String,
    message: schema
  })) as any
  encodeCache.set(schema, encode!)
  return encode!
}

/**
 * @since 1.0.0
 * @category serialization / deserialization
 */
export const serialize = <Version extends string, Msg extends Envelope.AnyMessage>(
  envelope: EnvelopeWithContext<Version, Msg>
): Effect.Effect<
  Envelope.Encoded,
  ParseError
> => {
  return Effect.suspend(() => {
    if (envelope.encodedCache !== undefined) {
      return Effect.succeed(envelope.encodedCache)
    }
    return Effect.provide(
      Effect.tap(getEncode(envelope.message)(envelope), (encoded) => {
        envelope.encodedCache = encoded
      }),
      envelope.context
    )
  })
}

/**
 * @since 1.0.0
 * @category serialization / deserialization
 */
export const EnvelopeFromSelf: Schema.Schema<
  Envelope.Any,
  Envelope.Any
> = Schema.declare(isEnvelope, {
  identifier: "Envelope"
})

/**
 * @since 1.0.0
 * @category serialization / deserialization
 */
export const PartialEncoded: Schema.Schema<
  Envelope.PartialEncoded,
  Envelope.Encoded
> = Schema.Struct({
  id: SnowflakeFromBigInt,
  address: EntityAddress,
  version: Schema.String,
  message: Schema.Unknown
})

/**
 * @since 1.0.0
 * @category serialization / deserialization
 */
export const PartialEncodedFromSelf: Schema.Struct<
  {
    id: Schema.Schema<Snowflake>
    address: Schema.Schema<EntityAddress>
    version: typeof Schema.String
    message: typeof Schema.Unknown
  }
> = Schema.Struct({
  id: Schema.typeSchema(SnowflakeFromBigInt),
  address: EntityAddressFromSelf,
  version: Schema.String,
  message: Schema.Unknown
})

/**
 * @since 1.0.0
 * @category serialization / deserialization
 */
export const deserializePartial: (envelope: Envelope.Encoded) => Effect.Effect<
  Envelope.PartialEncoded,
  ParseError
> = Schema.decode(PartialEncoded)
