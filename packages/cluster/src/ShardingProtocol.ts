/**
 * @since 1.0.0
 */
import * as Context from "effect/Context"

const symbolKey = "@effect/cluster/ShardingProtocol"

/**
 * @since 1.0.0
 * @category type ids
 */
export const TypeId: unique symbol = Symbol.for(symbolKey)

/**
 * @since 1.0.0
 * @category type ids
 */
export type TypeId = typeof TypeId

/**
 * @since 1.0.0
 * @category models
 */
export interface ShardingProtocol {
  readonly [TypeId]: TypeId
}

/**
 * @since 1.0.0
 * @category tags
 */
export const ShardingProtocol = Context.GenericTag<ShardingProtocol>(symbolKey)
