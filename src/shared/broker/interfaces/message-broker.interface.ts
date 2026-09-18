/**
 * Broker-agnostic transport abstraction for the distributed pipeline (EVO-1196).
 * Concrete adapters (kafkajs, amqplib) live in `../adapters/` and are selected
 * at boot by `BrokerModule` based on the `BROKER_TYPE` env var.
 */

export interface BrokerMessage<T = unknown> {
  id: string;
  payload: T;
  headers: Record<string, string>;
  raw: unknown;
}

/**
 * Result of a readiness health check against the broker (EVO-1226 [5.1]).
 * `connected` reflects transport liveness; `missingTopics` lists any of the
 * caller-supplied `expectedTopics` whose broker-side object is absent (Kafka
 * topic / RabbitMQ exchange). An empty `expectedTopics` ⇒ connection-only check
 * (`missingTopics` always `[]`).
 */
export interface BrokerHealth {
  connected: boolean;
  missingTopics: string[];
}

export interface IMessageBroker {
  /**
   * `headers` travel with the message and come back in `BrokerMessage.headers`.
   * The adapter's own keys (`correlationId`, `messageId`, `content-type`) win.
   */
  publish<T>(
    topic: string,
    payload: T,
    headers?: Record<string, string>,
  ): Promise<void>;
  subscribe<T>(
    topic: string,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void>;
  /**
   * Subscribe to every topic under a dot-delimited `prefix` (e.g.
   * `events.received` → all `events.received.<segment>` topics). Each adapter
   * maps the prefix to its native wildcard: Kafka to a RegExp consumer
   * subscription, RabbitMQ to a `<prefix>.#` binding on a shared `<prefix>`
   * topic exchange (see `EVENTS_RECEIVED_*` in contracts/broker-topics.ts).
   */
  subscribePattern<T>(
    prefix: string,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void>;
  ack(msg: BrokerMessage): Promise<void>;
  nack(msg: BrokerMessage, requeue?: boolean): Promise<void>;
  /**
   * Idempotently create a topic's broker-side topology (Kafka topic; RabbitMQ
   * exchange + default durable queue + binding) ahead of any publish/subscribe,
   * for explicit deploy-time provisioning (EVO-1200). Safe to call repeatedly.
   */
  provisionTopic(topic: string): Promise<void>;
  /**
   * Best-effort consumer lag for this process's consumer group/queue on
   * `topic` (Kafka: sum of partition high-watermark minus committed offset;
   * RabbitMQ: ready message count of the `${RUN_MODE}-${topic}` queue). Feeds
   * the `consumer_lag` gauge (NFR33); callers must tolerate a rejection and
   * never let a failed poll disturb message processing.
   */
  getTopicLag(topic: string): Promise<number>;
  /**
   * Readiness probe for the `/ready` endpoint (EVO-1226 [5.1]). Reports whether
   * the transport is connected and, for each `expectedTopics` entry, whether its
   * broker-side object exists (Kafka: topic via `admin.listTopics()`; RabbitMQ:
   * the resolved `topic` exchange via a throwaway channel `checkExchange`).
   * Pass `[]` for connection-only verification (dynamic/parametric topics).
   * Must never throw — adapters return `{ connected: false, ... }` on error.
   */
  healthCheck(expectedTopics: string[]): Promise<BrokerHealth>;
}

export const IMESSAGE_BROKER: unique symbol = Symbol('IMessageBroker');
