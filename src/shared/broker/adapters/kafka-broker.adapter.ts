import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  Admin,
  Consumer,
  EachMessagePayload,
  Kafka,
  KafkaConfig,
  Producer,
  SASLOptions,
} from 'kafkajs';
import { randomUUID } from 'crypto';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';
import {
  BrokerHealth,
  BrokerMessage,
  IMessageBroker,
} from '../interfaces/message-broker.interface';
import { BrokerType } from '../types/broker-type.enum';
import { BrokerMetrics } from '../metrics/broker-metrics';
import { CAMPAIGNS_CONTROL_TOPIC } from '../contracts/campaigns-control.contract';
import {
  DELIVERY_ATTEMPT_HEADER,
  DLQ_REASON_HEADER,
  DELIVERY_LIMIT_EXCEEDED,
  ORIGINAL_TOPIC_HEADER,
  dlqNameFor,
  nextAttempt,
  resolveDeliveryLimit,
} from '../redelivery';

const CLIENT_ID = 'evo-flow-broker';
const DEFAULT_NUM_PARTITIONS = 12;
const DEFAULT_REPLICATION_FACTOR = 1;

// EVO-1222 [4.8]: per-topic provisioning overrides. `campaigns.control` carries
// ordered pause/resume signals per campaign, so it is single-partition (global
// ordering) with short retention — it is a fast-path, not a history log.
const TOPIC_CONFIG_OVERRIDES: Record<
  string,
  { numPartitions?: number; retentionMs?: number }
> = {
  [CAMPAIGNS_CONTROL_TOPIC]: { numPartitions: 1, retentionMs: 86_400_000 },
};
const CONNECT_RETRY_BUDGET_MS = 30_000;
const CONNECT_RETRY_MAX_BACKOFF_MS = 15_000;
const TOPIC_ALREADY_EXISTS_PATTERN = /already exists/i;
const SUPPORTED_SASL_MECHANISMS = new Set([
  'plain',
  'scram-sha-256',
  'scram-sha-512',
]);
const BROKER_LABEL = 'kafka';

type SaslMechanism = 'plain' | 'scram-sha-256' | 'scram-sha-512';
type StructuredLogLevel = 'debug' | 'info' | 'warn' | 'error';

interface AckHandle {
  topic: string;
  partition: number;
  offset: string;
  consumer: Consumer;
}

@Injectable()
export class KafkaBrokerAdapter
  implements IMessageBroker, OnModuleInit, OnModuleDestroy
{
  private readonly logger = new CustomLoggerService(KafkaBrokerAdapter.name);

  private kafka: Kafka | null = null;
  private producer: Producer | null = null;
  private admin: Admin | null = null;
  private readonly consumers = new Map<string, Consumer>();
  // Per-topic subscription count. Repeat subscriptions to the same topic each
  // get a distinct consumer group so they coexist in one process (EVO-1737).
  private readonly topicSubscriptions = new Map<string, number>();
  private readonly pendingAcks = new WeakMap<BrokerMessage, AckHandle>();
  private readonly ensuredTopics = new Set<string>();
  private deliveryLimit = 3;
  private warnedAboutRunMode = false;
  private active = false;

  constructor(
    private readonly config: ConfigService,
    private readonly metrics: BrokerMetrics,
  ) {}

  async onModuleInit(): Promise<void> {
    const brokerType = this.config.get<string>('BROKER_TYPE');
    if (brokerType !== BrokerType.KAFKA) {
      // Adapter is registered but a different broker is active. Stay dormant.
      return;
    }

    this.deliveryLimit = resolveDeliveryLimit(this.config);
    this.kafka = this.buildKafkaClient();
    this.admin = this.kafka.admin();
    this.producer = this.kafka.producer({ idempotent: true });

    await this.connectWithBackoff();
    this.active = true;
    this.writeStructured('info', 'broker.boot', { broker: BROKER_LABEL });
  }

  async onModuleDestroy(): Promise<void> {
    if (!this.active) return;

    for (const [consumerKey, consumer] of this.consumers.entries()) {
      try {
        await consumer.disconnect();
      } catch (err) {
        this.writeStructured('warn', 'broker.shutdown.consumer_failed', {
          broker: BROKER_LABEL,
          consumerKey,
          error: (err as Error).message,
        });
      }
    }
    this.consumers.clear();
    this.topicSubscriptions.clear();

    try {
      await this.producer?.disconnect();
    } catch (err) {
      this.writeStructured('warn', 'broker.shutdown.producer_failed', {
        broker: BROKER_LABEL,
        error: (err as Error).message,
      });
    }
    try {
      await this.admin?.disconnect();
    } catch (err) {
      this.writeStructured('warn', 'broker.shutdown.admin_failed', {
        broker: BROKER_LABEL,
        error: (err as Error).message,
      });
    }

    this.active = false;
  }

  async publish<T>(
    topic: string,
    payload: T,
    headers: Record<string, string> = {},
  ): Promise<void> {
    this.assertActive('publish');

    await this.ensureTopicExists(topic);

    const correlationId = randomUUID();
    const messageId = randomUUID();
    const value = JSON.stringify(payload);

    await this.producer!.send({
      topic,
      messages: [
        {
          value,
          headers: {
            ...headers,
            correlationId,
            messageId,
            'content-type': 'application/json',
          },
        },
      ],
    });

    this.writeStructured('debug', 'broker.publish', {
      broker: BROKER_LABEL,
      topic,
      correlationId,
      messageId,
    });
  }

  async subscribe<T>(
    topic: string,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void> {
    this.assertActive('subscribe');

    await this.ensureTopicExists(topic);

    // Single mode runs every runner in one process, so a topic consumed by two
    // runners (e.g. campaigns.control: packer + sender, EVO-1222) would otherwise
    // collide on one consumer group. Give each repeat subscription its own group
    // so both receive every message — the same broadcast the distributed
    // deployment already gets from separate per-runner groups. EVO-1737.
    // The "-N" suffix is positional (module load order), so a group name is not
    // guaranteed stable across restarts; acceptable because repeat-subscribed
    // topics are broadcast control topics whose Postgres flag stays authoritative.
    const baseGroupId = `${this.resolveRunMode(topic)}-${topic}`;
    const priorSubs = this.topicSubscriptions.get(topic) ?? 0;
    const groupId =
      priorSubs === 0 ? baseGroupId : `${baseGroupId}-${priorSubs + 1}`;

    if (this.consumers.has(groupId)) {
      throw new Error(
        `KafkaBrokerAdapter already has a consumer registered for group "${groupId}".`,
      );
    }
    this.topicSubscriptions.set(topic, priorSubs + 1);
    const consumer = this.kafka!.consumer({ groupId });

    consumer.on(consumer.events.CRASH, (event) => {
      this.writeStructured('error', 'broker.consumer.crash', {
        broker: BROKER_LABEL,
        topic,
        groupId,
        restart: event.payload?.restart,
        error: event.payload?.error?.message,
      });
    });

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload) =>
        this.dispatchMessage(consumer, payload, handler),
    });

    this.consumers.set(groupId, consumer);
    this.writeStructured('info', 'broker.subscribe', {
      broker: BROKER_LABEL,
      topic,
      groupId,
    });
  }

  async provisionTopic(topic: string): Promise<void> {
    this.assertActive('provisionTopic');
    await this.ensureTopicExists(topic);
  }

  async healthCheck(expectedTopics: string[]): Promise<BrokerHealth> {
    const connected =
      this.active && this.producer !== null && this.admin !== null;
    if (!connected) {
      return { connected: false, missingTopics: expectedTopics };
    }
    if (expectedTopics.length === 0) {
      return { connected: true, missingTopics: [] };
    }
    try {
      const existing = await this.admin!.listTopics();
      const missingTopics = expectedTopics.filter((t) => !existing.includes(t));
      return { connected: true, missingTopics };
    } catch {
      // Metadata fetch failed — treat the transport as not ready rather than
      // claiming the topics are missing, so the readiness payload reads cleanly.
      return { connected: false, missingTopics: expectedTopics };
    }
  }

  async getTopicLag(topic: string): Promise<number> {
    this.assertActive('getTopicLag');

    const groupId = `${this.resolveRunMode(topic)}-${topic}`;
    const [latest, committed] = await Promise.all([
      this.admin!.fetchTopicOffsets(topic),
      this.admin!.fetchOffsets({ groupId, topics: [topic] }),
    ]);

    const committedByPartition = new Map<number, string>();
    for (const entry of committed) {
      for (const partition of entry.partitions) {
        committedByPartition.set(partition.partition, partition.offset);
      }
    }

    let lag = 0;
    for (const partition of latest) {
      const committedOffset = committedByPartition.get(partition.partition);
      // '-1' means the group never committed on this partition; with
      // fromBeginning=false the consumer starts at latest, so lag is 0.
      if (committedOffset === undefined || Number(committedOffset) < 0) {
        continue;
      }
      lag += Math.max(Number(partition.offset) - Number(committedOffset), 0);
    }
    return lag;
  }

  async subscribePattern<T>(
    prefix: string,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void> {
    this.assertActive('subscribePattern');

    const key = `pattern:${prefix}`;
    if (this.consumers.has(key)) {
      throw new Error(
        `KafkaBrokerAdapter already has a consumer registered for pattern "${prefix}".`,
      );
    }

    const pattern = new RegExp(`^${this.escapeRegexLiteral(prefix)}\\.[^.]+$`);
    const groupId = `${this.resolveRunMode(prefix)}-${prefix}`;
    const consumer = this.kafka!.consumer({ groupId });

    consumer.on(consumer.events.CRASH, (event) => {
      this.writeStructured('error', 'broker.consumer.crash', {
        broker: BROKER_LABEL,
        topic: prefix,
        groupId,
        restart: event.payload?.restart,
        error: event.payload?.error?.message,
      });
    });

    await consumer.connect();
    // RegExp subscription: kafkajs matches the pattern against topics known at
    // subscribe time and on each metadata refresh. Concrete per-segment topics
    // are created by the publisher's ensureTopicExists, so we deliberately do
    // NOT create a topic for the pattern here.
    await consumer.subscribe({ topics: [pattern], fromBeginning: false });

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload) =>
        this.dispatchMessage(consumer, payload, handler),
    });

    this.consumers.set(key, consumer);
    this.writeStructured('info', 'broker.subscribe.pattern', {
      broker: BROKER_LABEL,
      prefix,
      groupId,
      pattern: pattern.source,
    });
  }

  private escapeRegexLiteral(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  async ack(msg: BrokerMessage): Promise<void> {
    const handle = this.pendingAcks.get(msg);
    if (!handle) {
      throw new Error(
        `KafkaBrokerAdapter.ack called on unknown message id="${msg.id}". Was the message produced by this adapter's subscribe?`,
      );
    }

    await handle.consumer.commitOffsets([
      {
        topic: handle.topic,
        partition: handle.partition,
        offset: String(Number(handle.offset) + 1),
      },
    ]);

    this.pendingAcks.delete(msg);
    this.writeStructured('debug', 'broker.ack', {
      broker: BROKER_LABEL,
      topic: handle.topic,
      correlationId: msg.headers.correlationId,
      messageId: msg.headers.messageId,
    });
  }

  async nack(msg: BrokerMessage, requeue: boolean = true): Promise<void> {
    const handle = this.pendingAcks.get(msg);
    if (!handle) {
      throw new Error(
        `KafkaBrokerAdapter.nack called on unknown message id="${msg.id}". Was the message produced by this adapter's subscribe?`,
      );
    }

    const commitPastOriginal = (): Promise<void> =>
      handle.consumer.commitOffsets([
        {
          topic: handle.topic,
          partition: handle.partition,
          offset: String(Number(handle.offset) + 1),
        },
      ]);

    // Explicit terminal drop: caller already decided this is non-retriable.
    if (!requeue) {
      await commitPastOriginal();
      this.metrics.terminalFailures.inc({
        broker: BROKER_LABEL,
        topic: handle.topic,
      });
      this.pendingAcks.delete(msg);
      this.writeStructured('info', 'broker.nack.terminal', {
        broker: BROKER_LABEL,
        topic: handle.topic,
        correlationId: msg.headers.correlationId,
        messageId: msg.headers.messageId,
      });
      return;
    }

    // Redelivery backstop (EVO-1677): Kafka has no native delivery count and an
    // in-place `seek` rewinds the partition behind a poison message forever.
    // Instead, republish to the same topic with an incremented attempt header
    // (so the retry goes to the tail, not blocking the partition) — or, once the
    // limit is reached, route to the `<topic>.dlq` — then commit past the
    // original so the partition keeps moving.
    const attempt = nextAttempt(msg.headers);
    const value = JSON.stringify(msg.payload);
    const headers: Record<string, string> = {
      ...msg.headers,
      [DELIVERY_ATTEMPT_HEADER]: String(attempt),
    };

    if (attempt >= this.deliveryLimit) {
      const dlq = dlqNameFor(handle.topic);
      await this.ensureTopicExists(dlq);
      await this.producer!.send({
        topic: dlq,
        messages: [
          {
            value,
            headers: {
              ...headers,
              [DLQ_REASON_HEADER]: DELIVERY_LIMIT_EXCEEDED,
              [ORIGINAL_TOPIC_HEADER]: handle.topic,
            },
          },
        ],
      });
      await commitPastOriginal();
      this.metrics.deadLettered.inc({
        broker: BROKER_LABEL,
        topic: handle.topic,
      });
      this.pendingAcks.delete(msg);
      this.writeStructured('warn', 'broker.nack.dead_lettered', {
        broker: BROKER_LABEL,
        topic: handle.topic,
        dlq,
        attempt,
        correlationId: msg.headers.correlationId,
        messageId: msg.headers.messageId,
      });
      return;
    }

    await this.producer!.send({
      topic: handle.topic,
      messages: [{ value, headers }],
    });
    await commitPastOriginal();
    this.pendingAcks.delete(msg);
    this.writeStructured('info', 'broker.nack.requeue', {
      broker: BROKER_LABEL,
      topic: handle.topic,
      attempt,
      correlationId: msg.headers.correlationId,
      messageId: msg.headers.messageId,
    });
  }

  private async dispatchMessage<T>(
    consumer: Consumer,
    payload: EachMessagePayload,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void> {
    const { topic, partition, message } = payload;
    const headers = this.decodeHeaders(message.headers);

    let parsed: T;
    try {
      parsed = JSON.parse(message.value?.toString('utf8') ?? 'null') as T;
    } catch (err) {
      // Poison pill: skip the message permanently to keep the consumer moving.
      this.writeStructured('error', 'broker.consume.poison', {
        broker: BROKER_LABEL,
        topic,
        partition,
        offset: message.offset,
        correlationId: headers.correlationId,
        messageId: headers.messageId,
        error: (err as Error).message,
      });
      this.metrics.terminalFailures.inc({ broker: BROKER_LABEL, topic });
      await consumer.commitOffsets([
        { topic, partition, offset: String(Number(message.offset) + 1) },
      ]);
      return;
    }

    const brokerMsg: BrokerMessage<T> = {
      id: `${topic}/${partition}/${message.offset}`,
      payload: parsed,
      headers,
      raw: message,
    };
    this.pendingAcks.set(brokerMsg, {
      topic,
      partition,
      offset: message.offset,
      consumer,
    });

    this.writeStructured('debug', 'broker.consume', {
      broker: BROKER_LABEL,
      topic,
      partition,
      offset: message.offset,
      correlationId: headers.correlationId,
      messageId: headers.messageId,
    });

    try {
      await handler(brokerMsg);
    } catch (err) {
      // Handler did not ack/nack — leave the offset uncommitted so Kafka
      // re-delivers on the next session. Surface the error for visibility.
      this.writeStructured('error', 'broker.consume.handler_error', {
        broker: BROKER_LABEL,
        topic,
        partition,
        offset: message.offset,
        correlationId: headers.correlationId,
        messageId: headers.messageId,
        error: (err as Error).message,
      });
    }
  }

  private decodeHeaders(
    raw: EachMessagePayload['message']['headers'],
  ): Record<string, string> {
    const out: Record<string, string> = {};
    if (!raw) return out;
    for (const [key, value] of Object.entries(raw)) {
      if (value == null) continue;
      out[key] = Buffer.isBuffer(value)
        ? value.toString('utf8')
        : String(value);
    }
    return out;
  }

  private async ensureTopicExists(topic: string): Promise<void> {
    if (this.ensuredTopics.has(topic)) return;
    const override = TOPIC_CONFIG_OVERRIDES[topic];
    try {
      await this.admin!.createTopics({
        topics: [
          {
            topic,
            numPartitions: override?.numPartitions ?? DEFAULT_NUM_PARTITIONS,
            replicationFactor: DEFAULT_REPLICATION_FACTOR,
            ...(override?.retentionMs != null && {
              configEntries: [
                { name: 'retention.ms', value: String(override.retentionMs) },
              ],
            }),
          },
        ],
        waitForLeaders: true,
      });
    } catch (err) {
      const message = (err as Error).message ?? '';
      if (!TOPIC_ALREADY_EXISTS_PATTERN.test(message)) throw err;
    }
    this.ensuredTopics.add(topic);
  }

  private resolveRunMode(topic: string): string {
    const runMode = this.config.get<string>('RUN_MODE');
    if (runMode) return runMode;
    if (!this.warnedAboutRunMode) {
      this.writeStructured('warn', 'broker.subscribe.no_run_mode', {
        broker: BROKER_LABEL,
        topic,
        fallback: 'single',
        hint: 'Set RUN_MODE so consumer groups isolate per pipeline mode.',
      });
      this.warnedAboutRunMode = true;
    }
    return 'single';
  }

  private buildKafkaClient(): Kafka {
    const brokersRaw = this.config.get<string>('KAFKA_BROKERS') ?? '';
    const brokers = brokersRaw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    if (brokers.length === 0) {
      throw new Error(
        'KAFKA_BROKERS must be a comma-separated list of host:port pairs.',
      );
    }

    const sslEnabled =
      String(
        this.config.get<string>('KAFKA_SSL_ENABLED') ?? '',
      ).toLowerCase() === 'true';

    const kafkaConfig: KafkaConfig = {
      clientId: CLIENT_ID,
      brokers,
      ssl: sslEnabled,
      sasl: this.buildSaslConfig(),
    };

    return new Kafka(kafkaConfig);
  }

  private buildSaslConfig(): SASLOptions | undefined {
    const mechanism = (
      this.config.get<string>('KAFKA_SASL_MECHANISM') ?? ''
    ).toLowerCase();
    const username = this.config.get<string>('KAFKA_SASL_USERNAME');
    const password = this.config.get<string>('KAFKA_SASL_PASSWORD');

    if (!mechanism && !username && !password) return undefined;

    if (!SUPPORTED_SASL_MECHANISMS.has(mechanism)) {
      throw new Error(
        `KAFKA_SASL_MECHANISM="${mechanism}" is not supported. Use one of: plain, scram-sha-256, scram-sha-512.`,
      );
    }
    if (!username || !password) {
      throw new Error(
        'KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required when KAFKA_SASL_MECHANISM is set.',
      );
    }

    return {
      mechanism: mechanism as SaslMechanism,
      username,
      password,
    } as SASLOptions;
  }

  private async connectWithBackoff(): Promise<void> {
    const startTime = Date.now();
    const deadline = startTime + CONNECT_RETRY_BUDGET_MS;
    let lastErr: Error | null = null;
    let attempt = 0;

    while (true) {
      attempt++;
      try {
        await this.admin!.connect();
        await this.producer!.connect();
        return;
      } catch (err) {
        lastErr = err as Error;
        this.writeStructured('warn', 'broker.connect.retry', {
          broker: BROKER_LABEL,
          attempt,
          elapsedMs: Date.now() - startTime,
          error: lastErr.message,
        });
      }

      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new Error(
          `KafkaBrokerAdapter failed to connect within ${CONNECT_RETRY_BUDGET_MS / 1000}s retry budget (${attempt} attempts): ${lastErr.message}`,
        );
      }

      const exponential = 1000 * Math.pow(2, attempt - 1);
      const wait = Math.min(
        exponential,
        CONNECT_RETRY_MAX_BACKOFF_MS,
        remaining,
      );
      await this.sleep(wait);
    }
  }

  private assertActive(op: string): void {
    if (!this.active) {
      throw new Error(
        `KafkaBrokerAdapter.${op} called before adapter became active. Set BROKER_TYPE=kafka and ensure onModuleInit completed.`,
      );
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Emit a structured log line that is queryable via the Winston file logger
   * (JSON with all fields preserved) AND visible on stdout/stderr where it
   * matters. Hot-path events (`debug`) are file-only to avoid console flood.
   */
  private writeStructured(
    level: StructuredLogLevel,
    action: string,
    ctx: Record<string, unknown>,
  ): void {
    const file = this.logger.getFileLogger();
    const payload = { action, ...ctx };
    switch (level) {
      case 'debug':
        file.debug(action, payload);
        return;
      case 'info':
        this.logger.log(action, payload);
        return;
      case 'warn':
        this.logger.warn(action, payload);
        file.warn(action, payload);
        return;
      case 'error':
        this.logger.error(action, payload);
        file.error(action, payload);
        return;
    }
  }
}
