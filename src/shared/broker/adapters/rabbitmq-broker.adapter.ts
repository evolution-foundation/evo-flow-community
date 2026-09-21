import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqplib from 'amqplib';
import { randomUUID } from 'crypto';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';
import {
  BrokerHealth,
  BrokerMessage,
  IMessageBroker,
} from '../interfaces/message-broker.interface';
import { BrokerType } from '../types/broker-type.enum';
import { BrokerMetrics } from '../metrics/broker-metrics';
import {
  DELIVERY_ATTEMPT_HEADER,
  DLQ_REASON_HEADER,
  DELIVERY_LIMIT_EXCEEDED,
  ORIGINAL_TOPIC_HEADER,
  dlqNameFor,
  nextAttempt,
  resolveDeliveryLimit,
} from '../redelivery';

type AmqpConnection = Awaited<ReturnType<typeof amqplib.connect>>;
type AmqpChannel = Awaited<ReturnType<AmqpConnection['createChannel']>>;
type AmqpMessage = NonNullable<
  Parameters<NonNullable<Parameters<AmqpChannel['consume']>[1]>>[0]
>;

const BROKER_LABEL = 'rabbitmq';
const DEFAULT_PREFETCH = 100;
/**
 * Topic families that publish/subscribe through a single shared `topic`
 * exchange named after the prefix (routing key = full topic), instead of the
 * default one-exchange-per-topic model. This is what makes `<prefix>.#`
 * wildcard fan-in work (EVO-1195 contract; see EVENTS_RECEIVED_RABBITMQ_BINDING).
 * Scoped to `events.received` on purpose — `campaigns.*` keeps its own model.
 */
const SHARED_EXCHANGE_PREFIXES = ['events.received'] as const;
const CONNECT_RETRY_BUDGET_MS = 30_000;
const CONNECT_RETRY_MAX_BACKOFF_MS = 15_000;
const RECONNECT_BUDGET_MS = 5_000;
const RECONNECT_BACKGROUND_INTERVAL_MS = 5_000;

type StructuredLogLevel = 'debug' | 'info' | 'warn' | 'error';

interface SubscriptionState<T = unknown> {
  topic: string;
  queueName: string;
  exchange: string;
  bindingKey: string;
  consumerTag: string | null;
  handler: (msg: BrokerMessage<T>) => Promise<void>;
}

interface AmqpAckHandle {
  topic: string;
  queueName: string;
  raw: AmqpMessage;
}

@Injectable()
export class RabbitMQBrokerAdapter
  implements IMessageBroker, OnModuleInit, OnModuleDestroy
{
  private readonly logger = new CustomLoggerService(RabbitMQBrokerAdapter.name);

  private connection: AmqpConnection | null = null;
  private channel: AmqpChannel | null = null;
  private readonly subscriptions = new Map<string, SubscriptionState>();
  private readonly declaredExchanges = new Set<string>();
  // Prefixes registered at runtime via subscribePattern. Unioned with the
  // static SHARED_EXCHANGE_PREFIXES so a same-process publish under a pattern
  // prefix also routes through the shared exchange (RUN_MODE=single, tests).
  private readonly patternPrefixes = new Set<string>();
  private readonly pendingAcks = new WeakMap<BrokerMessage, AmqpAckHandle>();
  private readonly declaredDlqs = new Set<string>();
  private deliveryLimit = 3;
  private active = false;
  private reconnecting = false;
  private warnedAboutRunMode = false;

  constructor(
    private readonly config: ConfigService,
    private readonly metrics: BrokerMetrics,
  ) {}

  async onModuleInit(): Promise<void> {
    const brokerType = this.config.get<string>('BROKER_TYPE');
    if (brokerType !== BrokerType.RABBITMQ) {
      return;
    }

    this.validateConfig();
    await this.connectWithBackoff();
    this.active = true;
    this.writeStructured('info', 'broker.boot', { broker: BROKER_LABEL });
  }

  private validateConfig(): void {
    const url = this.config.get<string>('RABBITMQ_URL');
    if (!url) {
      throw new Error(
        'RABBITMQ_URL must be set (format: amqp://user:pass@host:port/vhost).',
      );
    }
    const prefetchRaw = this.config.get<string>('RABBITMQ_PREFETCH_COUNT');
    if (prefetchRaw !== undefined && prefetchRaw !== '') {
      const prefetch = parseInt(prefetchRaw, 10);
      if (Number.isNaN(prefetch) || prefetch <= 0) {
        throw new Error(
          `RABBITMQ_PREFETCH_COUNT="${prefetchRaw}" must be a positive integer.`,
        );
      }
    }
    this.deliveryLimit = resolveDeliveryLimit(this.config);
  }

  async onModuleDestroy(): Promise<void> {
    // Always run cleanup. Without this, a destroy fired while we're in the
    // background-reconnect chain (active=false, connection=null, reconnecting=true)
    // would skip cleanup and leave the setTimeout chain alive, leaking the process.
    this.reconnecting = false;
    this.active = false;

    for (const [topic, sub] of this.subscriptions.entries()) {
      if (!sub.consumerTag) continue;
      try {
        await this.channel?.cancel(sub.consumerTag);
      } catch (err) {
        this.writeStructured('warn', 'broker.shutdown.consumer_failed', {
          broker: BROKER_LABEL,
          topic,
          error: (err as Error).message,
        });
      }
    }
    this.subscriptions.clear();

    try {
      await this.channel?.close();
    } catch (err) {
      this.writeStructured('warn', 'broker.shutdown.channel_failed', {
        broker: BROKER_LABEL,
        error: (err as Error).message,
      });
    }
    try {
      await this.connection?.close();
    } catch (err) {
      this.writeStructured('warn', 'broker.shutdown.connection_failed', {
        broker: BROKER_LABEL,
        error: (err as Error).message,
      });
    }

    this.connection = null;
    this.channel = null;
  }

  async publish<T>(
    topic: string,
    payload: T,
    headers: Record<string, string> = {},
  ): Promise<void> {
    this.assertActive('publish');

    const { exchange, routingKey } = this.resolveExchange(topic);
    await this.ensureExchange(exchange);

    const correlationId = randomUUID();
    const messageId = randomUUID();
    const content = Buffer.from(JSON.stringify(payload));

    this.channel!.publish(exchange, routingKey, content, {
      persistent: true,
      contentType: 'application/json',
      messageId,
      correlationId,
      headers: {
        ...headers,
        correlationId,
        messageId,
        'content-type': 'application/json',
      },
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

    if (this.subscriptions.has(topic)) {
      throw new Error(
        `RabbitMQBrokerAdapter already has a consumer registered for topic "${topic}".`,
      );
    }

    const queueName = `${this.resolveRunMode(topic)}-${topic}`;
    const { exchange } = this.resolveExchange(topic);
    const state: SubscriptionState<T> = {
      topic,
      queueName,
      exchange,
      bindingKey: topic,
      consumerTag: null,
      handler,
    };
    this.subscriptions.set(topic, state as SubscriptionState);

    await this.attachConsumer(state);
    this.writeStructured('info', 'broker.subscribe', {
      broker: BROKER_LABEL,
      topic,
      queueName,
    });
  }

  async provisionTopic(topic: string): Promise<void> {
    this.assertActive('provisionTopic');
    await this.ensureExchange(topic);
    // Declare the durable queue but do NOT bind it. A bound, never-drained
    // default queue would accumulate a copy of every message — the real
    // consumer uses its own `${runMode}-${topic}` queue and binds it on
    // subscribe. Provisioning only guarantees the exchange + queue exist.
    await this.channel!.assertQueue(topic, { durable: true });
  }

  async healthCheck(expectedTopics: string[]): Promise<BrokerHealth> {
    const connected =
      this.active && this.connection !== null && this.channel !== null;
    if (!connected) {
      return { connected: false, missingTopics: expectedTopics };
    }
    if (expectedTopics.length === 0) {
      return { connected: true, missingTopics: [] };
    }

    const missingTopics: string[] = [];
    for (const topic of expectedTopics) {
      const { exchange } = this.resolveExchange(topic);
      // Use a throwaway channel: `checkExchange` on a missing exchange raises a
      // channel-level error that CLOSES the channel — running it on the shared
      // `this.channel` would tear down the live consumer. We must also attach an
      // 'error' listener, or amqplib's emitted error event crashes the process.
      let probeCh: AmqpChannel | null = null;
      try {
        probeCh = await this.connection!.createChannel();
        probeCh.on('error', () => undefined);
        await probeCh.checkExchange(exchange);
      } catch {
        missingTopics.push(topic);
      } finally {
        // The channel is already closed when the check failed; closing again is
        // a no-op we swallow.
        try {
          await probeCh?.close();
        } catch {
          /* already closed by the failed check */
        }
      }
    }
    return { connected: true, missingTopics };
  }

  async getTopicLag(topic: string): Promise<number> {
    this.assertActive('getTopicLag');
    const queueName = `${this.resolveRunMode(topic)}-${topic}`;
    // Same declaration as attachConsumer, so this is idempotent. checkQueue
    // would 404 on a missing queue and that is a channel-level error that
    // CLOSES the adapter's single shared channel — a best-effort metrics poll
    // must never take down publish/ack for the whole process.
    const { messageCount } = await this.channel!.assertQueue(queueName, {
      durable: true,
    });
    return messageCount;
  }

  async subscribePattern<T>(
    prefix: string,
    handler: (msg: BrokerMessage<T>) => Promise<void>,
  ): Promise<void> {
    this.assertActive('subscribePattern');

    if (this.subscriptions.has(prefix)) {
      throw new Error(
        `RabbitMQBrokerAdapter already has a consumer registered for prefix "${prefix}".`,
      );
    }

    // Bind a durable queue to the shared `<prefix>` topic exchange with the
    // `<prefix>.#` wildcard, so every `<prefix>.<segment>` publish fans in.
    this.patternPrefixes.add(prefix);
    const queueName = `${this.resolveRunMode(prefix)}-${prefix}`;
    const bindingKey = `${prefix}.#`;
    const state: SubscriptionState<T> = {
      topic: prefix,
      queueName,
      exchange: prefix,
      bindingKey,
      consumerTag: null,
      handler,
    };
    this.subscriptions.set(prefix, state as SubscriptionState);

    await this.attachConsumer(state);
    this.writeStructured('info', 'broker.subscribe.pattern', {
      broker: BROKER_LABEL,
      prefix,
      queueName,
      bindingKey,
    });
  }

  async ack(msg: BrokerMessage): Promise<void> {
    const handle = this.pendingAcks.get(msg);
    if (!handle) {
      throw new Error(
        `RabbitMQBrokerAdapter.ack called on unknown message id="${msg.id}". Was the message produced by this adapter's subscribe?`,
      );
    }
    if (!this.channel) {
      // Channel dropped after dispatch — broker will re-deliver on reconnect.
      this.pendingAcks.delete(msg);
      this.writeStructured('warn', 'broker.ack.no_channel', {
        broker: BROKER_LABEL,
        topic: handle.topic,
        correlationId: msg.headers.correlationId,
        messageId: msg.headers.messageId,
      });
      return Promise.resolve();
    }

    this.channel.ack(handle.raw);
    this.pendingAcks.delete(msg);
    this.writeStructured('debug', 'broker.ack', {
      broker: BROKER_LABEL,
      topic: handle.topic,
      correlationId: msg.headers.correlationId,
      messageId: msg.headers.messageId,
    });
    return Promise.resolve();
  }

  async nack(msg: BrokerMessage, requeue: boolean = true): Promise<void> {
    const handle = this.pendingAcks.get(msg);
    if (!handle) {
      throw new Error(
        `RabbitMQBrokerAdapter.nack called on unknown message id="${msg.id}". Was the message produced by this adapter's subscribe?`,
      );
    }
    if (!this.channel) {
      this.pendingAcks.delete(msg);
      this.writeStructured('warn', 'broker.nack.no_channel', {
        broker: BROKER_LABEL,
        topic: handle.topic,
        correlationId: msg.headers.correlationId,
        messageId: msg.headers.messageId,
      });
      return Promise.resolve();
    }

    // Explicit terminal drop: caller already decided this is non-retriable.
    if (!requeue) {
      this.channel.nack(handle.raw, false, false);
      this.pendingAcks.delete(msg);
      this.metrics.terminalFailures.inc({
        broker: BROKER_LABEL,
        topic: handle.topic,
      });
      this.writeStructured('info', 'broker.nack.terminal', {
        broker: BROKER_LABEL,
        topic: handle.topic,
        correlationId: msg.headers.correlationId,
        messageId: msg.headers.messageId,
      });
      return;
    }

    // Redelivery backstop (EVO-1677): instead of an in-place requeue that can
    // loop a poison message forever, republish to the same queue with an
    // incremented attempt header — or, once the limit is reached, route to the
    // `<queue>.dlq` and ack the original so it leaves the main queue.
    const attempt = nextAttempt(msg.headers);
    const headers: Record<string, string> = {
      ...msg.headers,
      [DELIVERY_ATTEMPT_HEADER]: String(attempt),
    };

    if (attempt >= this.deliveryLimit) {
      const dlq = dlqNameFor(handle.queueName);
      await this.ensureDlqQueue(dlq);
      this.channel.sendToQueue(dlq, handle.raw.content, {
        persistent: true,
        headers: {
          ...headers,
          [DLQ_REASON_HEADER]: DELIVERY_LIMIT_EXCEEDED,
          [ORIGINAL_TOPIC_HEADER]: handle.topic,
        },
      });
      this.channel.ack(handle.raw);
      this.pendingAcks.delete(msg);
      this.metrics.deadLettered.inc({
        broker: BROKER_LABEL,
        topic: handle.topic,
      });
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

    this.channel.sendToQueue(handle.queueName, handle.raw.content, {
      persistent: true,
      headers,
    });
    this.channel.ack(handle.raw);
    this.pendingAcks.delete(msg);
    this.writeStructured('info', 'broker.nack.requeue', {
      broker: BROKER_LABEL,
      topic: handle.topic,
      attempt,
      correlationId: msg.headers.correlationId,
      messageId: msg.headers.messageId,
    });
  }

  private async ensureDlqQueue(dlq: string): Promise<void> {
    if (this.declaredDlqs.has(dlq)) return;
    await this.channel!.assertQueue(dlq, { durable: true });
    this.declaredDlqs.add(dlq);
  }

  private async attachConsumer<T>(state: SubscriptionState<T>): Promise<void> {
    await this.ensureExchange(state.exchange);
    await this.channel!.assertQueue(state.queueName, { durable: true });
    await this.channel!.bindQueue(
      state.queueName,
      state.exchange,
      state.bindingKey,
    );

    const { consumerTag } = await this.channel!.consume(
      state.queueName,
      (raw) => {
        if (raw === null) return;
        void this.dispatchMessage(raw, state);
      },
    );
    state.consumerTag = consumerTag;
  }

  private async dispatchMessage<T>(
    raw: AmqpMessage,
    state: SubscriptionState<T>,
  ): Promise<void> {
    if (raw === null) return;

    const headers = this.decodeHeaders(raw.properties.headers);
    let parsed: T;
    try {
      parsed = JSON.parse(raw.content.toString('utf8')) as T;
    } catch (err) {
      this.writeStructured('error', 'broker.consume.poison', {
        broker: BROKER_LABEL,
        topic: state.topic,
        deliveryTag: raw.fields.deliveryTag,
        correlationId: headers.correlationId,
        messageId: headers.messageId,
        error: (err as Error).message,
      });
      this.metrics.terminalFailures.inc({
        broker: BROKER_LABEL,
        topic: state.topic,
      });
      try {
        this.channel?.nack(raw, false, false);
      } catch {
        // Channel may be closed; broker will re-deliver after reconnect.
      }
      return;
    }

    const brokerMsg: BrokerMessage<T> = {
      id: `${state.topic}/${raw.fields.deliveryTag}`,
      payload: parsed,
      headers,
      raw,
    };
    this.pendingAcks.set(brokerMsg, {
      topic: state.topic,
      queueName: state.queueName,
      raw,
    });

    this.writeStructured('debug', 'broker.consume', {
      broker: BROKER_LABEL,
      topic: state.topic,
      deliveryTag: raw.fields.deliveryTag,
      correlationId: headers.correlationId,
      messageId: headers.messageId,
    });

    try {
      await state.handler(brokerMsg);
    } catch (err) {
      // Handler did not ack/nack — leave unack'd; broker re-delivers on
      // consumer cancellation or after channel drop.
      this.writeStructured('error', 'broker.consume.handler_error', {
        broker: BROKER_LABEL,
        topic: state.topic,
        deliveryTag: raw.fields.deliveryTag,
        correlationId: headers.correlationId,
        messageId: headers.messageId,
        error: (err as Error).message,
      });
    }
  }

  private decodeHeaders(raw: unknown): Record<string, string> {
    const out: Record<string, string> = {};
    if (raw == null || typeof raw !== 'object') return out;
    for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
      if (value == null) continue;
      if (Buffer.isBuffer(value)) {
        out[key] = value.toString('utf8');
        continue;
      }
      if (typeof value === 'string') {
        out[key] = value;
        continue;
      }
      if (typeof value === 'number' || typeof value === 'boolean') {
        out[key] = String(value);
        continue;
      }
      out[key] = JSON.stringify(value);
    }
    return out;
  }

  private async ensureExchange(exchange: string): Promise<void> {
    if (this.declaredExchanges.has(exchange)) return;
    await this.channel!.assertExchange(exchange, 'topic', { durable: true });
    this.declaredExchanges.add(exchange);
  }

  /**
   * Map a topic to its `(exchange, routingKey)`. Topics under a shared-exchange
   * prefix route through a single `<prefix>` exchange (routing key = full
   * topic) so a `<prefix>.#` binding can fan in; all other topics keep the
   * one-exchange-per-topic model (exchange = routingKey = topic).
   */
  private resolveExchange(topic: string): {
    exchange: string;
    routingKey: string;
  } {
    const prefix = [...SHARED_EXCHANGE_PREFIXES, ...this.patternPrefixes].find(
      (p) => topic === p || topic.startsWith(`${p}.`),
    );
    return prefix
      ? { exchange: prefix, routingKey: topic }
      : { exchange: topic, routingKey: topic };
  }

  private resolveRunMode(topic: string): string {
    const runMode = this.config.get<string>('RUN_MODE');
    if (runMode) return runMode;
    if (!this.warnedAboutRunMode) {
      this.writeStructured('warn', 'broker.subscribe.no_run_mode', {
        broker: BROKER_LABEL,
        topic,
        fallback: 'single',
        hint: 'Set RUN_MODE so consumer queues isolate per pipeline mode.',
      });
      this.warnedAboutRunMode = true;
    }
    return 'single';
  }

  private async connectWithBackoff(): Promise<void> {
    const startTime = Date.now();
    const deadline = startTime + CONNECT_RETRY_BUDGET_MS;
    let lastErr: Error | null = null;
    let attempt = 0;

    while (true) {
      attempt++;
      try {
        await this.openConnection();
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
          `RabbitMQBrokerAdapter failed to connect within ${CONNECT_RETRY_BUDGET_MS / 1000}s retry budget (${attempt} attempts): ${lastErr.message}`,
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

  private async openConnection(): Promise<void> {
    const url = this.config.get<string>('RABBITMQ_URL')!;
    const vhost = this.config.get<string>('RABBITMQ_VHOST');
    const prefetchRaw = this.config.get<string>('RABBITMQ_PREFETCH_COUNT');
    const prefetch = prefetchRaw ? parseInt(prefetchRaw, 10) : DEFAULT_PREFETCH;

    const connectOpts = vhost ? { vhost } : undefined;
    this.connection = await amqplib.connect(url, connectOpts);
    this.channel = await this.connection.createChannel();
    await this.channel.prefetch(prefetch);

    this.connection.on('close', (err?: Error) => {
      if (err) {
        this.writeStructured('warn', 'broker.connection.close_with_error', {
          broker: BROKER_LABEL,
          error: err.message,
        });
      }
      void this.handleConnectionClose();
    });
    this.connection.on('error', (err) => {
      this.writeStructured('error', 'broker.connection.error', {
        broker: BROKER_LABEL,
        error: (err as Error).message,
      });
    });
    this.channel.on('error', (err) => {
      // amqplib emits 'error' on the channel for protocol errors (invalid ack,
      // publish to a missing exchange, etc.). Without a listener Node crashes
      // with 'Unhandled error event'. The 'close' event right after drives
      // the actual reconnect.
      this.writeStructured('error', 'broker.channel.error', {
        broker: BROKER_LABEL,
        error: (err as Error).message,
      });
    });
    this.channel.on('close', () => {
      // Channel close with the connection still alive means broker/amqplib
      // killed only the channel; consumers would silently stop without a
      // reconnect. Active=false means we're in shutdown — nothing to do.
      if (!this.active) return;
      this.writeStructured('warn', 'broker.channel.closed_unexpected', {
        broker: BROKER_LABEL,
      });
      void this.handleConnectionClose();
    });
  }

  private async handleConnectionClose(): Promise<void> {
    if (!this.active) return;
    if (this.reconnecting) return;
    this.reconnecting = true;
    this.active = false;

    this.writeStructured('warn', 'broker.connection.lost', {
      broker: BROKER_LABEL,
    });
    this.connection = null;
    this.channel = null;
    this.declaredExchanges.clear();
    // Same reason as declaredExchanges: the new channel must re-assert DLQ
    // queues, or a post-reconnect dead-letter could route to a queue the broker
    // dropped (and a non-confirm sendToQueue would silently discard it).
    this.declaredDlqs.clear();

    const startTime = Date.now();
    const deadline = startTime + RECONNECT_BUDGET_MS;

    const tryReconnect = async (): Promise<boolean> => {
      try {
        await this.openConnection();
        for (const sub of this.subscriptions.values()) {
          sub.consumerTag = null;
          await this.attachConsumer(sub);
        }
        return true;
      } catch (err) {
        this.writeStructured('warn', 'broker.connection.retry', {
          broker: BROKER_LABEL,
          elapsedMs: Date.now() - startTime,
          error: (err as Error).message,
        });
        return false;
      }
    };

    let attempt = 0;
    let wait = 500;
    while (Date.now() < deadline) {
      attempt++;
      if (await tryReconnect()) {
        this.reconnecting = false;
        this.active = true;
        this.writeStructured('info', 'broker.connection.restored', {
          broker: BROKER_LABEL,
          attempts: attempt,
          elapsedMs: Date.now() - startTime,
        });
        return;
      }
      const remaining = deadline - Date.now();
      const sleepMs = Math.min(wait, remaining);
      if (sleepMs <= 0) break;
      await this.sleep(sleepMs);
      wait = Math.min(wait * 2, 2000);
    }

    // Per D5b option B: budget exceeded, keep trying in background
    // at a steady cadence. active stays false so publish callers fail fast.
    this.active = false;
    this.writeStructured('error', 'broker.connection.budget_exceeded', {
      broker: BROKER_LABEL,
      budgetMs: RECONNECT_BUDGET_MS,
      hint: 'Switching to background reconnect; publishers will fail until restored.',
    });
    this.scheduleBackgroundReconnect();
  }

  private scheduleBackgroundReconnect(): void {
    const attempt = async (): Promise<void> => {
      if (!this.reconnecting) return;
      try {
        await this.openConnection();
        for (const sub of this.subscriptions.values()) {
          sub.consumerTag = null;
          await this.attachConsumer(sub);
        }
        this.reconnecting = false;
        this.active = true;
        this.writeStructured('info', 'broker.connection.restored', {
          broker: BROKER_LABEL,
          source: 'background',
        });
      } catch (err) {
        this.writeStructured('warn', 'broker.connection.retry', {
          broker: BROKER_LABEL,
          source: 'background',
          error: (err as Error).message,
        });
        setTimeout(() => void attempt(), RECONNECT_BACKGROUND_INTERVAL_MS);
      }
    };
    setTimeout(() => void attempt(), RECONNECT_BACKGROUND_INTERVAL_MS);
  }

  private assertActive(op: string): void {
    if (!this.active) {
      throw new Error(
        `RabbitMQBrokerAdapter.${op} called while inactive. Set BROKER_TYPE=rabbitmq and ensure onModuleInit completed; if reconnecting, retry later.`,
      );
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

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
