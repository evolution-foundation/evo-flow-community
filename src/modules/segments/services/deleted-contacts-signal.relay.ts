import { randomUUID } from 'crypto';
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';
import Redis from 'ioredis';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';
import { getProcessingConfig } from '../../processing/config/processing.config';
import { CONTACT_DELETED_INGESTED_EVENT } from '../queries/contact-event-names';

export interface ContactDeletedSignal {
  contactId?: string;
  relayed?: boolean;
}

interface RelayMessage {
  origin: string;
  contactId?: string;
}

/**
 * Carries the deleted-contact signal across processes. The ingest that emits it runs
 * in the api process, while segments are recomputed by the segment-worker: without this
 * relay the worker's DeletedContactsCacheService keeps a stale set until its TTL.
 * Best effort: if Redis is down the caches fall back to that TTL.
 */
@Injectable()
export class DeletedContactsSignalRelay
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new CustomLoggerService(
    DeletedContactsSignalRelay.name,
  );
  private readonly origin = randomUUID();
  private readonly channel: string;
  private publisher: Redis | null = null;
  private subscriber: Redis | null = null;

  constructor(private readonly eventEmitter: EventEmitter2) {
    // Pub/sub channels ignore the Redis db index, so the db goes into the name.
    const db = getProcessingConfig().redis?.db ?? 5;
    this.channel = `evo-flow:db${db}:segments:contact-deleted`;
  }

  // Never awaits Redis: an outage at boot must not hold up any run mode.
  onModuleInit(): void {
    this.publisher = this.createClient({ enableOfflineQueue: false });
    this.subscriber = this.createClient({});

    const subscriber = this.subscriber;
    subscriber.on('message', (channel: string, raw: string) =>
      this.onRelayMessage(channel, raw),
    );
    // Subscribing on every 'ready' also covers a boot where Redis was still down.
    subscriber.on('ready', () => {
      subscriber
        .subscribe(this.channel)
        .then(() =>
          this.logger.log(
            `Listening for deleted-contact signals on ${this.channel}`,
          ),
        )
        .catch((error: Error) =>
          this.logger.warn(
            `Could not subscribe to ${this.channel}; caches fall back to their TTL: ${error.message}`,
          ),
        );
    });

    for (const client of [this.publisher, subscriber]) {
      client.connect().catch(() => undefined);
    }
  }

  // disconnect(), not quit(): with Redis down, quit() rejects and the client keeps
  // reconnecting, leaving an open handle behind.
  onModuleDestroy(): void {
    for (const client of [this.subscriber, this.publisher]) {
      client?.disconnect();
    }
  }

  @OnEvent(CONTACT_DELETED_INGESTED_EVENT)
  async onContactDeletedIngested(signal?: ContactDeletedSignal): Promise<void> {
    if (signal?.relayed || !this.publisher) return;

    const message: RelayMessage = {
      origin: this.origin,
      contactId: signal?.contactId,
    };
    try {
      await this.publisher.publish(this.channel, JSON.stringify(message));
    } catch (error) {
      this.logger.warn(
        `Could not publish the deleted-contact signal; other processes fall back to their TTL: ${(error as Error).message}`,
      );
    }
  }

  private onRelayMessage(channel: string, raw: string): void {
    if (channel !== this.channel) return;

    const message = parseRelayMessage(raw);
    if (!message) {
      this.logger.warn(`Ignoring malformed deleted-contact signal: ${raw}`);
      return;
    }
    if (message.origin === this.origin) return;

    this.logger.debug(
      `Deleted-contact signal from another process (contact ${message.contactId ?? 'unknown'})`,
    );
    const relayed: ContactDeletedSignal = {
      contactId: message.contactId,
      relayed: true,
    };
    this.eventEmitter.emit(CONTACT_DELETED_INGESTED_EVENT, relayed);
  }

  private createClient(options: { enableOfflineQueue?: boolean }): Redis {
    const redis = getProcessingConfig().redis;
    const client = new Redis({
      host: redis?.host || 'localhost',
      port: redis?.port || 6379,
      password: redis?.password,
      db: redis?.db ?? 5,
      ...(redis?.tls ? { tls: redis.tls } : {}),
      lazyConnect: true,
      maxRetriesPerRequest: 3,
      connectTimeout: 10000,
      ...options,
    });
    // One line per outage, not one per reconnect attempt.
    let down = false;
    client.on('error', (error) => {
      if (down) return;
      down = true;
      this.logger.error(`Deleted-contact signal Redis error: ${error.message}`);
    });
    client.on('ready', () => {
      if (down) this.logger.log('Deleted-contact signal Redis connection back');
      down = false;
    });
    return client;
  }
}

function parseRelayMessage(raw: string): RelayMessage | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;

  const { origin, contactId } = parsed as Record<string, unknown>;
  if (typeof origin !== 'string' || !origin) return null;
  return {
    origin,
    contactId: typeof contactId === 'string' ? contactId : undefined,
  };
}
