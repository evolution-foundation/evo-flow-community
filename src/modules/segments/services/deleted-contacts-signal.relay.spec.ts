import { Test } from '@nestjs/testing';
import { EventEmitter2, EventEmitterModule } from '@nestjs/event-emitter';
import { ClickHouseService } from '../../processing/clickhouse/clickhouse.service';
import { CONTACT_DELETED_INGESTED_EVENT } from '../queries/contact-event-names';
import { DeletedContactsCacheService } from './deleted-contacts-cache.service';
import { DeletedContactsSignalRelay } from './deleted-contacts-signal.relay';

type Handler = (...args: any[]) => void;

/** An in-memory Redis server shared by every fake client: pub/sub crosses "processes". */
const bus = {
  subscribers: new Set<FakeRedis>(),
  failPublish: false,
};

class FakeRedis {
  handlers = new Map<string, Handler[]>();
  channels = new Set<string>();

  on(event: string, handler: Handler) {
    this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]);
    return this;
  }

  emit(event: string, ...args: any[]) {
    (this.handlers.get(event) ?? []).forEach((h) => h(...args));
  }

  connect = jest.fn(() => {
    this.emit('ready');
    return Promise.resolve();
  });

  subscribe = jest.fn((channel: string) => {
    this.channels.add(channel);
    bus.subscribers.add(this);
    return Promise.resolve(1);
  });

  publish = jest.fn((channel: string, message: string) => {
    if (bus.failPublish) return Promise.reject(new Error('connection lost'));
    bus.subscribers.forEach((client) => {
      if (client.channels.has(channel))
        client.emit('message', channel, message);
    });
    return Promise.resolve(bus.subscribers.size);
  });

  disconnect = jest.fn();
}

jest.mock('ioredis', () => ({
  __esModule: true,
  default: jest.fn().mockImplementation(() => new FakeRedis()),
}));

/** One evo-flow process: its own emitter, cache and relay, wired as Nest would. */
function bootProcess() {
  const emitter = new EventEmitter2();
  const cache = new DeletedContactsCacheService({} as any);
  const relay = new DeletedContactsSignalRelay(emitter);
  const onCacheSignal = jest.spyOn(cache, 'onContactDeletedIngested');

  emitter.on(CONTACT_DELETED_INGESTED_EVENT, () =>
    cache.onContactDeletedIngested(),
  );
  emitter.on(CONTACT_DELETED_INGESTED_EVENT, (signal) => {
    void relay.onContactDeletedIngested(signal);
  });
  relay.onModuleInit();

  return { emitter, cache, relay, onCacheSignal };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('DeletedContactsSignalRelay', () => {
  beforeEach(() => {
    bus.subscribers.clear();
    bus.failPublish = false;
  });

  it('invalidates the cache of another process (api ingests, segment-worker recomputes)', async () => {
    const api = bootProcess();
    const worker = bootProcess();
    (worker.cache as any).cached = new Set(['stale']);
    (worker.cache as any).expiresAt = Number.MAX_SAFE_INTEGER;

    api.emitter.emit(CONTACT_DELETED_INGESTED_EVENT, { contactId: 'c-1' });
    await flush();

    expect(worker.onCacheSignal).toHaveBeenCalledTimes(1);
    expect((worker.cache as any).cached).toBeNull();
  });

  it('does not signal the originating process twice', async () => {
    const api = bootProcess();
    bootProcess();

    api.emitter.emit(CONTACT_DELETED_INGESTED_EVENT, { contactId: 'c-1' });
    await flush();

    expect(api.onCacheSignal).toHaveBeenCalledTimes(1);
  });

  it('does not publish again what it received from another process', async () => {
    const api = bootProcess();
    const worker = bootProcess();

    api.emitter.emit(CONTACT_DELETED_INGESTED_EVENT, { contactId: 'c-1' });
    await flush();

    const workerPublisher = (worker.relay as any).publisher as FakeRedis;
    expect(workerPublisher.publish).not.toHaveBeenCalled();
  });

  it('carries the contact id across processes', async () => {
    const api = bootProcess();
    const worker = bootProcess();
    const seen: unknown[] = [];
    worker.emitter.on(CONTACT_DELETED_INGESTED_EVENT, (signal) =>
      seen.push(signal),
    );

    api.emitter.emit(CONTACT_DELETED_INGESTED_EVENT, { contactId: 'c-1' });
    await flush();

    expect(seen).toEqual([{ contactId: 'c-1', relayed: true }]);
  });

  it('ignores malformed messages and messages without an origin', () => {
    const worker = bootProcess();
    const subscriber = (worker.relay as any).subscriber as FakeRedis;
    const channel = [...subscriber.channels][0];

    subscriber.emit('message', channel, 'not json');
    subscriber.emit('message', channel, JSON.stringify({ contactId: 'c-1' }));
    subscriber.emit('message', channel, 'null');

    expect(worker.onCacheSignal).not.toHaveBeenCalled();
  });

  it('ignores other channels', () => {
    const worker = bootProcess();
    const subscriber = (worker.relay as any).subscriber as FakeRedis;

    subscriber.emit(
      'message',
      'some-other-channel',
      JSON.stringify({ origin: 'elsewhere', contactId: 'c-1' }),
    );

    expect(worker.onCacheSignal).not.toHaveBeenCalled();
  });

  it('never throws when Redis fails to publish: the ingest goes on and caches fall back to the TTL', async () => {
    const api = bootProcess();
    bus.failPublish = true;

    await expect(
      api.relay.onContactDeletedIngested({ contactId: 'c-1' }),
    ).resolves.toBeUndefined();
  });

  it('namespaces the channel with the Redis db, since pub/sub ignores the db index', () => {
    const worker = bootProcess();
    const subscriber = (worker.relay as any).subscriber as FakeRedis;

    expect([...subscriber.channels]).toEqual([
      expect.stringMatching(/^evo-flow:db\d+:segments:contact-deleted$/),
    ]);
  });

  it('closes both connections on shutdown, without waiting on a Redis that may be down', () => {
    const worker = bootProcess();
    const { publisher, subscriber } = worker.relay as any;

    worker.relay.onModuleDestroy();

    expect((publisher as FakeRedis).disconnect).toHaveBeenCalled();
    expect((subscriber as FakeRedis).disconnect).toHaveBeenCalled();
  });

  it('logs one error per outage, not one per reconnect attempt', () => {
    const worker = bootProcess();
    const subscriber = (worker.relay as any).subscriber as FakeRedis;
    const logError = jest.spyOn((worker.relay as any).logger, 'error');

    subscriber.emit('error', new Error('ECONNREFUSED'));
    subscriber.emit('error', new Error('ECONNREFUSED'));
    subscriber.emit('error', new Error('ECONNREFUSED'));
    expect(logError).toHaveBeenCalledTimes(1);

    subscriber.emit('ready');
    subscriber.emit('error', new Error('ECONNREFUSED'));
    expect(logError).toHaveBeenCalledTimes(2);
  });
});

describe('DeletedContactsSignalRelay wired by Nest', () => {
  beforeEach(() => bus.subscribers.clear());

  it('the real @OnEvent publishes a local deletion once and never a relayed one', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [EventEmitterModule.forRoot()],
      providers: [
        DeletedContactsSignalRelay,
        DeletedContactsCacheService,
        { provide: ClickHouseService, useValue: {} },
      ],
    }).compile();
    await moduleRef.init();

    const emitter = moduleRef.get(EventEmitter2);
    const relay = moduleRef.get(DeletedContactsSignalRelay);
    const cache = moduleRef.get(DeletedContactsCacheService);
    const publisher = (relay as any).publisher as FakeRedis;
    (cache as any).cached = new Set(['stale']);

    emitter.emit(CONTACT_DELETED_INGESTED_EVENT, { contactId: 'c-1' });
    emitter.emit(CONTACT_DELETED_INGESTED_EVENT, {
      contactId: 'c-2',
      relayed: true,
    });
    await flush();

    expect(publisher.publish).toHaveBeenCalledTimes(1);
    expect(JSON.parse(publisher.publish.mock.calls[0][1])).toMatchObject({
      contactId: 'c-1',
    });
    expect((cache as any).cached).toBeNull();

    await moduleRef.close();
  });
});
