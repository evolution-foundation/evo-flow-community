jest.mock('@clickhouse/client', () => ({
  createClient: jest.fn(),
}));

jest.mock('../config/processing.config', () => ({
  getProcessingConfig: jest.fn(() => ({
    clickhouse: {
      protocol: 'http',
      host: 'localhost',
      port: 8123,
      database: 'evo_campaign',
      username: 'default',
      password: '',
    },
    kafka: {
      brokersInternal: 'evo-campaign-kafka:29092',
      topic: 'evo-campaign-events',
      groupId: 'evo-campaign-consumers',
    },
  })),
}));

import { ClickHouseService } from './clickhouse.service';

type ServiceInternals = {
  createKafkaIntegration(
    databaseName: string,
    tableName: string,
  ): Promise<void>;
  ensureContactEventsViewKeepsMilliseconds(
    databaseName: string,
    viewName: string,
  ): Promise<boolean>;
  ensureKafkaEngineBroker(...args: unknown[]): Promise<boolean>;
  query: jest.Mock<Promise<Array<{ create: string }>>>;
  command: jest.Mock<Promise<void>, [{ query: string }]>;
  logger: {
    log: jest.Mock;
    warn: jest.Mock;
    error: jest.Mock;
    debug: jest.Mock;
  };
};

const TRUNCATING_VIEW =
  'CREATE MATERIALIZED VIEW evo_campaign.contact_events_kafka_mv TO evo_campaign.contact_events AS SELECT ' +
  'parseDateTimeBestEffort(occurred_at) AS occurred_at, parseDateTimeBestEffort(processing_time) AS processing_time ' +
  'FROM evo_campaign.contact_events_kafka_queue';
const MILLISECOND_VIEW =
  'CREATE MATERIALIZED VIEW evo_campaign.contact_events_kafka_mv TO evo_campaign.contact_events AS SELECT ' +
  'parseDateTime64BestEffort(occurred_at, 3) AS occurred_at, parseDateTime64BestEffort(processing_time, 3) AS processing_time ' +
  'FROM evo_campaign.contact_events_kafka_queue';

describe('ClickHouseService — contact_events view keeps milliseconds', () => {
  let internals: ServiceInternals;

  beforeEach(() => {
    internals = new ClickHouseService() as unknown as ServiceInternals;
    internals.query = jest.fn(() => Promise.resolve([]));
    internals.command = jest.fn<Promise<void>, [{ query: string }]>(() =>
      Promise.resolve(),
    );
    internals.logger = {
      log: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    };
  });

  const commands = () => internals.command.mock.calls.map(([c]) => c.query);
  const check = () =>
    internals.ensureContactEventsViewKeepsMilliseconds(
      'evo_campaign',
      'contact_events_kafka_mv',
    );

  it('does nothing when the view does not exist yet', async () => {
    await expect(check()).resolves.toBe(false);
    expect(internals.command).not.toHaveBeenCalled();
  });

  it('drops a view created by an older boot that truncates to seconds', async () => {
    internals.query.mockResolvedValue([{ create: TRUNCATING_VIEW }]);

    await expect(check()).resolves.toBe(true);
    expect(commands()).toEqual([
      'DROP VIEW IF EXISTS evo_campaign.contact_events_kafka_mv',
    ]);
  });

  it('drops a view that keeps milliseconds for only one column', async () => {
    internals.query.mockResolvedValue([
      {
        create: MILLISECOND_VIEW.replace(
          'parseDateTime64BestEffort(processing_time, 3)',
          'parseDateTimeBestEffort(processing_time)',
        ),
      },
    ]);

    await expect(check()).resolves.toBe(true);
  });

  it('keeps a view that already parses milliseconds', async () => {
    internals.query.mockResolvedValue([{ create: MILLISECOND_VIEW }]);

    await expect(check()).resolves.toBe(false);
    expect(internals.command).not.toHaveBeenCalled();
  });

  it('does not block boot when the check fails', async () => {
    internals.query.mockRejectedValue(new Error('clickhouse down'));

    await expect(check()).resolves.toBe(false);
    expect(internals.command).not.toHaveBeenCalled();
    expect(internals.logger.error).toHaveBeenCalled();
  });

  it('creates the view with millisecond timestamps, after dropping the old one', async () => {
    jest.spyOn(internals, 'ensureKafkaEngineBroker').mockResolvedValue(false);
    internals.query.mockResolvedValue([{ create: TRUNCATING_VIEW }]);

    await internals.createKafkaIntegration('evo_campaign', 'contact_events');

    const all = commands();
    const drop = all.indexOf(
      'DROP VIEW IF EXISTS evo_campaign.contact_events_kafka_mv',
    );
    const create = all.findIndex((q) =>
      q.includes('CREATE MATERIALIZED VIEW IF NOT EXISTS'),
    );
    expect(drop).toBeGreaterThanOrEqual(0);
    expect(create).toBeGreaterThan(drop);
    expect(all[create]).toContain(
      'parseDateTime64BestEffort(occurred_at, 3) as occurred_at',
    );
    expect(all[create]).toContain(
      'parseDateTime64BestEffort(processing_time, 3) as processing_time',
    );
    expect(all[create]).not.toContain('parseDateTimeBestEffort(');
  });
});
