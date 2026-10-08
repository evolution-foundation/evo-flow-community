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
  createKafkaIntegration(databaseName: string, tableName: string): Promise<void>;
  ensureKafkaEngineBroker(
    databaseName: string,
    tableName: string,
    expectedBrokers: string,
    dependentViews?: string[],
    expectedGroup?: string,
  ): Promise<boolean>;
  extractKafkaBrokers(createTableQuery: string): string | null;
  query: jest.Mock;
  command: jest.Mock;
  logger: { log: jest.Mock; warn: jest.Mock; error: jest.Mock; debug: jest.Mock };
};

describe('ClickHouseService — contact-events broker guard (EVO-1925)', () => {
  let service: ClickHouseService;
  let internals: ServiceInternals;

  beforeEach(() => {
    service = new ClickHouseService();
    internals = service as unknown as ServiceInternals;
    internals.query = jest.fn().mockResolvedValue([]);
    internals.command = jest.fn().mockResolvedValue(undefined);
    internals.logger = {
      log: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    };
  });

  describe('ensureKafkaEngineBroker (contact_events_kafka_queue)', () => {
    it('does nothing when the table does not exist', async () => {
      internals.query.mockResolvedValue([]);

      const dropped = await internals.ensureKafkaEngineBroker(
        'evo_campaign',
        'contact_events_kafka_queue',
        'evo-campaign-kafka:29092',
        ['contact_events_kafka_mv'],
      );

      expect(dropped).toBe(false);
      expect(internals.command).not.toHaveBeenCalled();
    });

    it('does nothing when the broker already matches', async () => {
      internals.query.mockResolvedValue([
        {
          engine: 'Kafka',
          create:
            "ENGINE = Kafka('evo-campaign-kafka:29092', 'evo-campaign-events', 'evo-campaign-consumers-clickhouse', 'JSONEachRow')",
        },
      ]);

      const dropped = await internals.ensureKafkaEngineBroker(
        'evo_campaign',
        'contact_events_kafka_queue',
        'evo-campaign-kafka:29092',
        ['contact_events_kafka_mv'],
      );

      expect(dropped).toBe(false);
      expect(internals.command).not.toHaveBeenCalled();
    });

    it('drops the dependent MV and table when the broker is stale (localhost frozen)', async () => {
      internals.query.mockResolvedValue([
        {
          engine: 'Kafka',
          create:
            "ENGINE = Kafka('localhost:9092', 'evo-campaign-events', 'evo-campaign-consumers-clickhouse', 'JSONEachRow')",
        },
      ]);

      const dropped = await internals.ensureKafkaEngineBroker(
        'evo_campaign',
        'contact_events_kafka_queue',
        'evo-campaign-kafka:29092',
        ['contact_events_kafka_mv'],
      );

      expect(dropped).toBe(true);
      const commands = internals.command.mock.calls.map((c) => c[0].query);
      // MV must be dropped before the source Kafka table.
      expect(commands[0]).toContain(
        'DROP VIEW IF EXISTS evo_campaign.contact_events_kafka_mv',
      );
      expect(commands[1]).toContain(
        'DROP TABLE IF EXISTS evo_campaign.contact_events_kafka_queue',
      );
      expect(internals.logger.warn).toHaveBeenCalledWith(
        expect.stringContaining('stale broker'),
      );
    });

    it('does not block boot when validation throws', async () => {
      internals.query.mockRejectedValue(new Error('clickhouse down'));

      const dropped = await internals.ensureKafkaEngineBroker(
        'evo_campaign',
        'contact_events_kafka_queue',
        'evo-campaign-kafka:29092',
        ['contact_events_kafka_mv'],
      );

      expect(dropped).toBe(false);
      expect(internals.command).not.toHaveBeenCalled();
      expect(internals.logger.error).toHaveBeenCalled();
    });
  });

  describe('createKafkaIntegration wiring', () => {
    it('guards the broker before creating the Kafka queue table', async () => {
      const ensureSpy = jest
        .spyOn(internals as any, 'ensureKafkaEngineBroker')
        .mockResolvedValue(false);

      await internals.createKafkaIntegration('evo_campaign', 'contact_events');

      expect(ensureSpy).toHaveBeenCalledWith(
        'evo_campaign',
        'contact_events_kafka_queue',
        'evo-campaign-kafka:29092',
        ['contact_events_kafka_mv'],
        'evo-campaign-consumers-clickhouse',
      );

      // Guard must run before the CREATE TABLE command for the Kafka queue.
      const createQueueIdx = internals.command.mock.calls.findIndex((c) =>
        c[0].query.includes('contact_events_kafka_queue'),
      );
      expect(createQueueIdx).toBeGreaterThanOrEqual(0);
      const ensureOrder = ensureSpy.mock.invocationCallOrder[0];
      const createOrder =
        internals.command.mock.invocationCallOrder[createQueueIdx];
      expect(ensureOrder).toBeLessThan(createOrder);
    });

    it('logs the broker the contact_events queue ended up bound to', async () => {
      jest
        .spyOn(internals as any, 'ensureKafkaEngineBroker')
        .mockResolvedValue(false);
      internals.query.mockResolvedValue([
        {
          create:
            "ENGINE = Kafka('evo-campaign-kafka:29092', 'evo-campaign-events', 'evo-campaign-consumers-clickhouse', 'JSONEachRow')",
        },
      ]);

      await internals.createKafkaIntegration('evo_campaign', 'contact_events');

      expect(internals.logger.log).toHaveBeenCalledWith(
        expect.stringContaining(
          "is bound to broker 'evo-campaign-kafka:29092'",
        ),
      );
    });
  });
});
