import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import {
  createClient,
  ClickHouseClient,
  ClickHouseSettings,
  DataFormat,
} from '@clickhouse/client';
import { getProcessingConfig } from '../config/processing.config';
import { v4 as uuidv4 } from 'uuid';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';

export interface ClickHouseQueryBuilder {
  query: string;
  parameters: Record<string, unknown>;
}

@Injectable()
export class ClickHouseService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new CustomLoggerService(ClickHouseService.name);
  private client: ClickHouseClient | null = null;
  private readonly config = getProcessingConfig();
  private readonly instanceId = Math.random().toString(36).substring(7);

  async onModuleInit() {
    this.logger.log(
      `🆔 ClickHouse instance ${this.instanceId} onModuleInit called`,
    );
    await this.connect();
    await this.initializeDatabase();
  }

  async onModuleDestroy() {
    if (this.client) {
      await this.client.close();
      this.logger.log('ClickHouse client closed');
    }
  }

  private async connect() {
    try {
      // First, connect without specifying database to create it
      const protocol = this.config.clickhouse?.protocol || 'http';
      this.client = createClient({
        url: `${protocol}://${this.config.clickhouse?.host || 'localhost'}:${this.config.clickhouse?.port || 8123}`,
        username: this.config.clickhouse?.username || 'default',
        password: this.config.clickhouse?.password || '',
        request_timeout: 180000, // 3 minutes
        clickhouse_settings: {
          date_time_input_format: 'best_effort',
          input_format_skip_unknown_fields: 1,
          max_bytes_ratio_before_external_group_by: 0.5,
        } as ClickHouseSettings,
      });

      // Test connection
      const result = await this.client.query({
        query: 'SELECT version()',
        format: 'JSONEachRow',
      });

      const rows = await result.json<{ version: string }>();
      this.logger.log(`Connected to ClickHouse version: ${rows[0]?.version}`);
    } catch (error) {
      this.logger.error(
        `Failed to connect to ClickHouse: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  private async initializeDatabase() {
    try {
      this.logger.log('Initializing ClickHouse database and tables...');

      const databaseName = this.config.clickhouse?.database || 'evo_campaign';

      // 1. Criar database se não existir
      try {
        await this.command({
          query: `CREATE DATABASE IF NOT EXISTS ${databaseName}`,
        });
        this.logger.log(`✅ Database '${databaseName}' ensured`);
      } catch (error) {
        this.logger.error(`❌ Failed to create database: ${error.message}`, error.stack);
        throw error;
      }

      // 2. Reconnect with the specific database
      if (this.client) {
        await this.client.close();
      }
      this.client = createClient({
        url: `${this.config.clickhouse?.protocol || 'http'}://${this.config.clickhouse?.host || 'localhost'}:${this.config.clickhouse?.port || 8123}`,
        database: databaseName,
        username: this.config.clickhouse?.username || 'default',
        password: this.config.clickhouse?.password || '',
        request_timeout: 180000, // 3 minutes
        clickhouse_settings: {
          date_time_input_format: 'best_effort',
          input_format_skip_unknown_fields: 1,
          max_bytes_ratio_before_external_group_by: 0.5,
        } as ClickHouseSettings,
      });
      this.logger.log(
        `Reconnected to ClickHouse with database '${databaseName}'`,
      );

      // 3. Criar tabela principal de eventos (ANTES da integração Kafka)
      try {
        await this.createContactEventsTable(databaseName, 'contact_events');
        this.logger.log(`✅ Contact events table creation completed`);
      } catch (error) {
        this.logger.error(`❌ Failed to create contact events table: ${error.message}`, error.stack);
        throw error;
      }

      // 4. Criar integração Kafka (DEPOIS da tabela, pois a MV depende dela)
      try {
        await this.createKafkaIntegration(databaseName, 'contact_events');
        this.logger.log(`✅ Kafka integration creation completed`);
      } catch (error) {
        this.logger.error(`❌ Failed to create Kafka integration: ${error.message}`, error.stack);
        // Don't throw - Kafka integration is optional for ClickHouse Cloud
        this.logger.warn('⚠️ Continuing without Kafka integration (ClickHouse Cloud may not support Kafka Engine)');
      }

      // 5. Criar tabelas para segment computation
      try {
        await this.createSegmentTables(databaseName);
        this.logger.log(`✅ Segment tables creation completed`);
      } catch (error) {
        this.logger.error(`❌ Failed to create segment tables: ${error.message}`, error.stack);
        throw error;
      }

      // 6. Criar fila de triggers para journeys
      try {
        await this.createJourneyTriggerQueue(databaseName);
        this.logger.log(`✅ Journey trigger queue creation completed`);
      } catch (error) {
        this.logger.error(`❌ Failed to create journey trigger queue: ${error.message}`, error.stack);
        // Don't throw - journey triggers are optional
        this.logger.warn('⚠️ Continuing without journey trigger queue');
      }

      // 7. Listar todas as tabelas criadas
      try {
        const tables = await this.query<{ name: string }>({
          query: `SHOW TABLES FROM ${databaseName}`,
        });
        this.logger.log(`📋 Tables in database '${databaseName}': ${tables.map(t => t.name).join(', ') || 'none'}`);
      } catch (error) {
        this.logger.error(`❌ Failed to list tables: ${error.message}`, error.stack);
      }

      // 8. Verificar se as tabelas foram criadas
      try {
        await this.verifyTablesExist(databaseName, 'contact_events');
        this.logger.log(`✅ Table verification completed`);
      } catch (error) {
        this.logger.error(`❌ Failed to verify tables: ${error.message}`, error.stack);
        // Don't throw - verification is just a check
      }

      // Verify client state after initialization
      if (this.client) {
        this.logger.log(
          '✅ ClickHouse client is properly set and ready for operations',
        );
      } else {
        this.logger.error(
          '❌ ClickHouse client is NULL after initialization - this should not happen!',
        );
      }

      this.logger.log(
        `✅ ClickHouse instance ${this.instanceId} database initialization completed successfully`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to initialize ClickHouse database: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  getClient(): ClickHouseClient {
    if (!this.client) {
      this.logger.error(
        `❌ getClient() called but this.client is null on instance ${this.instanceId}!`,
      );
      this.logger.error('Stack trace:', new Error().stack);
      throw new Error('ClickHouse client not initialized');
    }
    return this.client;
  }

  async query<T = any>(params: {
    query: string;
    parameters?: Record<string, unknown>;
    format?: DataFormat;
  }): Promise<T[]> {
    const queryId = this.generateQueryId();

    try {
      this.logger.debug(`Executing query ${queryId}: ${params.query}`);

      const result = await this.getClient().query({
        query: params.query,
        format: params.format || 'JSONEachRow',
        query_params: params.parameters,
        query_id: queryId,
      });

      const rows = await result.json<T>();
      this.logger.debug(
        `Query ${queryId} returned ${Array.isArray(rows) ? rows.length : 'non-array'} rows`,
      );

      return Array.isArray(rows) ? rows : ([rows] as T[]);
    } catch (error) {
      this.logger.error(
        `Query ${queryId} failed: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async insert(params: {
    table: string;
    values: any[];
    format?: DataFormat;
    asyncInsert?: boolean;
  }): Promise<void> {
    const queryId = this.generateQueryId();

    try {
      this.logger.debug(
        `Inserting ${params.values.length} records into ${params.table} (${queryId})`,
      );

      const settings: ClickHouseSettings = {};

      if (params.asyncInsert) {
        // Async mode ch-async
        settings.async_insert = 1;
        settings.wait_for_async_insert = 1;
        settings.async_insert_busy_timeout_ms = 1000;
      } else {
        // Sync mode ch-sync
        settings.wait_end_of_query = 1;
      }

      await this.getClient().insert({
        table: params.table,
        values: params.values,
        format: params.format || 'JSONEachRow',
        clickhouse_settings: settings,
        query_id: queryId,
      });

      this.logger.log(
        `Successfully inserted ${params.values.length} records into ${params.table} (${queryId})`,
      );
    } catch (error) {
      this.logger.error(
        `Insert ${queryId} failed: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async command(params: {
    query: string;
    parameters?: Record<string, unknown>;
  }): Promise<void> {
    const queryId = this.generateQueryId();

    try {
      this.logger.debug(`Executing command ${queryId}: ${params.query}`);

      await this.getClient().command({
        query: params.query,
        query_params: params.parameters,
        query_id: queryId,
        clickhouse_settings: {
          wait_end_of_query: 1,
        },
      });

      this.logger.debug(`Command ${queryId} executed successfully`);
    } catch (error) {
      this.logger.error(
        `Command ${queryId} failed: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  createQueryBuilder(): ClickHouseQueryBuilderImpl {
    return new ClickHouseQueryBuilderImpl();
  }

  private generateQueryId(): string {
    return uuidv4().replace(/-/g, '_');
  }

  async healthCheck(): Promise<boolean> {
    try {
      await this.query({ query: 'SELECT 1' });
      return true;
    } catch (error) {
      this.logger.error(`ClickHouse health check failed: ${error.message}`);
      return false;
    }
  }

  getConfig() {
    return {
      host: this.config.clickhouse?.host || 'localhost',
      port: this.config.clickhouse?.port || 8123,
      database: this.config.clickhouse?.database || 'evo_campaign',
      table: this.config.clickhouse?.table || 'contact_events',
      connected: !!this.client,
    };
  }

  /**
   * Guard a Kafka Engine table against a stale broker "frozen" by a previous boot.
   *
   * Kafka Engine tables created with `CREATE TABLE IF NOT EXISTS` "freeze" the
   * broker string from the first boot. If that boot lacked KAFKA_BROKERS_INTERNAL
   * the table is stuck on 'localhost:9092' forever — ClickHouse can't reach Kafka
   * and the topic silently receives nothing (EVO-1893/EVO-1925). This reads the
   * existing DDL, and if the broker diverges from `expectedBrokers` (or the
   * consumer group from `expectedGroup`, when given), drops the dependent
   * materialized views and the Kafka table so the caller can recreate them.
   *
   * @returns true if the table was dropped (stale), false if absent or already correct.
   */
  private async ensureKafkaEngineBroker(
    databaseName: string,
    tableName: string,
    expectedBrokers: string,
    dependentViews: string[] = [],
    expectedGroup?: string,
  ): Promise<boolean> {
    try {
      const rows = await this.query<{ engine: string; create: string }>({
        query: `
          SELECT engine, create_table_query AS create
          FROM system.tables
          WHERE database = {database:String} AND name = {table:String}
        `,
        parameters: { database: databaseName, table: tableName },
      });

      const existing = rows?.[0];

      // Table doesn't exist yet → nothing to fix, caller will create it fresh.
      if (!existing) {
        return false;
      }

      // Only Kafka Engine tables carry a frozen broker; anything else is left alone.
      if (existing.engine !== 'Kafka') {
        return false;
      }

      const currentBrokers = this.extractKafkaBrokers(existing.create);
      const currentGroup = this.extractKafkaGroup(existing.create);
      const staleBroker = currentBrokers !== expectedBrokers;
      const staleGroup =
        expectedGroup !== undefined && currentGroup !== expectedGroup;

      if (!staleBroker && !staleGroup) {
        this.logger.log(
          `Kafka Engine table '${databaseName}.${tableName}' already points at the configured broker ('${expectedBrokers}')` +
            (expectedGroup ? ` and group ('${expectedGroup}')` : ''),
        );
        return false;
      }

      if (staleBroker) {
        this.logger.warn(
          `⚠️ Kafka Engine table '${databaseName}.${tableName}' is bound to a stale broker ` +
            `('${currentBrokers ?? 'unknown'}') but the configured broker is '${expectedBrokers}'. ` +
            `Recreating it so the pipeline reaches Kafka (EVO-1893/EVO-1925).`,
        );
      }
      if (staleGroup) {
        this.logger.warn(
          `⚠️ Kafka Engine table '${databaseName}.${tableName}' uses consumer group ` +
            `'${currentGroup ?? 'unknown'}' but the configured group is '${expectedGroup}'. Recreating it.`,
        );
      }

      // Drop dependent materialized views first (they block dropping the source).
      for (const view of dependentViews) {
        await this.command({
          query: `DROP VIEW IF EXISTS ${databaseName}.${view}`,
        });
        this.logger.log(`Dropped stale dependent view '${databaseName}.${view}'`);
      }

      await this.command({
        query: `DROP TABLE IF EXISTS ${databaseName}.${tableName}`,
      });
      this.logger.log(
        `Dropped stale Kafka Engine table '${databaseName}.${tableName}'`,
      );

      return true;
    } catch (error) {
      // Don't block boot on the validation itself; recreating is best-effort.
      this.logger.error(
        `Failed to validate broker for '${databaseName}.${tableName}': ${error.message}`,
        error.stack,
      );
      return false;
    }
  }

  /**
   * Extract the broker list from a Kafka Engine DDL, e.g.
   * `... ENGINE = Kafka('broker:9092', 'topic', ...)` → `broker:9092`.
   */
  private extractKafkaBrokers(createTableQuery: string): string | null {
    const match = createTableQuery.match(/Kafka\(\s*'([^']*)'/i);
    return match ? match[1] : null;
  }

  /**
   * Extract the consumer group (third argument) from a Kafka Engine DDL.
   */
  private extractKafkaGroup(createTableQuery: string): string | null {
    const match = createTableQuery.match(
      /Kafka\(\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'([^']*)'/i,
    );
    return match ? match[1] : null;
  }

  /**
   * Log the broker the contact-events Kafka Engine table is actually bound to.
   * The Kafka Engine consumes/produces in the background and does not raise to
   * this Node process, so without this the only failure mode is a silently empty
   * pipeline (EVO-1925).
   */
  private async logContactEventsKafkaState(
    databaseName: string,
    tableName: string,
  ) {
    try {
      const rows = await this.query<{ create: string }>({
        query: `
          SELECT create_table_query AS create
          FROM system.tables
          WHERE database = {database:String} AND name = {table:String}
        `,
        parameters: { database: databaseName, table: `${tableName}_kafka_queue` },
      });

      const brokers = rows?.[0]
        ? this.extractKafkaBrokers(rows[0].create)
        : null;
      this.logger.log(
        `Contact-events Kafka Engine table '${databaseName}.${tableName}_kafka_queue' is bound to broker '${brokers ?? 'unknown'}'`,
      );
    } catch (error) {
      this.logger.warn(
        `Could not inspect contact-events Kafka Engine state: ${error.message}`,
      );
    }
  }

  /**
   * Create Kafka integration tables
   * This creates a Kafka Engine table that reads from Kafka
   * and a Materialized View that moves data to the main table
   */
  async createKafkaIntegration(databaseName: string, tableName: string) {
    try {
      this.logger.log('Creating Kafka integration for ClickHouse...');

      // Use internal broker address for ClickHouse Kafka Engine
      this.logger.log(`🔍 DEBUG - KAFKA_BROKERS_INTERNAL from env: ${process.env.KAFKA_BROKERS_INTERNAL}`);
      this.logger.log(`🔍 DEBUG - this.config.kafka?.brokersInternal: ${this.config.kafka?.brokersInternal}`);

      const kafkaBrokers = this.config.kafka?.brokersInternal || 'kafka:29092';
      const kafkaTopic = this.config.kafka?.topic || 'evo-campaign-events';
      const kafkaGroupId = `${this.config.kafka?.groupId || 'evo-campaign-consumers'}-clickhouse`;

      this.logger.log(`Using Kafka brokers for ClickHouse: ${kafkaBrokers}`);

      // 0. Guard against a stale broker "frozen" by a previous boot (EVO-1925).
      //    The Kafka table is created with `IF NOT EXISTS`, so if a first boot ran
      //    without KAFKA_BROKERS_INTERNAL it baked in 'localhost:9092' and every
      //    later boot keeps the wrong broker → ClickHouse can't reach Kafka, the
      //    topic stays empty and contact_events ingestion silently stalls.
      //    Detect divergence and DROP+recreate so the configured broker wins.
      await this.ensureKafkaEngineBroker(
        databaseName,
        `${tableName}_kafka_queue`,
        kafkaBrokers,
        // The MV reads from the Kafka queue and writes into the main table; it
        // must be dropped before the underlying Kafka table can be dropped.
        [`${tableName}_kafka_mv`],
        kafkaGroupId,
      );

      // 1. Create Kafka queue table
      const createKafkaTableQuery = `
        CREATE TABLE IF NOT EXISTS ${databaseName}.${tableName}_kafka_queue (
          contact_id String,
          event_type String,
          event_name String,
          properties String,
          traits String,
          context String,
          anonymous_id Nullable(String),
          message_id Nullable(String),
          occurred_at String,
          processing_time String,
          message_raw String
        )
        ENGINE = Kafka('${kafkaBrokers}', '${kafkaTopic}', '${kafkaGroupId}', 'JSONEachRow')
        SETTINGS 
          kafka_thread_per_consumer = 1,
          kafka_num_consumers = 3,
          kafka_max_block_size = 1048576,
          kafka_skip_broken_messages = 100,
          kafka_commit_every_batch = 1
      `;

      await this.command({
        query: createKafkaTableQuery,
      });

      this.logger.log(`Kafka queue table '${tableName}_kafka_queue' created`);

      // 2. Create Materialized View to move data from Kafka to main table
      const createMaterializedViewQuery = `
        CREATE MATERIALIZED VIEW IF NOT EXISTS ${databaseName}.${tableName}_kafka_mv
        TO ${databaseName}.${tableName}
        AS SELECT
          generateUUIDv4() as id,
          contact_id,
          CAST(event_type AS Enum8('identify' = 1, 'track' = 2, 'page' = 3, 'screen' = 4, 'segment' = 5)) as event_type,
          event_name,
          properties,
          traits,
          anonymous_id,
          message_id,
          parseDateTimeBestEffort(occurred_at) as occurred_at,
          parseDateTimeBestEffort(processing_time) as processing_time,
          message_raw,
          CASE 
            WHEN length(contact_id) > 0 THEN contact_id
            ELSE anonymous_id
          END as contact_or_anonymous_id
        FROM ${databaseName}.${tableName}_kafka_queue
        WHERE length(contact_id) > 0 OR length(anonymous_id) > 0
      `;

      await this.command({
        query: createMaterializedViewQuery,
      });

      this.logger.log(`Materialized view '${tableName}_kafka_mv' created`);

      // 3. Create a view to monitor Kafka consumer lag (optional but useful)
      // DISABLED: Requires newer ClickHouse version for kafka_topic column
      /*
      const createMonitoringViewQuery = `
        CREATE OR REPLACE VIEW ${databaseName}.${tableName}_kafka_lag AS
        SELECT
          database,
          table,
          kafka_topic,
          kafka_consumer_group,
          kafka_partition,
          kafka_committed_offset,
          kafka_current_offset,
          kafka_current_offset - kafka_committed_offset as lag
        FROM system.kafka_consumers
        WHERE database = '${databaseName}' 
          AND table = '${tableName}_kafka_queue'
      `;
      
      await this.command({
        query: createMonitoringViewQuery,
      });
      */

      // Surface what broker the live table actually ended up bound to, so a
      // stale/unreachable broker is visible in the logs instead of failing
      // silently (the ClickHouse Kafka Engine produces/consumes in the
      // background and swallows connection errors). EVO-1925.
      await this.logContactEventsKafkaState(databaseName, tableName);

      this.logger.log(
        '✅ Kafka integration for ClickHouse created successfully',
      );
    } catch (error) {
      this.logger.error(
        `Failed to create Kafka integration: ${error.message}`,
        error.stack,
      );
      // Don't throw - allow system to continue without Kafka integration
      this.logger.warn(
        'System will continue without Kafka-ClickHouse integration',
      );
    }
  }

  async createContactEventsTable(databaseName: string, tableName: string) {
    try {
      this.logger.log(
        `Creating table '${databaseName}.${tableName}' with proper schema`,
      );

      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${databaseName}.${tableName} (
          id UUID DEFAULT generateUUIDv4(),
          contact_id String,
          event_type Enum8('identify' = 1, 'track' = 2, 'page' = 3, 'screen' = 4, 'segment' = 5),
          event_name String,
          properties String,  -- JSON as string
          traits String,      -- JSON as string
          anonymous_id Nullable(String),
          message_id Nullable(String),
          occurred_at DateTime64(3),
          processing_time DateTime64(3),
          message_raw String, -- JSON as string
          contact_or_anonymous_id String,

          -- Indexes for common queries
          INDEX idx_contact_id contact_id TYPE bloom_filter(0.01) GRANULARITY 1,
          INDEX idx_event_type event_type TYPE minmax GRANULARITY 1,
          INDEX idx_event_name event_name TYPE bloom_filter(0.01) GRANULARITY 1,
          INDEX idx_occurred_at occurred_at TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(occurred_at)
        ORDER BY (occurred_at, event_type)
        TTL occurred_at + INTERVAL 365 DAY
        SETTINGS index_granularity = 8192
      `;

      await this.command({
        query: createTableQuery,
      });

      this.logger.log(
        `Table '${databaseName}.${tableName}' ensured with proper schema`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to create contact events table: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async createSegmentTables(databaseName: string) {
    try {
      this.logger.log('Creating segment computation tables...');

      // 1. Computed property state table (adapted for Contact entity)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.computed_property_state_v2 (
            type Enum('contact_property' = 1, 'segment' = 2),
            computed_property_id LowCardinality(String),
            state_id LowCardinality(String),
            contact_id String,
            last_value AggregateFunction(argMax, String, DateTime64(3)),
            unique_count AggregateFunction(uniq, String),
            event_time DateTime64(3),
            grouped_message_ids AggregateFunction(groupArray, String),
            computed_at DateTime64(3)
          ) ENGINE = AggregatingMergeTree()
          ORDER BY (type, computed_property_id, state_id, contact_id, event_time)
          PARTITION BY toYYYYMM(computed_at)
          SETTINGS index_granularity = 8192
        `,
      });

      // 2. Computed property assignments table (adapted for Contact entity)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.computed_property_assignments_v2 (
            type Enum('contact_property' = 1, 'segment' = 2),
            computed_property_id LowCardinality(String),
            contact_id String,
            segment_value Boolean,
            contact_property_value String,
            max_event_time DateTime64(3),
            assigned_at DateTime64(3) DEFAULT now64(3)
          ) ENGINE = ReplacingMergeTree()
          ORDER BY (type, computed_property_id, contact_id)
          PARTITION BY toYYYYMM(assigned_at)
          SETTINGS index_granularity = 8192
        `,
      });

      // 3. Processed properties tracking table (adapted for Contact entity)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.processed_computed_properties_v2 (
            contact_id String,
            type Enum('contact_property' = 1, 'segment' = 2),
            computed_property_id LowCardinality(String),
            processed_for LowCardinality(String),
            processed_for_type LowCardinality(String),
            segment_value Boolean,
            contact_property_value String,
            max_event_time DateTime64(3),
            processed_at DateTime64(3) DEFAULT now64(3)
          ) ENGINE = ReplacingMergeTree()
          ORDER BY (computed_property_id, processed_for_type, processed_for, contact_id)
          PARTITION BY toYYYYMM(processed_at)
          SETTINGS index_granularity = 8192
        `,
      });

      // 4. Updated state tracking (temporary table with TTL - adapted for Contact entity)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.updated_computed_property_state (
            type Enum('contact_property' = 1, 'segment' = 2),
            computed_property_id LowCardinality(String),
            state_id LowCardinality(String),
            contact_id String,
            computed_at DateTime64(3)
          ) ENGINE = MergeTree
          PARTITION BY toYYYYMMDD(computed_at)
          ORDER BY computed_at
          TTL toStartOfDay(computed_at) + INTERVAL 24 HOUR
          SETTINGS index_granularity = 8192
        `,
      });

      // 5. Updated assignments tracking (temporary table with TTL - adapted for Contact entity)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.updated_property_assignments_v2 (
            type Enum('contact_property' = 1, 'segment' = 2),
            computed_property_id LowCardinality(String),
            contact_id String,
            assigned_at DateTime64(3)
          ) ENGINE = MergeTree
          PARTITION BY toYYYYMMDD(assigned_at)
          ORDER BY assigned_at
          TTL toStartOfDay(assigned_at) + INTERVAL 24 HOUR
          SETTINGS index_granularity = 8192
        `,
      });

      // 6. State index table for performance (adapted for Contact entity)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.computed_property_state_index (
            type Enum('contact_property' = 1, 'segment' = 2),
            computed_property_id LowCardinality(String),
            state_id LowCardinality(String),
            contact_id String,
            indexed_value Int64,
            INDEX primary_idx indexed_value TYPE minmax GRANULARITY 4
          ) ENGINE = ReplacingMergeTree()
          ORDER BY (type, computed_property_id, state_id, contact_id)
          PARTITION BY toYYYYMM(toDateTime(indexed_value))
          SETTINGS index_granularity = 8192
        `,
      });

      // 7. Resolved segment state table (cache final para performance)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.resolved_segment_state (
            segment_id LowCardinality(String),
            state_id LowCardinality(String),
            contact_id String,
            segment_state_value Boolean,
            max_event_time DateTime64(3),
            INDEX segment_state_value_idx segment_state_value TYPE minmax GRANULARITY 4,
            computed_at DateTime64(3),
            INDEX computed_at_idx computed_at TYPE minmax GRANULARITY 4
          ) ENGINE = ReplacingMergeTree()
          ORDER BY (
            segment_id,
            state_id,
            contact_id
          )
          PARTITION BY toYYYYMM(computed_at)
          SETTINGS index_granularity = 8192
        `,
      });

      // 8. Group contact assignments (grupos → contatos)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.group_contact_assignments (
            group_id String,
            contact_id String,
            assigned Boolean,
            assigned_at DateTime64(3) DEFAULT now64(3)
          ) ENGINE = ReplacingMergeTree()
          ORDER BY (
            group_id,
            contact_id
          )
          PARTITION BY toYYYYMM(assigned_at)
          SETTINGS index_granularity = 8192
        `,
      });

      // 9. Contact group assignments (contatos → grupos)
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.contact_group_assignments (
            group_id LowCardinality(String),
            contact_id LowCardinality(String),
            assigned Boolean,
            assigned_at DateTime64(3) DEFAULT now64(3)
          ) ENGINE = ReplacingMergeTree()
          ORDER BY (
            contact_id,
            group_id
          )
          PARTITION BY toYYYYMM(assigned_at)
          SETTINGS index_granularity = 8192
        `,
      });

      // 10. Create Materialized Views to populate tracking tables
      await this.createTrackingMaterializedViews(databaseName);

      this.logger.log('✅ All segment computation tables created successfully');
    } catch (error) {
      this.logger.error(
        `Failed to create segment tables: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  async createTrackingMaterializedViews(databaseName: string) {
    try {
      this.logger.log('Creating tracking materialized views...');

      // 1. Materialized View para updated_property_assignments_v2
      await this.command({
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS ${databaseName}.updated_property_assignments_v2_mv
          TO ${databaseName}.updated_property_assignments_v2
          AS SELECT
            type,
            computed_property_id,
            contact_id,
            assigned_at
          FROM ${databaseName}.computed_property_assignments_v2
          GROUP BY
            type,
            computed_property_id,
            contact_id,
            assigned_at
        `,
      });

      // 2. Materialized View para updated_computed_property_state
      await this.command({
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS ${databaseName}.updated_computed_property_state_v2_mv
          TO ${databaseName}.updated_computed_property_state
          AS SELECT
            type,
            computed_property_id,
            state_id,
            contact_id,
            computed_at
          FROM ${databaseName}.computed_property_state_v2
          GROUP BY
            type,
            computed_property_id,
            state_id,
            contact_id,
            computed_at
        `,
      });

      this.logger.log('✅ Tracking materialized views created successfully');
    } catch (error) {
      this.logger.error(
        `Failed to create tracking materialized views: ${error.message}`,
        error.stack,
      );
      // Don't throw - allow system to continue without materialized views
      this.logger.warn(
        'System will continue without tracking materialized views',
      );
    }
  }

  async verifyTablesExist(databaseName: string, tableName: string) {
    try {
      const checkTableQuery = `
        SELECT count(*) as count 
        FROM system.tables 
        WHERE database = {database:String} 
        AND name = {table:String}
      `;

      const result = await this.query({
        query: checkTableQuery,
        parameters: {
          database: databaseName,
          table: tableName,
        },
      });

      if (result && result[0]?.count > 0) {
        this.logger.log(
          `✅ Table ${databaseName}.${tableName} verified successfully`,
        );
      } else {
        this.logger.warn(
          `⚠️ Table ${databaseName}.${tableName} verification failed`,
        );
      }
    } catch (error) {
      this.logger.warn(`Table verification failed: ${error.message}`);
    }
  }

  /**
   * Create Journey Trigger Queue infrastructure
   * This creates Kafka engine table and materialized view to feed ALL events to journey triggers
   */
  async createJourneyTriggerQueue(databaseName: string) {
    try {
      this.logger.log(
        'Creating Journey Trigger Queue for Temporal workflows...',
      );

      this.logger.log(`🔍 DEBUG - KAFKA_BROKERS_INTERNAL from env: ${process.env.KAFKA_BROKERS_INTERNAL}`);
      this.logger.log(`🔍 DEBUG - this.config.kafka?.brokersInternal: ${this.config.kafka?.brokersInternal}`);

      const kafkaBrokers = this.config.kafka?.brokersInternal || 'kafka:29092';
      const kafkaTopic = 'journey-triggers';
      // ClickHouse only produces into this topic, but the table still names a
      // consumer group: never the journey workers' own 'temporal-workers', or a
      // consumer started on it would split their partitions and drop triggers.
      const kafkaGroupId = `${this.config.kafka?.groupId || 'evo-campaign-consumers'}-journey-triggers-clickhouse`;

      this.logger.log(
        `Using Kafka brokers for Journey Triggers: ${kafkaBrokers}`,
      );

      // 0. Guard against a stale broker "frozen" by a previous boot (EVO-1893).
      //    The table is created with `IF NOT EXISTS`, so if a first boot ran
      //    without KAFKA_BROKERS_INTERNAL it baked in 'localhost:9092' and every
      //    later boot keeps the wrong broker → ClickHouse can't reach Kafka, the
      //    topic stays empty and NO event-based journey fires, silently.
      //    Detect divergence and DROP+recreate so the configured broker wins.
      await this.ensureKafkaEngineBroker(
        databaseName,
        'journey_trigger_kafka_queue',
        kafkaBrokers,
        // The MV reads from contact_events and writes into the queue table; it
        // must be dropped before the underlying Kafka table can be dropped.
        ['events_to_journey_triggers_mv'],
        kafkaGroupId,
      );

      // 1. Create Kafka table for journey triggers
      await this.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${databaseName}.journey_trigger_kafka_queue (
            messageId String,
            contactId String,
            anonymousId Nullable(String),
            eventName String,
            eventType String,
            properties String,
            traits Nullable(String),
            context Nullable(String),
            timestamp String
          )
          ENGINE = Kafka('${kafkaBrokers}', '${kafkaTopic}', '${kafkaGroupId}', 'JSONEachRow')
          SETTINGS
            kafka_thread_per_consumer = 1,
            kafka_num_consumers = 2,
            kafka_max_block_size = 1048576,
            kafka_skip_broken_messages = 10,
            kafka_commit_every_batch = 1
        `,
      });

      this.logger.log('Kafka table for journey triggers created');

      // 2. Create materialized view to feed ALL events to journey trigger queue
      await this.command({
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS ${databaseName}.events_to_journey_triggers_mv
          TO ${databaseName}.journey_trigger_kafka_queue
          AS SELECT
            COALESCE(message_id, toString(generateUUIDv4())) as messageId,
            contact_id as contactId,
            anonymous_id as anonymousId,
            event_name as eventName,
            toString(event_type) as eventType,
            properties,
            traits,
            message_raw as context,
            toString(occurred_at) as timestamp
          FROM ${databaseName}.contact_events
          -- NO WHERE clause - send ALL events to be analyzed by JourneyTriggerProcessor
        `,
      });

      this.logger.log('Materialized view for journey triggers created');

      // Surface what broker the live table actually ended up bound to, so a
      // stale/unreachable broker is visible in the logs instead of failing
      // silently (the ClickHouse Kafka Engine produces into the topic in the
      // background and swallows connection errors). EVO-1893.
      await this.logJourneyTriggerKafkaState(databaseName);

      this.logger.log(
        '✅ Journey Trigger Queue infrastructure created successfully',
      );
    } catch (error) {
      this.logger.error(
        `Failed to create journey trigger queue: ${error.message}`,
        error.stack,
      );
      // Don't throw - allow system to continue without journey triggers
      this.logger.warn('System will continue without journey trigger queue');
    }
  }

  /**
   * Log the broker the journey-trigger Kafka Engine table is actually bound to,
   * plus any Kafka exceptions ClickHouse has recorded for it. The Kafka Engine
   * consumes/produces in the background and does not raise to this Node process,
   * so without this the only failure mode is a silently empty topic (EVO-1893).
   */
  private async logJourneyTriggerKafkaState(databaseName: string) {
    try {
      const rows = await this.query<{ create: string }>({
        query: `
          SELECT create_table_query AS create
          FROM system.tables
          WHERE database = {database:String} AND name = 'journey_trigger_kafka_queue'
        `,
        parameters: { database: databaseName },
      });

      const brokers = rows?.[0]
        ? this.extractKafkaBrokers(rows[0].create)
        : null;
      this.logger.log(
        `Journey-trigger Kafka Engine table is bound to broker '${brokers ?? 'unknown'}'`,
      );

      // system.kafka_consumers carries the last exception per consumer (e.g.
      // "Connection refused"). Best-effort: older ClickHouse versions may lack
      // some columns, so any failure here is downgraded to a debug line.
      try {
        const errors = await this.query<{
          last_exception: string;
          num_messages_read: string;
        }>({
          query: `
            SELECT
              last_exception,
              num_messages_read
            FROM system.kafka_consumers
            WHERE database = {database:String}
              AND table = 'journey_trigger_kafka_queue'
              AND last_exception != ''
          `,
          parameters: { database: databaseName },
        });

        for (const row of errors ?? []) {
          this.logger.error(
            `❌ ClickHouse Kafka Engine error on journey-trigger topic: ${row.last_exception}`,
          );
        }
      } catch (innerError) {
        this.logger.debug(
          `Could not read system.kafka_consumers for journey triggers: ${innerError.message}`,
        );
      }
    } catch (error) {
      this.logger.warn(
        `Could not inspect journey-trigger Kafka Engine state: ${error.message}`,
      );
    }
  }
}

export class ClickHouseQueryBuilderImpl {
  private parameters: Map<string, unknown> = new Map();
  private queryParts: string[] = [];

  addParameter(value: unknown, type: string = 'String'): string {
    const key = `param_${this.parameters.size}`;
    this.parameters.set(key, value);
    return `{${key}:${type}}`;
  }

  addQueryPart(part: string): this {
    this.queryParts.push(part);
    return this;
  }

  build(): ClickHouseQueryBuilder {
    return {
      query: this.queryParts.join(' '),
      parameters: Object.fromEntries(this.parameters),
    };
  }

  reset(): this {
    this.parameters.clear();
    this.queryParts.length = 0;
    return this;
  }
}
