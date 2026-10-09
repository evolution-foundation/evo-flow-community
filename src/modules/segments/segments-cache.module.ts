import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule } from '@nestjs/config';
import { SegmentCacheService } from '../cache/services/segment-cache.service';
import { SegmentInvalidationService } from './services/segment-invalidation.service';
import { ModularSegmentComputationService } from './services/modular-segment-computation.service';
import { SegmentStateManagementService } from './services/segment-state-management.service';
import { SegmentChangeDetectionService } from './services/segment-change-detection.service';
import { SegmentClickHouseQueryBuilderService } from './services/segment-clickhouse-query-builder.service';
import { SegmentQueryExecutionService } from './services/segment-query-execution.service';
import { SegmentAssignmentService } from './services/segment-assignment.service';
import { SegmentEventsService } from './services/segment-events.service';
import { DeletedContactsCacheService } from './services/deleted-contacts-cache.service';
import { DeletedContactsSignalRelay } from './services/deleted-contacts-signal.relay';
import { SegmentCircuitBreakerService } from './services/segment-circuit-breaker.service';
import { SegmentMetricsService } from './metrics/segment-metrics.service';
import { SegmentQueueService } from './services/segment-queue.service';
import { Segment } from './entities/segment.entity';
import { ProcessingModule } from '../processing/processing.module';

/**
 * Module for ClickHouse-based segment caching
 * Provides high-performance segment lookups and event-driven invalidation
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Segment]),
    ConfigModule,
    forwardRef(() => ProcessingModule),
  ],
  providers: [
    SegmentCacheService,
    SegmentInvalidationService,
    ModularSegmentComputationService,
    SegmentStateManagementService,
    SegmentChangeDetectionService,
    SegmentClickHouseQueryBuilderService,
    SegmentQueryExecutionService,
    SegmentAssignmentService,
    SegmentEventsService,
    DeletedContactsCacheService,
    DeletedContactsSignalRelay,
    SegmentCircuitBreakerService,
    SegmentMetricsService,
    SegmentQueueService,
  ],
  exports: [
    SegmentCacheService,
    SegmentInvalidationService,
    ModularSegmentComputationService,
    SegmentEventsService,
    DeletedContactsCacheService,
    SegmentCircuitBreakerService,
    SegmentMetricsService,
    SegmentQueueService,
  ],
})
export class SegmentsCacheModule {}
