import { Injectable, Optional } from '@nestjs/common';
import { Repository, IsNull, Not } from 'typeorm';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { Segment } from '../entities/segment.entity';
import { TenantDbContext } from '../../../evo-extension-points';
import { ModularSegmentComputationService } from './modular-segment-computation.service';
import { ClickHouseService } from '../../processing/clickhouse/clickhouse.service';
import { SegmentDistributedJobService } from './segment-distributed-job.service';
import { SegmentCacheService } from '../../cache/services/segment-cache.service';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';

export interface SegmentComputationResult {
  segmentId: string;
  contactsAdded: number;
  contactsRemoved: number;
  totalContacts: number;
  processingTimeMs: number;
}

@Injectable()
export class SegmentComputationService {
  private readonly logger = new CustomLoggerService(
    SegmentComputationService.name,
  );

  constructor(
    private readonly db: TenantDbContext,
    private modularComputationService: ModularSegmentComputationService,
    private clickhouseService: ClickHouseService,
    private segmentCacheService: SegmentCacheService,
    private eventEmitter: EventEmitter2,
    @Optional() private distributedJobService?: SegmentDistributedJobService,
  ) {}

  private get segmentRepository(): Repository<Segment> {
    return this.db.getRepository(Segment);
  }

  private isDistributedProcessingEnabled(): boolean {
    return process.env.ENABLE_DISTRIBUTED_PROCESSING === 'true';
  }

  /**
   * Stores the computation result and invalidates the cache. The write is
   * skipped when the definition was edited while computing: the result is for
   * the old definition, and the edit's NULL last_computed_at must survive so
   * the next run rebuilds from scratch.
   */
  private async recordComputation(
    segment: Segment,
    contactsCount: number,
  ): Promise<void> {
    const segmentId = segment.id;
    const query = this.segmentRepository
      .createQueryBuilder()
      .update(Segment)
      .set({ lastComputedAt: new Date(), contactsCount })
      .where('id = :segmentId', { segmentId });
    if (segment.definitionUpdatedAt) {
      query.andWhere(
        "date_trunc('milliseconds', definition_updated_at) = :loaded",
        { loaded: segment.definitionUpdatedAt },
      );
    } else {
      query.andWhere('definition_updated_at IS NULL');
    }
    const { affected } = await query.execute();

    if (affected === 0) {
      this.logger.log(
        `Segment ${segmentId} was edited during computation; result discarded`,
      );
    }

    await this.invalidateComputedSegment(segmentId);
  }

  /**
   * Update segment metadata with cache invalidation (standardized pattern)
   */
  private async updateSegmentWithCacheInvalidation(
    segmentId: string,
    updateData: Partial<Segment>,
  ): Promise<void> {
    // Update segment metadata
    await this.segmentRepository.update(segmentId, updateData);

    await this.invalidateComputedSegment(segmentId);
  }

  private async invalidateComputedSegment(segmentId: string): Promise<void> {
    // Invalidate cache and emit computation event (following standard pattern)
    await this.segmentCacheService.invalidateSegment(segmentId);
    this.eventEmitter.emit('segment.computed', { segmentId });

    this.logger.debug(`Segment ${segmentId} updated with cache invalidation`);
  }

  /**
   * Verifica se a definição do segmento contém janelas de tempo (withinSeconds)
   */
  private hasTimeWindows(definition: any): boolean {
    if (!definition?.nodes) {
      return false;
    }

    const checkNodeForTimeWindow = (node: any): boolean => {
      if (!node) return false;

      if (node.withinSeconds && node.withinSeconds > 0) {
        return true;
      }

      if (node.children && Array.isArray(node.children)) {
        for (const childId of node.children) {
          const childNode = definition.nodes.find((n: any) => n.id === childId);
          if (childNode && checkNodeForTimeWindow(childNode)) {
            return true;
          }
        }
      }

      return false;
    };

    if (definition.entryNode) {
      return checkNodeForTimeWindow(definition.entryNode);
    }

    for (const node of definition.nodes) {
      if (checkNodeForTimeWindow(node)) {
        return true;
      }
    }

    return false;
  }

  /**
   * A first run or an edited definition rebuilds from every event, so the
   * previous ClickHouse state must go first. Time-window segments clean up
   * inside ModularSegmentComputationService, after keeping the old assignments.
   */
  private async cleanupBeforeFullRecompute(segment: Segment): Promise<void> {
    if (this.hasTimeWindows(segment.definition)) {
      return;
    }

    const definitionWasUpdated =
      !!segment.definitionUpdatedAt &&
      !!segment.lastComputedAt &&
      segment.definitionUpdatedAt > segment.lastComputedAt;
    const isFirstComputation = !segment.lastComputedAt;

    if (!definitionWasUpdated && !isFirstComputation) {
      this.logger.log(
        `Skipping cleanup for segment ${segment.id} - using incremental processing`,
      );
      return;
    }

    this.logger.log(
      `Cleaning up old ClickHouse data for segment ${segment.id} (reason: ${
        definitionWasUpdated ? 'definition updated' : 'first computation'
      })`,
    );
    await this.modularComputationService.cleanupOldSegmentData(segment.id);
  }

  async computeSegment(segmentId: string): Promise<SegmentComputationResult> {
    const startTime = Date.now();

    const segment = await this.segmentRepository.findOne({
      where: { id: segmentId },
    });

    if (!segment) {
      throw new Error(`Segment ${segmentId} not found`);
    }

    if (!segment.definition) {
      this.logger.warn(
        `Segment ${segmentId} has no definition, skipping computation`,
      );
      return {
        segmentId,
        contactsAdded: 0,
        contactsRemoved: 0,
        totalContacts: 0,
        processingTimeMs: Date.now() - startTime,
      };
    }

    this.logger.log(`Computing segment ${segment.name} (${segmentId})`);

    // Check if distributed processing is enabled
    if (this.isDistributedProcessingEnabled() && this.distributedJobService) {
      this.logger.log(
        `Using Kafka distributed processing for segment ${segmentId}`,
      );

      // Submit job to distributed queue and return immediately with pending status
      await this.distributedJobService.createSegmentJob(segment);

      // Return a result indicating the job was queued for processing
      return {
        segmentId,
        contactsAdded: 0,
        contactsRemoved: 0,
        totalContacts: 0,
        processingTimeMs: Date.now() - startTime,
      };
    }

    this.logger.log(`Using ClickHouse computation for segment ${segmentId}`);

    await this.cleanupBeforeFullRecompute(segment);

    const result = await this.modularComputationService.computeSegment(segment);

    await this.recordComputation(segment, result.totalContacts);

    return {
      segmentId: result.segmentId,
      contactsAdded: result.contactsAdded,
      contactsRemoved: result.contactsRemoved,
      totalContacts: result.totalContacts,
      processingTimeMs: result.processingTimeMs,
    };
  }

  async computeAllSegments(): Promise<SegmentComputationResult[]> {
    const segments = await this.segmentRepository.find();

    this.logger.log(`Computing ${segments.length} segments`);

    // Use batch processing for ClickHouse with multiple segments
    if (segments.length > 1) {
      this.logger.log(
        `Using ClickHouse batch computation for ${segments.length} segments`,
      );

      for (const segment of segments) {
        await this.cleanupBeforeFullRecompute(segment);
      }

      const results =
        await this.modularComputationService.computeSegmentsBatch(segments);

      const updatePromises = results.map(async (result) => {
        const segment = segments.find((s) => s.id === result.segmentId);
        if (segment) {
          await this.recordComputation(segment, result.totalContacts);
        }

        return {
          segmentId: result.segmentId,
          contactsAdded: result.contactsAdded,
          contactsRemoved: result.contactsRemoved,
          totalContacts: result.totalContacts,
          processingTimeMs: result.processingTimeMs,
        };
      });

      return await Promise.all(updatePromises);
    }

    // Fallback to individual processing
    const results: SegmentComputationResult[] = [];
    for (const segment of segments) {
      try {
        const result = await this.computeSegment(segment.id);
        results.push(result);
      } catch (error) {
        this.logger.error(
          `Failed to compute segment ${segment.id}: ${error.message}`,
        );
        results.push({
          segmentId: segment.id,
          contactsAdded: 0,
          contactsRemoved: 0,
          totalContacts: 0,
          processingTimeMs: 0,
        });
      }
    }

    return results;
  }

  async getSegmentContacts(
    segmentId: string,
    limit = 100,
    offset = 0,
  ): Promise<string[]> {
    try {
      const qb = this.clickhouseService.createQueryBuilder();
      const segmentIdParam = qb.addParameter(segmentId, 'String');
      const limitParam = qb.addParameter(limit, 'Int32');
      const offsetParam = qb.addParameter(offset, 'Int32');

      const query = `
        SELECT DISTINCT contact_id
        FROM computed_property_assignments_v2 FINAL
        WHERE
          computed_property_id = ${segmentIdParam}
          AND type = 'segment'
          AND segment_value = true
        ORDER BY contact_id
        LIMIT ${offsetParam}, ${limitParam}
      `;

      const result = await this.clickhouseService.query({
        query,
        parameters: qb.build().parameters,
      });
      return result.map((row: any) => row.contact_id);
    } catch (error: any) {
      this.logger.error(
        `Error getting segment contacts from ClickHouse for segment ${segmentId}: ${error.message}`,
      );
      // Propagate instead of returning [] silently: a ClickHouse failure here
      // must not be indistinguishable from a genuinely empty segment.
      throw error;
    }
  }

  async getContactSegments(contactId: string): Promise<string[]> {
    try {
      const query = `
        SELECT DISTINCT computed_property_id as segmentId
        FROM computed_property_assignments_v2 FINAL
        WHERE
          type = 'segment'
          AND contact_id = {contactId:String}
          AND segment_value = true
      `;

      const result = await this.clickhouseService.query({
        query,
        parameters: {
          contactId,
        },
      });

      return result.map((row: any) => row.segmentId);
    } catch (error: any) {
      this.logger.error(`Error getting contact segments: ${error.message}`);
      // Propagate: a ClickHouse failure must not look like "contact in no segment".
      throw error;
    }
  }

  async isContactInSegment(
    contactId: string,
    segmentId: string,
  ): Promise<boolean> {
    try {
      const query = `
        SELECT segment_value as inSegment
        FROM computed_property_assignments_v2 FINAL
        WHERE
          type = 'segment'
          AND computed_property_id = {segmentId:String}
          AND contact_id = {contactId:String}
        LIMIT 1
      `;

      const results = await this.clickhouseService.query({
        query,
        parameters: {
          segmentId,
          contactId,
        },
      });

      return results.length > 0 ? results[0].inSegment : false;
    } catch (error: any) {
      this.logger.error(`Error checking segment assignment: ${error.message}`);
      // Propagate: a ClickHouse failure must not be silently treated as "not in segment".
      throw error;
    }
  }

  async removeContactFromAllSegments(contactId: string): Promise<void> {
    try {
      const deleteQuery = `
        DELETE FROM computed_property_assignments_v2
        WHERE
          type = 'segment'
          AND contact_id = {contactId:String}
      `;

      await this.clickhouseService.command({
        query: deleteQuery,
        parameters: {
          contactId,
        },
      });

      this.logger.log(`Removed contact ${contactId} from all segments`);
    } catch (error: any) {
      this.logger.error(
        `Error removing contact from segments: ${error.message}`,
      );
      throw error;
    }
  }

  async invalidateSegmentCache(segmentId: string): Promise<void> {
    const segment = await this.segmentRepository.findOne({
      where: { id: segmentId },
    });

    if (!segment) {
      this.logger.warn(`Segment ${segmentId} not found for cache invalidation`);
      return;
    }

    this.logger.debug(
      `ClickHouse cache invalidation skipped for segment ${segmentId} - will be handled by SegmentInvalidationService`,
    );

    // For invalidation, we just invalidate cache without modifying lastComputedAt
    await this.updateSegmentWithCacheInvalidation(segmentId, {});

    this.logger.log(`Invalidated cache for segment ${segmentId}`);
  }

  async getSegmentComputationStats(): Promise<{
    totalSegments: number;
    computedSegments: number;
    pendingSegments: number;
    totalAssignments: number;
  }> {
    const [totalSegments, computedSegments] = await Promise.all([
      this.segmentRepository.count(),
      this.segmentRepository.count({
        where: {
          lastComputedAt: Not(IsNull()),
        },
      }),
    ]);

    // Get total assignments from ClickHouse
    let totalAssignments = 0;
    try {
      const query = `
        SELECT count(DISTINCT contact_id) as total
        FROM computed_property_assignments_v2 FINAL
        WHERE
          type = 'segment'
          AND segment_value = true
      `;

      const result = await this.clickhouseService.query({ query });

      totalAssignments = result[0]?.total || 0;
    } catch (error: any) {
      this.logger.error(`Error getting assignment stats: ${error.message}`);
    }

    return {
      totalSegments,
      computedSegments,
      pendingSegments: totalSegments - computedSegments,
      totalAssignments,
    };
  }
}
