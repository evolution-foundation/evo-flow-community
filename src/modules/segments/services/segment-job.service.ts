import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Segment } from '../entities/segment.entity';
import { SegmentComputationService } from './segment-computation.service';
import { getSegmentComputationConfig } from '../config/segment-computation.config';
import { RunMode } from '../../processing/enums/run-mode.enum';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';

@Injectable()
export class SegmentJobService {
  private readonly logger = new CustomLoggerService(SegmentJobService.name);
  private readonly segmentConfig = getSegmentComputationConfig();
  private readonly runMode: RunMode;

  constructor(
    @InjectRepository(Segment)
    private segmentRepository: Repository<Segment>,
    private segmentComputationService: SegmentComputationService,
    private configService: ConfigService,
  ) {
    this.runMode = this.configService.get<RunMode>('RUN_MODE', RunMode.SINGLE);

    // Only log cron status if in appropriate mode
    const isEnabled = this.isSegmentProcessingEnabled();
    if (isEnabled) {
      this.logger.log(
        `🎯 Segment cron jobs mode: ${this.segmentConfig.type} ` +
          `(cron-job enabled: ${this.segmentConfig.enableCronJob})`,
      );
    } else {
      this.logger.log(`🎯 Segment cron jobs: DISABLED (${this.runMode} mode)`);
    }
  }

  private isSegmentProcessingEnabled(): boolean {
    return (
      this.runMode === RunMode.SINGLE || this.runMode === RunMode.SEGMENT_WORKER
    );
  }

  calculateTime(startTime: number): string {
    const endTime = Date.now();
    const duration = endTime - startTime;
    return `${duration}ms`;
  }

  /**
   * Process segments with limited parallelism to avoid overwhelming the database
   */
  private async processSegmentsWithLimitedParallelism(
    segments: Segment[],
    maxConcurrency: number = 2,
  ): Promise<any[]> {
    const results: any[] = [];

    for (let i = 0; i < segments.length; i += maxConcurrency) {
      const batch = segments.slice(i, i + maxConcurrency);

      const batchPromises = batch.map(async (segment) => {
        try {
          const result = await this.segmentComputationService.computeSegment(
            segment.id,
          );

          this.logger.log(
            `Recomputed segment ${segment.name}: ${result.totalContacts} contacts ` +
              `(+${result.contactsAdded}, -${result.contactsRemoved}) in ${result.processingTimeMs}ms`,
          );

          return result;
        } catch (error) {
          this.logger.error(
            `Failed to recompute segment ${segment.name}: ${error instanceof Error ? error.message : 'Unknown error'}`,
          );

          return {
            segmentId: segment.id,
            contactsAdded: 0,
            contactsRemoved: 0,
            totalContacts: 0,
            processingTimeMs: 0,
          };
        }
      });

      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);
    }

    return results;
  }

  @Cron(CronExpression.EVERY_5_MINUTES)
  async recomputeStaleSegments(): Promise<void> {
    // Check if this mode should run segment processing
    if (!this.isSegmentProcessingEnabled()) {
      return; // Silent skip in non-segment modes
    }

    // Check if cron-job processing is enabled
    if (!this.segmentConfig.enableCronJob) {
      this.logger.debug(
        '⏸️ Cron-job processing disabled, skipping scheduled recomputation',
      );
      return;
    }
    const startTime = Date.now();
    this.logger.log(`Starting scheduled segment recomputation... ${startTime}`);

    try {
      const staleSegments = await this.segmentRepository
        .createQueryBuilder('segment')
        .where(
          `
          segment.last_computed_at IS NULL 
          OR segment.last_computed_at < segment.definition_updated_at
          OR segment.last_computed_at < NOW() - INTERVAL '1 hour'
        `,
        )
        .getMany();

      if (staleSegments.length === 0) {
        this.logger.log('No stale segments found for recomputation');
        return;
      }

      this.logger.log(
        `Found ${staleSegments.length} stale segments for recomputation`,
      );

      // Log performance configuration
      this.logger.log(
        `Performance config: ${this.segmentConfig.maxConcurrentSegments} concurrent segments`,
      );

      let results: any[] = [];

      // Use batch processing if multiple stale segments, otherwise individual
      if (staleSegments.length > 1) {
        try {
          this.logger.log(
            `Processing ${staleSegments.length} segments in batch`,
          );

          const batchResults =
            await this.segmentComputationService.computeAllSegments();

          // computeAllSegments returns every segment, in its own order.
          const staleById = new Map(staleSegments.map((s) => [s.id, s]));
          const staleResults = batchResults.filter((r) =>
            staleById.has(r.segmentId),
          );
          staleResults.forEach((result) => {
            const segment = staleById.get(result.segmentId);
            if (segment && result.totalContacts > 0) {
              this.logger.log(
                `Recomputed segment ${segment.name}: ${result.totalContacts} contacts ` +
                  `(+${result.contactsAdded}, -${result.contactsRemoved}) in ${result.processingTimeMs}ms`,
              );
            }
          });

          results = staleResults;
        } catch (error) {
          this.logger.error(
            `Batch processing failed, falling back to individual processing: ${error instanceof Error ? error.message : 'Unknown error'}`,
          );

          results = await this.processSegmentsWithLimitedParallelism(
            staleSegments,
            this.segmentConfig.maxConcurrentSegments,
          );
        }
      } else {
        results = await this.processSegmentsWithLimitedParallelism(
          staleSegments,
          1,
        );
      }

      this.logger.log(
        `Completed scheduled recomputation: ${results.length}/${staleSegments.length} segments processed in ${this.calculateTime(startTime)}`,
      );
    } catch (error) {
      this.logger.error(
        `Error in scheduled segment recomputation: ${error.message}`,
      );
    }
  }

  @Cron(CronExpression.EVERY_DAY_AT_2AM)
  async fullRecomputeAllSegments(): Promise<void> {
    // Check if this mode should run segment processing
    if (!this.isSegmentProcessingEnabled()) {
      return; // Silent skip in non-segment modes
    }

    // Check if cron-job processing is enabled
    if (!this.segmentConfig.enableCronJob) {
      this.logger.debug(
        '⏸️ Cron-job processing disabled, skipping daily recomputation',
      );
      return;
    }

    this.logger.log('Starting daily full segment recomputation...');

    try {
      this.logger.log('Recomputing all segments');
      const results =
        await this.segmentComputationService.computeAllSegments();

      const totalContacts = results.reduce(
        (sum, r) => sum + r.totalContacts,
        0,
      );
      const totalTime = results.reduce(
        (sum, r) => sum + r.processingTimeMs,
        0,
      );

      this.logger.log(
        `Completed daily full segment recomputation: ${results.length} segments, ${totalContacts} total contacts, ${totalTime}ms`,
      );
    } catch (error) {
      this.logger.error(
        `Error in daily segment recomputation: ${error.message}`,
      );
    }
  }

  async triggerSegmentRecomputation(segmentId: string): Promise<void> {
    this.logger.log(`Manual trigger: recomputing segment ${segmentId}`);

    try {
      const result =
        await this.segmentComputationService.computeSegment(segmentId);
      this.logger.log(
        `Manual recomputation completed: ${result.totalContacts} contacts ` +
          `(+${result.contactsAdded}, -${result.contactsRemoved}) in ${result.processingTimeMs}ms`,
      );
    } catch (error) {
      this.logger.error(
        `Manual recomputation failed for segment ${segmentId}: ${error.message}`,
      );
      throw error;
    }
  }

  async triggerAllSegmentsRecomputation(): Promise<void> {
    this.logger.log(`Manual trigger: recomputing all segments`);

    try {
      const results =
        await this.segmentComputationService.computeAllSegments();

      const totalContacts = results.reduce(
        (sum, r) => sum + r.totalContacts,
        0,
      );
      const totalTime = results.reduce((sum, r) => sum + r.processingTimeMs, 0);

      this.logger.log(
        `Manual recomputation completed: ${results.length} segments, ` +
          `${totalContacts} total contacts, ${totalTime}ms`,
      );
    } catch (error) {
      this.logger.error(
        `Manual recomputation failed: ${error.message}`,
      );
      throw error;
    }
  }

  async invalidateSegmentOnContactChange(contactId: string): Promise<void> {
    this.logger.log(
      `Contact ${contactId} changed, invalidating related segments`,
    );

    try {
      const segments = await this.segmentRepository.find();

      for (const segment of segments) {
        if (this.shouldInvalidateSegment(segment)) {
          await this.segmentComputationService.invalidateSegmentCache(
            segment.id,
          );
          this.logger.log(
            `Invalidated segment ${segment.name} due to contact change`,
          );
        }
      }
    } catch (error) {
      this.logger.error(
        `Error invalidating segments for contact change: ${error.message}`,
      );
    }
  }

  async invalidateSegmentOnEventReceived(
    contactId: string,
    eventName: string,
  ): Promise<void> {
    this.logger.log(
      `Event ${eventName} received for contact ${contactId}, checking segments`,
    );

    try {
      const segments = await this.segmentRepository.find();

      for (const segment of segments) {
        if (this.segmentReferencesEvent(segment, eventName)) {
          await this.segmentComputationService.invalidateSegmentCache(
            segment.id,
          );
          this.logger.log(
            `Invalidated segment ${segment.name} due to event ${eventName}`,
          );
        }
      }
    } catch (error) {
      this.logger.error(
        `Error invalidating segments for event: ${error.message}`,
      );
    }
  }

  private shouldInvalidateSegment(segment: Segment): boolean {
    if (!segment.definition) {
      return false;
    }

    // Usar definição diretamente
    const definition = segment.definition;

    // Handle both legacy and advanced definitions
    if ('entryNode' in definition && 'nodes' in definition) {
      return (
        this.nodeReferencesContactData(definition.entryNode) ||
        definition.nodes.some((node) => this.nodeReferencesContactData(node))
      );
    } else if ('children' in definition) {
      // Legacy format
      return definition.children.some((node) =>
        this.nodeReferencesContactData(node),
      );
    }

    return false;
  }

  private nodeReferencesContactData(node: Record<string, any>): boolean {
    switch (node.type) {
      case 'Everyone':
        return true;
      case 'Trait':
        return true;
      case 'And':
      case 'Or':
        return true;
      default:
        return false;
    }
  }

  private segmentReferencesEvent(segment: Segment, eventName: string): boolean {
    if (!segment.definition) {
      return false;
    }

    // Usar definição diretamente
    const definition = segment.definition;

    // Handle both legacy and advanced definitions
    if ('entryNode' in definition && 'nodes' in definition) {
      return (
        this.nodeReferencesEvent(definition.entryNode, eventName) ||
        definition.nodes.some((node) =>
          this.nodeReferencesEvent(node, eventName),
        )
      );
    } else if ('children' in definition) {
      // Legacy format
      return definition.children.some((node) =>
        this.nodeReferencesEvent(node, eventName),
      );
    }

    return false;
  }

  private nodeReferencesEvent(
    node: Record<string, any>,
    eventName: string,
  ): boolean {
    switch (node.type) {
      case 'Performed':
        return node.event === eventName;
      case 'CustomAttribute':
        // identify-DTO custom-attribute change (EVO-1839): a CustomAttribute node
        // is affected when the attribute changes. Accept both event-name forms.
        return (
          eventName === 'contact.custom_attribute.changed' ||
          eventName === 'custom_attribute_changed'
        );
      case 'And':
      case 'Or':
        return true;
      default:
        return false;
    }
  }
}
