import {
  Injectable,
  NotFoundException,
  ConflictException,
  UnprocessableEntityException,
} from '@nestjs/common';
import { Repository, SelectQueryBuilder } from 'typeorm';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { randomUUID } from 'crypto';
import { Segment, SegmentDefinition } from './entities/segment.entity';
import { TenantDbContext } from '../../evo-extension-points';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';
import { CreateSegmentDto } from './dto/create-segment.dto';
import { UpdateSegmentDto } from './dto/update-segment.dto';
import { FilterSegmentsDto } from './dto/filter-segments.dto';
import { SegmentResponseDto } from './dto/segment-response.dto';
import { SegmentComputationService } from './services/segment-computation.service';
import { ModularSegmentComputationService } from './services/modular-segment-computation.service';
import { SegmentCacheService } from '../cache/services/segment-cache.service';

/** Max number of contact ids returned in a preview sample. Shared with the
 *  controller's API docs so the value cannot drift. */
export const SEGMENT_PREVIEW_SAMPLE_SIZE = 10;

@Injectable()
export class SegmentsService {
  // new CustomLoggerService(ctx) is the module's convention (see sibling
  // services) and is context-scoped per class — injecting the shared singleton
  // would lose/clobber the [SegmentsService] log context.
  private readonly logger = new CustomLoggerService(SegmentsService.name);

  constructor(
    private readonly db: TenantDbContext,
    private segmentComputationService: SegmentComputationService,
    private modularSegmentComputationService: ModularSegmentComputationService,
    private segmentCacheService: SegmentCacheService,
    private eventEmitter: EventEmitter2,
  ) {}

  private get segmentRepository(): Repository<Segment> {
    return this.db.getRepository(Segment);
  }

  async create(
    createSegmentDto: CreateSegmentDto,
  ): Promise<SegmentResponseDto> {
    const existingSegment = await this.segmentRepository.findOne({
      where: {
        name: createSegmentDto.name,
      },
    });

    if (existingSegment) {
      throw new ConflictException(
        `Segment with name '${createSegmentDto.name}' already exists`,
      );
    }

    const segment = this.segmentRepository.create({
      ...createSegmentDto,
      status: createSegmentDto.status || 'running',
      computedCount: 0,
    });

    const savedSegment = await this.segmentRepository.save(segment);

    await this.segmentCacheService.cacheSegment(savedSegment);

    this.eventEmitter.emit('segment.created', {
      segmentId: savedSegment.id,
    });

    return await this.toResponseDto(savedSegment);
  }

  /**
   * Compute a segment definition WITHOUT persisting anything. Returns the
   * in-segment contact count and a small sample of ids.
   *
   * The pipeline keys all intermediate ClickHouse state on `segment.id`, so we
   * run it for a transient `preview-*` id and always clean those rows up.
   * Only STAGE 1 (state) + STAGE 2 (assignments) run — both the count and the
   * sample read from `computed_property_assignments_v2`; STAGE 3 (change-event
   * emission) and STAGE 4 (resolved-state save) are intentionally skipped.
   * Postgres is never touched.
   */
  async previewByDefinition(
    definition: SegmentDefinition,
  ): Promise<{ count: number; sample: { id: string }[] }> {
    // The DTO only checks @IsObject(); reject a structurally invalid definition
    // here as a 422 instead of letting computeState throw a generic error.
    if (
      !definition ||
      typeof definition !== 'object' ||
      !('entryNode' in definition) ||
      !definition.entryNode
    ) {
      throw new UnprocessableEntityException(
        'Invalid segment definition: missing entryNode',
      );
    }

    const SAMPLE_SIZE = SEGMENT_PREVIEW_SAMPLE_SIZE;
    const previewId = `preview-${randomUUID()}`;
    const now = Date.now();

    // Transient instance only — never passed to segmentRepository.save().
    const segment = this.segmentRepository.create({
      id: previewId,
      name: '__preview__',
      definition,
      status: 'paused',
      computedCount: 0,
    });

    try {
      await this.modularSegmentComputationService.computeState(
        segment,
        definition,
        now,
      );
      await this.modularSegmentComputationService.computeAssignments(
        segment,
        definition,
        now,
      );

      const { totalContacts } =
        await this.modularSegmentComputationService.countFinalAssignments(
          previewId,
        );
      const sampleIds = await this.segmentComputationService.getSegmentContacts(
        previewId,
        SAMPLE_SIZE,
        0,
      );

      return {
        count: totalContacts,
        sample: sampleIds.map((id) => ({ id })),
      };
    } finally {
      // Best-effort: drop every ephemeral ClickHouse row keyed by previewId so a
      // preview leaves zero footprint. Swallow failures so the result still
      // returns to the caller.
      await this.modularSegmentComputationService
        .cleanupOldSegmentData(previewId)
        .catch((error) =>
          this.logger.error(
            `Failed to clean up preview segment ${previewId}: ${(error as Error).message}`,
          ),
        );
    }
  }

  async findAll(filter: FilterSegmentsDto): Promise<{
    segments: SegmentResponseDto[];
    total: number;
    page: number;
    limit: number;
  }> {
    const queryBuilder = this.segmentRepository.createQueryBuilder('segment');

    this.applyFilters(queryBuilder, filter);

    const total = await queryBuilder.getCount();

    queryBuilder
      .orderBy('segment.name', 'ASC')
      .skip(((filter.page || 1) - 1) * (filter.limit || 10))
      .take(filter.limit || 10);

    const segments = await queryBuilder.getMany();

    const segmentsWithCache = await Promise.all(
      segments.map(async (segment) => {
        const cachedSegment = await this.segmentCacheService.getSegment(
          segment.id,
        );

        if (cachedSegment) {
          return this.cachedSegmentToResponseDto(cachedSegment);
        }

        await this.segmentCacheService.cacheSegment({
          id: segment.id,
          name: segment.name,
          definition: segment.definition,
          status: segment.status,
          contactsCount: segment.contactsCount,
          lastComputedAt: segment.lastComputedAt,
          createdAt: segment.createdAt,
          updatedAt: segment.updatedAt,
        });

        return this.toResponseDto(segment);
      }),
    );

    return {
      segments: segmentsWithCache,
      total,
      page: filter.page || 1,
      limit: filter.limit || 10,
    };
  }

  async findOne(id: string): Promise<SegmentResponseDto> {
    const cachedSegment = await this.segmentCacheService.getSegment(id);

    if (cachedSegment) {
      return this.cachedSegmentToResponseDto(cachedSegment);
    }

    const segment = await this.segmentRepository.findOne({
      where: { id },
    });

    if (!segment) {
      throw new NotFoundException(`Segment with ID ${id} not found`);
    }

    await this.segmentCacheService.cacheSegment({
      id: segment.id,
      name: segment.name,
      definition: segment.definition,
      status: segment.status,
      contactsCount: segment.contactsCount,
      lastComputedAt: segment.lastComputedAt,
      createdAt: segment.createdAt,
      updatedAt: segment.updatedAt,
    });

    return await this.toResponseDto(segment);
  }

  async update(
    id: string,
    updateSegmentDto: UpdateSegmentDto,
  ): Promise<SegmentResponseDto> {
    const segment = await this.findOne(id);

    if (!segment) {
      throw new NotFoundException(`Segment with ID ${id} not found`);
    }

    // A new definition needs a full rebuild: NULL (TypeORM skips undefined)
    // makes the next computation read every event, not only newer ones.
    const changes = Object.fromEntries(
      Object.entries(updateSegmentDto).filter(([, v]) => v !== undefined),
    ) as UpdateSegmentDto;
    if (changes.definition !== undefined) {
      await this.segmentRepository.update(
        { id },
        {
          ...changes,
          lastComputedAt: () => 'NULL',
          definitionUpdatedAt: new Date(),
        },
      );
    } else if (Object.keys(changes).length > 0) {
      await this.segmentRepository.update({ id }, changes);
    }

    const updatedSegment = await this.segmentRepository.findOne({
      where: { id },
    });

    if (!updatedSegment) {
      throw new NotFoundException(
        `Segment with ID ${id} not found after update`,
      );
    }

    await this.segmentCacheService.cacheSegment({
      id: updatedSegment.id,
      name: updatedSegment.name,
      definition: updatedSegment.definition,
      status: updatedSegment.status,
      contactsCount: updatedSegment.contactsCount,
      lastComputedAt: updatedSegment.lastComputedAt,
      createdAt: updatedSegment.createdAt,
      updatedAt: updatedSegment.updatedAt,
    });

    this.eventEmitter.emit('segment.updated', { segmentId: id });

    return await this.toResponseDto(updatedSegment);
  }

  async remove(id: string): Promise<void> {
    const segment = await this.segmentRepository.findOne({
      where: { id },
    });

    if (!segment) {
      throw new NotFoundException(`Segment with ID ${id} not found`);
    }

    try {
      let contactCount = 0;
      try {
        const result =
          await this.modularSegmentComputationService.countFinalAssignments(id);
        contactCount = result.totalContacts;
      } catch (error) {
        console.warn(
          `Could not get contact count from ClickHouse for segment ${id}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }

      await this.modularSegmentComputationService.cleanupOldSegmentData(id);

      await this.segmentCacheService.invalidateSegment(id);
      this.eventEmitter.emit('segment.deleted', { segmentId: id });

      await this.segmentRepository.remove(segment);

      console.log(
        `Successfully removed segment ${segment.name} (${id}) with ${contactCount} contacts`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.error(`Failed to fully cleanup segment ${id}: ${errorMessage}`);

      await this.segmentRepository.remove(segment);

      throw error;
    }
  }

  async recomputeSegment(id: string): Promise<SegmentResponseDto> {
    const cachedSegment = await this.segmentCacheService.getSegment(id);
    let segmentExists = false;

    if (cachedSegment) {
      segmentExists = true;
    } else {
      const dbSegment = await this.segmentRepository.findOne({
        where: { id },
      });

      if (!dbSegment) {
        throw new NotFoundException(`Segment with ID ${id} not found`);
      }
      segmentExists = true;
    }

    if (!segmentExists) {
      throw new NotFoundException(`Segment with ID ${id} not found`);
    }

    await this.segmentComputationService.computeSegment(id);

    const updatedSegment = await this.segmentRepository.findOne({
      where: { id },
    });

    if (!updatedSegment) {
      throw new NotFoundException(
        `Segment with ID ${id} not found after recompute`,
      );
    }

    await this.segmentCacheService.cacheSegment({
      id: updatedSegment.id,
      name: updatedSegment.name,
      definition: updatedSegment.definition,
      status: updatedSegment.status,
      contactsCount: updatedSegment.contactsCount,
      lastComputedAt: updatedSegment.lastComputedAt,
      createdAt: updatedSegment.createdAt,
      updatedAt: updatedSegment.updatedAt,
    });

    this.eventEmitter.emit('segment.computed', { segmentId: id });

    return this.toResponseDto(updatedSegment);
  }

  async getSegmentContacts(
    segmentId: string,
    page: number = 1,
    limit: number = 10,
  ): Promise<{
    contactIds: string[];
    total: number;
    page: number;
    limit: number;
  }> {
    const contactIds = await this.segmentComputationService.getSegmentContacts(
      segmentId,
      limit,
      (page - 1) * limit,
    );

    let total = 0;
    const cachedSegment = await this.segmentCacheService.getSegment(segmentId);

    if (cachedSegment) {
      total = cachedSegment.contactsCount || 0;
    } else {
      const segment = await this.segmentRepository.findOne({
        where: { id: segmentId },
      });
      total = segment?.contactsCount || 0;

      if (segment) {
        await this.segmentCacheService.cacheSegment({
          id: segment.id,
          name: segment.name,
          definition: segment.definition,
          status: segment.status,
          contactsCount: segment.contactsCount,
          lastComputedAt: segment.lastComputedAt,
          createdAt: segment.createdAt,
          updatedAt: segment.updatedAt,
        });
      }
    }

    return {
      contactIds,
      total,
      page,
      limit,
    };
  }

  async getSegmentContactIds(
    segmentId: string,
    limit: number = 1000,
    offset: number = 0,
  ): Promise<{
    contactIds: string[];
    total: number;
    limit: number;
    offset: number;
  }> {
    const cachedSegment = await this.segmentCacheService.getSegment(segmentId);
    let contactsCount = 0;

    if (cachedSegment) {
      contactsCount = cachedSegment.contactsCount || 0;
    } else {
      const segment = await this.segmentRepository.findOne({
        where: { id: segmentId },
      });

      if (!segment) {
        throw new NotFoundException(`Segment with ID ${segmentId} not found`);
      }

      contactsCount = segment.contactsCount || 0;

      await this.segmentCacheService.cacheSegment({
        id: segment.id,
        name: segment.name,
        definition: segment.definition,
        status: segment.status,
        contactsCount: segment.contactsCount,
        lastComputedAt: segment.lastComputedAt,
        createdAt: segment.createdAt,
        updatedAt: segment.updatedAt,
      });
    }

    try {
      const result =
        await this.modularSegmentComputationService.countFinalAssignments(
          segmentId,
        );

      const contactIds =
        await this.segmentComputationService.getSegmentContacts(
          segmentId,
          limit,
          offset,
        );

      return {
        contactIds,
        total: result.totalContacts,
        limit,
        offset,
      };
    } catch (error) {
      console.error('Error getting segment contact IDs:', error);
      return {
        contactIds: [],
        total: contactsCount,
        limit,
        offset,
      };
    }
  }

  private applyFilters(
    queryBuilder: SelectQueryBuilder<Segment>,
    filter: FilterSegmentsDto,
  ): void {
    if (filter.ids && filter.ids.length > 0) {
      queryBuilder.andWhere('segment.id IN (:...ids)', { ids: filter.ids });
    }

    if (filter.status) {
      queryBuilder.andWhere('segment.status = :status', {
        status: filter.status,
      });
    }

    if (filter.search) {
      queryBuilder.andWhere('segment.name ILIKE :search', {
        search: `%${filter.search}%`,
      });
    }
  }

  async recomputeAllSegments(): Promise<any[]> {
    return await this.segmentComputationService.computeAllSegments();
  }

  private toResponseDto(segment: Segment): SegmentResponseDto {
    return {
      id: segment.id,
      name: segment.name,
      definition: segment.definition,
      status: segment.status,
      computedCount: segment.computedCount,
      contactsCount: segment.contactsCount,
      lastComputedAt: segment.lastComputedAt,
      definitionUpdatedAt: segment.definitionUpdatedAt,
      createdAt: segment.createdAt,
      updatedAt: segment.updatedAt,
    };
  }

  private cachedSegmentToResponseDto(cachedSegment: any): SegmentResponseDto {
    return {
      id: cachedSegment.id,
      name: cachedSegment.name,
      definition: cachedSegment.definition,
      status: cachedSegment.status,
      computedCount: 0,
      contactsCount: cachedSegment.contactsCount,
      lastComputedAt: cachedSegment.lastComputedAt,
      definitionUpdatedAt: new Date(),
      createdAt: cachedSegment.createdAt,
      updatedAt: cachedSegment.updatedAt,
    };
  }

  async getCacheStats() {
    return this.segmentCacheService.getCacheStats();
  }

  async warmupCacheForAccount(segmentIds?: string[]) {
    return this.segmentCacheService.warmupCache(segmentIds);
  }

  async clearAllCaches() {
    return this.segmentCacheService.clearAllCaches();
  }
}
