import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import {
  CONTACT_DELETED_INGESTED_EVENT,
  DELETED_CONTACTS_SUBQUERY,
} from '../queries/contact-event-names';
import { ClickHouseService } from '../../processing/clickhouse/clickhouse.service';
import { CustomLoggerService } from 'src/common/services/custom-logger.service';

@Injectable()
export class DeletedContactsCacheService {
  private readonly logger = new CustomLoggerService(
    DeletedContactsCacheService.name,
  );
  private readonly CACHE_KEY = 'deleted_contacts';
  private cached: Set<string> | null = null;
  private expiresAt: number = 0;
  private readonly CACHE_TTL = 300000; // 5 minutes
  // Ingest → Kafka → ClickHouse MV is async: a fetch right after the deleted-contact
  // signal may not see the row yet and would re-cache a stale set for CACHE_TTL. During
  // this window every call queries ClickHouse and nothing is cached (CRM-215).
  private readonly BYPASS_AFTER_DELETE_MS = 15000;
  private bypassCacheUntil = 0;
  // Concurrent callers share one ClickHouse fetch. Each invalidation bumps the
  // generation, so a fetch that started before it never re-caches the old set.
  private inFlight: Promise<Set<string>> | null = null;
  private generation = 0;

  constructor(private readonly clickhouseService: ClickHouseService) {}

  async getDeletedContacts(): Promise<Set<string>> {
    const now = Date.now();
    const bypass = now < this.bypassCacheUntil;

    if (!bypass && this.cached && now < this.expiresAt) {
      this.logger.debug('Deleted contacts cache hit');
      return this.cached;
    }

    if (this.inFlight) {
      return this.inFlight;
    }

    this.logger.debug('Deleted contacts cache miss, fetching from ClickHouse');

    const pending = this.loadDeletedContacts(this.generation).finally(() => {
      if (this.inFlight === pending) this.inFlight = null;
    });
    this.inFlight = pending;
    return pending;
  }

  private async loadDeletedContacts(generation: number): Promise<Set<string>> {
    try {
      const deletedContacts = await this.fetchDeletedContactsFromClickHouse();
      if (generation === this.generation) {
        const now = Date.now();
        this.cached = deletedContacts;
        // A set fetched inside the bypass window may be incomplete: let it expire with the window.
        this.expiresAt =
          now < this.bypassCacheUntil
            ? this.bypassCacheUntil
            : now + this.CACHE_TTL;
        this.logger.debug(`Cached ${deletedContacts.size} deleted contacts`);
      }
      return deletedContacts;
    } catch (error) {
      this.logger.error('Failed to fetch deleted contacts:', error);
      if (this.cached) {
        this.logger.warn('Using expired cache data due to fetch error');
        return this.cached;
      }
      return new Set<string>();
    }
  }

  private async fetchDeletedContactsFromClickHouse(): Promise<Set<string>> {
    const query = DELETED_CONTACTS_SUBQUERY;

    const result = await this.clickhouseService.query({ query });

    const deletedContacts = new Set<string>();
    result.forEach((row: any) => {
      if (row.contact_or_anonymous_id) {
        deletedContacts.add(row.contact_or_anonymous_id);
      }
    });

    return deletedContacts;
  }

  @OnEvent(CONTACT_DELETED_INGESTED_EVENT)
  onContactDeletedIngested(): void {
    this.invalidateCache();
    this.bypassCacheUntil = Date.now() + this.BYPASS_AFTER_DELETE_MS;
  }

  invalidateCache(): void {
    this.generation += 1;
    this.inFlight = null;
    this.cached = null;
    this.expiresAt = 0;
    this.logger.debug('Deleted contacts cache invalidated');
  }

  getCacheStats(): {
    totalEntries: number;
    expiredEntries: number;
    memoryUsage: number;
  } {
    const now = Date.now();
    const expired = this.cached && now >= this.expiresAt ? 1 : 0;
    return {
      totalEntries: this.cached ? 1 : 0,
      expiredEntries: expired,
      memoryUsage: this.cached ? this.cached.size * 36 : 0,
    };
  }
}
