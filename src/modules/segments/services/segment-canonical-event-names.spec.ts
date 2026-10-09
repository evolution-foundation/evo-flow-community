import { SegmentClickHouseQueryBuilderService } from './segment-clickhouse-query-builder.service';
import { SegmentNodeType } from '../entities/segment.entity';
import {
  applyDeletedContactsOptimization,
  deletedContactsCaseBranchRegex,
  DELETED_CONTACTS_SUBQUERY,
} from '../queries/contact-event-names';
import { DeletedContactsCacheService } from './deleted-contacts-cache.service';

/**
 * CRM-215 — the CRM emits dotted canonical event names (`contact.label.added`,
 * `contact.deleted`) with the label id in `traits`; the builder filtered the legacy
 * underscore spelling and read `properties`, so Label segments computed 0 members and
 * deleted contacts were never excluded. Both spellings are accepted until the central
 * normalization lands.
 */
describe('CRM-215 segment SQL matches the canonical contact event names', () => {
  const builder = new SegmentClickHouseQueryBuilderService();
  const segment = { id: 'seg-1' } as any;

  it('Label has: filters both label spellings and reads labelId from traits', () => {
    const node = {
      id: 'n1',
      type: SegmentNodeType.Label,
      labelId: 'lbl-1',
      condition: 'has',
    } as any;

    const [subQuery] = builder.segmentNodeToStateSubQuery(segment, node);

    expect(subQuery.condition).toContain("'contact.label.added'");
    expect(subQuery.condition).toContain("'contact.label.removed'");
    expect(subQuery.condition).toContain("'label_added'");
    expect(subQuery.condition).toContain(
      "JSONExtractString(traits, 'labelId') = 'lbl-1'",
    );
    expect(subQuery.condition).not.toContain(
      "JSONExtractString(properties, 'labelId')",
    );
    expect(subQuery.argMaxValue).toContain(
      "if(event_name IN ('contact.label.added', 'label_added'), 'true', 'false')",
    );
  });

  it('Label not_has: the exclusion subselect uses the same canonical names and traits', () => {
    const node = {
      id: 'n1',
      type: SegmentNodeType.Label,
      labelId: 'lbl-1',
      condition: 'not_has',
    } as any;

    const [subQuery] = builder.segmentNodeToStateSubQuery(segment, node);

    expect(subQuery.argMaxValue).toContain("'contact.label.added'");
    expect(subQuery.argMaxValue).toContain(
      "JSONExtractString(traits, 'labelId') = 'lbl-1'",
    );
    expect(subQuery.argMaxValue).not.toContain('properties');
    expect(subQuery.argMaxValue).toContain(
      "HAVING argMax(if(event_name IN ('contact.label.added', 'label_added'), 'true', 'false'), occurred_at) = 'true'",
    );
  });

  // Definitions saved by the old editor hold the title; the event carries both traits,
  // so both spellings must match and no backfill is needed.
  it.each(['has', 'not_has'])(
    'Label %s: matches the stored value against labelId OR labelName',
    (condition) => {
      const node = {
        id: 'n1',
        type: SegmentNodeType.Label,
        labelId: 'VIP',
        condition,
      } as any;

      const [subQuery] = builder.segmentNodeToStateSubQuery(segment, node);
      const sql = `${subQuery.condition} ${subQuery.argMaxValue}`;

      expect(sql).toContain(
        "(JSONExtractString(traits, 'labelId') = 'VIP' OR JSONExtractString(traits, 'labelName') = 'VIP')",
      );
    },
  );

  it('every deleted-contacts guard matches contact.deleted (and the legacy spelling)', () => {
    const nodes = [
      {
        id: 'n2',
        type: SegmentNodeType.Label,
        labelId: 'lbl-1',
        condition: 'has',
      },
      {
        id: 'n3',
        type: SegmentNodeType.Label,
        labelId: 'lbl-1',
        condition: 'not_has',
      },
      {
        id: 'n4',
        type: SegmentNodeType.UserProperty,
        path: 'customAttributes.tier',
        operator: { type: 'Equals', value: 'x' },
        value: 'x',
      },
    ] as any[];

    for (const node of nodes) {
      const serialized = JSON.stringify(
        builder.segmentNodeToStateSubQuery(segment, node),
      );
      expect(serialized).toContain(
        "event_name IN ('contact.deleted', 'contact_deleted')",
      );
      expect(serialized).not.toContain("event_name = 'contact_deleted'");
    }
  });

  it('the execution-time rewrite still recognizes the deleted-contacts CASE branch', () => {
    const node = {
      id: 'n1',
      type: SegmentNodeType.Label,
      labelId: 'lbl-1',
      condition: 'has',
    } as any;
    const [subQuery] = builder.segmentNodeToStateSubQuery(segment, node);

    const rewritten = String(subQuery.argMaxValue).replace(
      deletedContactsCaseBranchRegex(),
      `WHEN 1=0 THEN 'false'`,
    );

    expect(rewritten).toContain(`WHEN 1=0 THEN 'false'`);
    expect(rewritten).not.toContain(DELETED_CONTACTS_SUBQUERY);
  });
});

describe('CRM-215 deleted-contacts cache never trades correctness for speed', () => {
  const builder = new SegmentClickHouseQueryBuilderService();
  const segment = { id: 'seg-1' } as any;
  const node = {
    id: 'n1',
    type: SegmentNodeType.Label,
    labelId: 'lbl-1',
    condition: 'not_has',
  } as any;

  it('keeps the real subselect when the cache is empty (empty cache ≠ no deleted contacts)', () => {
    const [subQuery] = builder.segmentNodeToStateSubQuery(segment, node);
    const sql = String(subQuery.argMaxValue);

    const out = applyDeletedContactsOptimization(sql, new Set());

    expect(out).toBe(sql);
    expect(out).toContain(DELETED_CONTACTS_SUBQUERY);
    expect(out).not.toContain('WHEN 1=0');
  });

  it('inlines the cached ids (escaped) when the cache has entries', () => {
    const [subQuery] = builder.segmentNodeToStateSubQuery(segment, node);
    const sql = String(subQuery.argMaxValue);

    const out = applyDeletedContactsOptimization(
      sql,
      new Set(['c-1', "x' OR '1'='1"]),
    );

    expect(out).not.toContain(DELETED_CONTACTS_SUBQUERY);
    expect(out).toContain(
      "WHEN contact_or_anonymous_id IN ('c-1','x'' OR ''1''=''1') THEN 'false'",
    );
  });

  // Most node types mark a deleted contact with the EMPTY sentinel and membership is
  // `argMaxMerge(last_value) != ''`: rewriting it to 'false' put the contact back in.
  it.each([
    [SegmentNodeType.LastPerformed, { event: 'order_placed' }],
    [SegmentNodeType.Performed, { event: 'order_placed' }],
    [SegmentNodeType.WhatsApp, { event: 'MessageSent' }],
  ])(
    '%s: keeps the empty sentinel of the deleted-contacts branch',
    (type, extra) => {
      const emptySentinelNode = { id: 'n-empty', type, ...extra } as any;
      const [subQuery] = builder.segmentNodeToStateSubQuery(
        segment,
        emptySentinelNode,
      );
      const lastValueSql = builder.generateArgMaxValidation(subQuery);
      expect(lastValueSql).toContain("THEN ''");

      const out = applyDeletedContactsOptimization(
        lastValueSql,
        new Set(['deleted-1']),
      );

      expect(out).toContain(
        "WHEN contact_or_anonymous_id IN ('deleted-1') THEN ''",
      );
      expect(out).not.toContain(DELETED_CONTACTS_SUBQUERY);
    },
  );

  it('does not read `$` inside a contact id as a capture reference', () => {
    const [subQuery] = builder.segmentNodeToStateSubQuery(segment, node);
    const sql = String(subQuery.argMaxValue);

    const out = applyDeletedContactsOptimization(
      sql,
      new Set(['a$&b', 'c$1d']),
    );

    expect(out).toContain(
      "WHEN contact_or_anonymous_id IN ('a$&b','c$1d') THEN 'false'",
    );
  });

  it('bypasses the cache for a short window after the signal (ClickHouse ingest is async)', async () => {
    const fetches: Set<string>[] = [
      new Set(['stale']),
      new Set(['stale', 'fresh']),
    ];
    const clickhouse = { query: jest.fn() } as any;
    const cache = new DeletedContactsCacheService(clickhouse);
    (cache as any).fetchDeletedContactsFromClickHouse = jest.fn(() =>
      Promise.resolve(fetches.shift() ?? new Set<string>()),
    );

    expect(await cache.getDeletedContacts()).toEqual(new Set(['stale']));
    expect(await cache.getDeletedContacts()).toEqual(new Set(['stale'])); // cache hit

    cache.onContactDeletedIngested();

    expect(await cache.getDeletedContacts()).toEqual(
      new Set(['stale', 'fresh']),
    ); // re-queried
    expect(
      (cache as any).fetchDeletedContactsFromClickHouse,
    ).toHaveBeenCalledTimes(2);
    expect((cache as any).expiresAt).toBeLessThanOrEqual(
      (cache as any).bypassCacheUntil,
    );
  });

  it('drops the cached snapshot when a deleted-contact event is ingested', () => {
    const cache = new DeletedContactsCacheService({} as any);
    (cache as any).cached = new Set(['c-1']);
    (cache as any).expiresAt = Number.MAX_SAFE_INTEGER;

    cache.onContactDeletedIngested();

    expect((cache as any).cached).toBeNull();
  });

  describe('concurrent fetches', () => {
    function deferredFetches(cache: DeletedContactsCacheService) {
      const pending: Array<(set: Set<string>) => void> = [];
      const fetch = jest.fn(
        () => new Promise<Set<string>>((resolve) => pending.push(resolve)),
      );
      (cache as any).fetchDeletedContactsFromClickHouse = fetch;
      return { fetch, pending };
    }

    it('shares one ClickHouse query among concurrent calls inside the bypass window', async () => {
      const cache = new DeletedContactsCacheService({} as any);
      const { fetch, pending } = deferredFetches(cache);
      cache.onContactDeletedIngested();

      const calls = Array.from({ length: 5 }, () => cache.getDeletedContacts());
      pending[0](new Set(['gone']));
      const results = await Promise.all(calls);

      expect(fetch).toHaveBeenCalledTimes(1);
      results.forEach((set) => expect(set).toEqual(new Set(['gone'])));
    });

    it('shares one query among concurrent cache misses outside the window too', async () => {
      const cache = new DeletedContactsCacheService({} as any);
      const { fetch, pending } = deferredFetches(cache);

      const calls = [cache.getDeletedContacts(), cache.getDeletedContacts()];
      pending[0](new Set(['gone']));
      await Promise.all(calls);

      expect(fetch).toHaveBeenCalledTimes(1);
      expect(await cache.getDeletedContacts()).toEqual(new Set(['gone'])); // cache hit
      expect(fetch).toHaveBeenCalledTimes(1);
    });

    it('does not re-cache a set fetched before the deletion signal arrived', async () => {
      const cache = new DeletedContactsCacheService({} as any);
      const { fetch, pending } = deferredFetches(cache);

      const beforeSignal = cache.getDeletedContacts();
      cache.onContactDeletedIngested();
      const afterSignal = cache.getDeletedContacts();

      expect(fetch).toHaveBeenCalledTimes(2); // the old fetch is not shared after the signal
      pending[0](new Set(['stale']));
      expect(await beforeSignal).toEqual(new Set(['stale']));
      expect((cache as any).cached).toBeNull();

      pending[1](new Set(['stale', 'fresh']));
      expect(await afterSignal).toEqual(new Set(['stale', 'fresh']));
      expect((cache as any).cached).toEqual(new Set(['stale', 'fresh']));
    });

    it('does not cache for 5 minutes a set whose fetch started inside the window and ended after it', async () => {
      const t0 = 1_000_000;
      const clock = jest.spyOn(Date, 'now').mockReturnValue(t0);
      try {
        const cache = new DeletedContactsCacheService({} as any);
        const { fetch, pending } = deferredFetches(cache);
        cache.onContactDeletedIngested(); // window open until t0 + 15 s

        clock.mockReturnValue(t0 + 10_000);
        const insideWindow = cache.getDeletedContacts();
        clock.mockReturnValue(t0 + 16_000); // ClickHouse answers after the window closed
        pending[0](new Set(['maybe-incomplete']));
        await insideWindow;

        clock.mockReturnValue(t0 + 17_000);
        const next = cache.getDeletedContacts();
        expect(fetch).toHaveBeenCalledTimes(2); // refetched, not served from cache
        pending[1](new Set(['maybe-incomplete', 'complete']));
        expect(await next).toEqual(new Set(['maybe-incomplete', 'complete']));
      } finally {
        clock.mockRestore();
      }
    });

    it('starts a new query once the shared one settles', async () => {
      const cache = new DeletedContactsCacheService({} as any);
      const { fetch, pending } = deferredFetches(cache);
      cache.onContactDeletedIngested();

      const first = cache.getDeletedContacts();
      pending[0](new Set(['a']));
      await first;
      const second = cache.getDeletedContacts(); // still inside the window: no cache hit
      pending[1](new Set(['a', 'b']));

      expect(await second).toEqual(new Set(['a', 'b']));
      expect(fetch).toHaveBeenCalledTimes(2);
    });
  });
});
