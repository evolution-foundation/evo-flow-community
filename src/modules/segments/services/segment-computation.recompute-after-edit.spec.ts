import { SegmentComputationService } from './segment-computation.service';

type StoredSegment = {
  id: string;
  definition: unknown;
  lastComputedAt: Date | null;
  definitionUpdatedAt: Date | null;
};

const labelDefinition = {
  entryNode: { id: 'entry', type: 'And', children: ['n1'] },
  nodes: [{ id: 'n1', type: 'Label', labelId: 'l1', condition: 'has' }],
};

const timeWindowDefinition = {
  entryNode: { id: 'entry', type: 'And', children: ['n1'] },
  nodes: [{ id: 'n1', type: 'Performed', event: 'x', withinSeconds: 60 }],
};

function build(segments: StoredSegment[]) {
  const whereClauses: Array<[string, Record<string, unknown> | undefined]> = [];
  type UpdateQuery = {
    update: jest.Mock<UpdateQuery>;
    set: jest.Mock<UpdateQuery>;
    where: jest.Mock<UpdateQuery>;
    andWhere: jest.Mock<UpdateQuery, [string, Record<string, unknown>?]>;
    execute: jest.Mock<Promise<{ affected: number }>>;
  };
  const query: UpdateQuery = {
    update: jest.fn(() => query),
    set: jest.fn(() => query),
    where: jest.fn(() => query),
    andWhere: jest.fn((clause: string, params?: Record<string, unknown>) => {
      whereClauses.push([clause, params]);
      return query;
    }),
    execute: jest.fn(() => Promise.resolve({ affected: 1 })),
  };
  const repository = {
    find: jest.fn(() => Promise.resolve(segments)),
    findOne: jest.fn(({ where }: { where: { id: string } }) =>
      Promise.resolve(segments.find((s) => s.id === where.id) ?? null),
    ),
    createQueryBuilder: jest.fn(() => query),
  };
  const calls: string[] = [];
  const result = (segmentId: string) => ({
    segmentId,
    contactsAdded: 0,
    contactsRemoved: 0,
    totalContacts: 3,
    processingTimeMs: 1,
  });
  const modular = {
    cleanupOldSegmentData: jest.fn((id: string) => {
      calls.push(`cleanup:${id}`);
      return Promise.resolve();
    }),
    computeSegment: jest.fn((s: StoredSegment) => {
      calls.push(`compute:${s.id}`);
      return Promise.resolve(result(s.id));
    }),
    computeSegmentsBatch: jest.fn((list: StoredSegment[]) => {
      calls.push('batch');
      return Promise.resolve(list.map((s) => result(s.id)));
    }),
  };

  const service = new SegmentComputationService(
    { getRepository: () => repository } as never,
    modular as never,
    {} as never,
    { invalidateSegment: jest.fn(() => Promise.resolve()) } as never,
    { emit: jest.fn() } as never,
  );
  return { service, modular, query, whereClauses, calls };
}

describe('SegmentComputationService after a definition edit', () => {
  const computed = new Date('2026-11-18T11:35:00Z');
  const edited = new Date('2026-11-18T11:40:00Z');

  it('batch recompute drops the old ClickHouse state of edited and new segments before computing', async () => {
    const { service, calls } = build([
      {
        id: 'edited',
        definition: labelDefinition,
        lastComputedAt: null,
        definitionUpdatedAt: edited,
      },
      {
        id: 'legacy-edit',
        definition: labelDefinition,
        lastComputedAt: computed,
        definitionUpdatedAt: edited,
      },
      {
        id: 'up-to-date',
        definition: labelDefinition,
        lastComputedAt: edited,
        definitionUpdatedAt: computed,
      },
      {
        id: 'time-window',
        definition: timeWindowDefinition,
        lastComputedAt: null,
        definitionUpdatedAt: edited,
      },
    ]);

    await service.computeAllSegments();

    expect(calls).toEqual(['cleanup:edited', 'cleanup:legacy-edit', 'batch']);
  });

  it('single recompute drops the old state of an edited segment before computing', async () => {
    const { service, calls } = build([
      {
        id: 'edited',
        definition: labelDefinition,
        lastComputedAt: null,
        definitionUpdatedAt: edited,
      },
    ]);

    await service.computeSegment('edited');

    expect(calls).toEqual(['cleanup:edited', 'compute:edited']);
  });

  it('stores the result only if the definition is still the one that was computed', async () => {
    const { service, whereClauses } = build([
      {
        id: 'edited',
        definition: labelDefinition,
        lastComputedAt: null,
        definitionUpdatedAt: edited,
      },
    ]);

    await service.computeSegment('edited');

    expect(whereClauses).toEqual([
      [
        "date_trunc('milliseconds', definition_updated_at) = :loaded",
        { loaded: edited },
      ],
    ]);
  });

  it('requires definition_updated_at to still be NULL when the segment was never edited', async () => {
    const { service, whereClauses } = build([
      {
        id: 'new',
        definition: labelDefinition,
        lastComputedAt: null,
        definitionUpdatedAt: null,
      },
    ]);

    await service.computeSegment('new');

    expect(whereClauses).toEqual([
      ['definition_updated_at IS NULL', undefined],
    ]);
  });

  it('does not fail when an edit during the computation discards the result', async () => {
    const { service, query } = build([
      {
        id: 'edited',
        definition: labelDefinition,
        lastComputedAt: null,
        definitionUpdatedAt: edited,
      },
    ]);
    query.execute.mockResolvedValueOnce({ affected: 0 });

    await expect(service.computeSegment('edited')).resolves.toMatchObject({
      segmentId: 'edited',
      totalContacts: 3,
    });
  });
});
