import { SegmentJobService } from './segment-job.service';

// nodeReferencesEvent is a pure predicate over its arguments (no `this` deps),
// so we can exercise it without wiring the injected repositories (EVO-1839 AC6).
describe('SegmentJobService.nodeReferencesEvent — CustomAttribute (EVO-1839)', () => {
  // configService.get is read in the constructor; return the requested default.
  const configService = { get: (_key: string, def?: unknown) => def } as any;
  const svc = new SegmentJobService(
    null as any,
    null as any,
    configService,
  ) as any;

  it('flags a CustomAttribute node on the canonical dotted event', () => {
    expect(
      svc.nodeReferencesEvent(
        { type: 'CustomAttribute' },
        'contact.custom_attribute.changed',
      ),
    ).toBe(true);
  });

  it('flags a CustomAttribute node on the legacy underscore event', () => {
    expect(
      svc.nodeReferencesEvent(
        { type: 'CustomAttribute' },
        'custom_attribute_changed',
      ),
    ).toBe(true);
  });

  it('does not flag a CustomAttribute node on an unrelated event', () => {
    expect(
      svc.nodeReferencesEvent({ type: 'CustomAttribute' }, 'contact.created'),
    ).toBe(false);
  });

  it('still honors Performed nodes (regression)', () => {
    expect(svc.nodeReferencesEvent({ type: 'Performed', event: 'x' }, 'x')).toBe(
      true,
    );
    expect(svc.nodeReferencesEvent({ type: 'Performed', event: 'x' }, 'y')).toBe(
      false,
    );
  });
});

describe('SegmentJobService.recomputeStaleSegments — batch logging', () => {
  const previousRunMode = process.env.RUN_MODE;
  beforeAll(() => {
    process.env.RUN_MODE = 'segment-worker';
  });
  afterAll(() => {
    if (previousRunMode === undefined) delete process.env.RUN_MODE;
    else process.env.RUN_MODE = previousRunMode;
  });

  const result = (segmentId: string, totalContacts: number) => ({
    segmentId,
    contactsAdded: 0,
    contactsRemoved: 0,
    totalContacts,
    processingTimeMs: 1,
  });

  it('logs each stale segment with its own result, whatever the batch order', async () => {
    const stale = [
      { id: 'a', name: 'Segment A' },
      { id: 'b', name: 'Segment B' },
    ];
    const queryBuilder = {
      where: () => queryBuilder,
      getMany: () => Promise.resolve(stale),
    };
    const segmentRepository = { createQueryBuilder: () => queryBuilder };
    const segmentComputationService = {
      computeAllSegments: () =>
        Promise.resolve([result('c', 7), result('b', 22), result('a', 23)]),
    };
    const configService = { get: () => 'segment-worker' };
    const svc = new SegmentJobService(
      segmentRepository as never,
      segmentComputationService as never,
      configService as never,
    );
    const lines: string[] = [];
    jest
      .spyOn(
        (svc as unknown as { logger: { log: (m: string) => void } }).logger,
        'log',
      )
      .mockImplementation((message: string) => {
        lines.push(message);
      });

    await svc.recomputeStaleSegments();

    const recomputed = lines.filter((l) => l.startsWith('Recomputed segment'));
    expect(recomputed).toHaveLength(2);
    expect(recomputed[0]).toMatch(/^Recomputed segment Segment B: 22 contacts/);
    expect(recomputed[1]).toMatch(/^Recomputed segment Segment A: 23 contacts/);
    expect(
      lines.find((l) => l.startsWith('Completed scheduled recomputation')),
    ).toMatch(/: 2\/2 segments/);
  });
});
