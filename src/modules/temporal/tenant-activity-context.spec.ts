// Mock the dynamically-imported DataSource so the helper runs with no real DB.
jest.mock('../../database/ormconfig', () => ({
  AppDataSource: { isInitialized: true, manager: { tag: 'global-pool' } },
}));

import { EntityManager } from 'typeorm';
import { ClsServiceManager } from 'nestjs-cls';
import {
  EvoExtensionPoints,
  TENANT_DB_MANAGER_CLS_KEY,
} from '../../evo-extension-points';
import { runActivityInTenantDbContext } from './tenant-activity-context';

/**
 * Story 10.1b AC3 — the Temporal-side seam. Proves the helper threads the
 * payload tenant through the registered `tenant_db_context` impl and surfaces its
 * scoping / explicit-failure behaviour. The runner's actual `set_config`/throw
 * logic is covered by the overlay's runner spec; here we prove the wiring.
 */
describe('runActivityInTenantDbContext', () => {
  afterEach(() => EvoExtensionPoints.reset());

  it('community default runs the work on the global pool manager (no scoping)', async () => {
    const got = await runActivityInTenantDbContext('tenant-A', (m) =>
      Promise.resolve(m),
    );
    expect(got).toEqual({ tag: 'global-pool' });
  });

  it('passes the payload tenant to the registered impl and runs on the scoped manager', async () => {
    const seen: { tenantId?: string | null } = {};
    EvoExtensionPoints.replace('tenant_db_context', (_ds, tenantId, work) => {
      seen.tenantId = tenantId;
      return work({ tag: 'scoped' } as unknown as EntityManager);
    });

    const got = await runActivityInTenantDbContext('tenant-B', (m) =>
      Promise.resolve(m),
    );

    expect(seen.tenantId).toBe('tenant-B');
    expect(got).toEqual({ tag: 'scoped' });
  });

  it('runs on the manager already bound in CLS when the payload has no tenant', async () => {
    const impl = jest.fn();
    EvoExtensionPoints.replace('tenant_db_context', impl);
    const cls = ClsServiceManager.getClsService();

    const got = await cls.run(() => {
      cls.set(TENANT_DB_MANAGER_CLS_KEY, { tag: 'bound' });
      return runActivityInTenantDbContext(undefined, (m) => Promise.resolve(m));
    });

    expect(got).toEqual({ tag: 'bound' });
    expect(impl).not.toHaveBeenCalled();
  });

  it('propagates an explicit failure (missing tenant under multi-tenant) instead of leaking', async () => {
    EvoExtensionPoints.replace('tenant_db_context', () => {
      throw new Error('TENANT_CONTEXT_REQUIRED');
    });

    await expect(
      runActivityInTenantDbContext(null, () => Promise.resolve('unreachable')),
    ).rejects.toThrow('TENANT_CONTEXT_REQUIRED');
  });
});
