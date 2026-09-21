#!/usr/bin/env node
/**
 * Anti-forgetting guard for the tenant DB-context seam (ADR14, story 10.1b, AC4).
 *
 * The 16 tenant-scoped tables are protected by Postgres RLS. App code MUST reach
 * them through the `TenantDbContext` seam (HTTP: `this.db.getRepository(E)`;
 * Temporal: `runActivityInTenantDbContext(...)`) so queries run on a connection
 * carrying `app.current_tenant_id`. Reaching them through the GLOBAL pool —
 * `@InjectRepository(E)` or `AppDataSource/dataSource.getRepository(E)` — bypasses
 * the seam: under multi-tenant that query silently returns zero rows (RLS with no
 * GUC) or, worse, leaks if RLS is ever relaxed.
 *
 * This guard fails the build when such a bypass is added for any of the 16
 * entities OUTSIDE the explicit allowlist of files whose tenant-scoping is
 * intentionally deferred (documented below). It does NOT flag seam access
 * (`db.getRepository`, `manager.getRepository`, `em.getRepository`).
 *
 * Run: `node scripts/check-tenant-db-context.mjs` (also `npm run lint:tenant-db`).
 */
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(fileURLToPath(new URL('.', import.meta.url)), '..');
const SRC = join(ROOT, 'src');

/** The 16 tenant-scoped entities (migration-targets.ts in the enterprise overlay). */
const TENANT_ENTITIES = [
  'Journey',
  'JourneySession',
  'ScheduledJourneyAction',
  'Campaign',
  'CampaignContact',
  'CampaignTemplate',
  'CampaignConfig',
  'CampaignExecution',
  'Segment',
  'Tag',
  'Tagging',
  'MessageTemplate',
  'ShortLink',
  'LinkParameter',
  'CustomDomain',
  'User',
];

const ENTITY_ALT = TENANT_ENTITIES.join('|');
// @InjectRepository(Entity) — DI of a global-pool repository.
const INJECT_RE = new RegExp(`@InjectRepository\\(\\s*(${ENTITY_ALT})\\b`);
// AppDataSource/dataSource(.connection)?.getRepository(Entity | 'Entity') — direct
// pool access. Seam access via db/manager/em.getRepository is intentionally NOT matched.
const DIRECT_RE = new RegExp(
  `\\b(?:AppDataSource|dataSource|connection)\\b[^\\n;]*\\.getRepository\\(\\s*['"\`]?(${ENTITY_ALT})\\b`,
);

/**
 * Files exempt from the ban, each with the reason its seam migration is deferred.
 * Keep this list SHRINKING — every entry is known tenant-isolation debt.
 */
const ALLOWLIST = new Map([
  // Seam implementations themselves legitimately resolve repositories.
  ['src/evo-extension-points/tenant-db-context/tenant-db-context.service.ts', 'seam implementation'],
  ['src/modules/temporal/tenant-activity-context.ts', 'seam implementation (Temporal)'],
  // Redis cache layer — tenant-partitioning is a separate story (the cache short-
  // circuits the DB on the read path, so RLS alone cannot isolate it).
  ['src/modules/cache/services/journey-cache.service.ts', 'cache tenant-partitioning — separate story'],
  ['src/modules/cache/services/journey-session-cache.service.ts', 'cache tenant-partitioning — separate story'],
  ['src/modules/cache/services/link-cache.service.ts', 'cache tenant-partitioning — separate story'],
  ['src/modules/cache/services/segment-cache.service.ts', 'cache tenant-partitioning — separate story'],
  // Cron / queue / Kafka-consumer background services — no request/Temporal tenant
  // source yet; run single-tenant DEFAULT_TENANT_ID until multi-tenant go-live.
  ['src/modules/segments/services/segment-scheduler.service.ts', 'background cron — deferred to go-live'],
  ['src/modules/segments/services/segment-job.service.ts', 'background cron — deferred to go-live'],
  ['src/modules/segments/services/segment-queue.service.ts', 'background queue — deferred to go-live'],
  ['src/modules/processing/services/atomic-processor.service.ts', 'background processor — deferred to go-live'],
  ['src/modules/processing/services/batch-processor.service.ts', 'background processor — deferred to go-live'],
  ['src/modules/processing/services/enhanced-cron-segment-processor.service.ts', 'background cron — deferred to go-live'],
  ['src/modules/journeys/services/journey-trigger-processor.service.ts', 'event-consumer — deferred to go-live'],
  ['src/modules/journeys/services/wait-registry.service.ts', 'Temporal wait registry — deferred to go-live'],
  // Temporal activities/nodes/utils — full payload→activity tenant propagation is
  // staged; one reference site (updateExecutionProgress) already uses the seam.
  ['src/modules/temporal/activities/journey-execution.activities.ts', 'Temporal propagation staged'],
  ['src/modules/temporal/activities/campaign-message-sending.activities.ts', 'Temporal propagation staged'],
  ['src/modules/temporal/activities/action-nodes.activities.ts', 'Temporal propagation staged'],
  ['src/modules/temporal/activities/wait.activities.ts', 'Temporal propagation staged'],
  ['src/modules/temporal/activities/nodes/base.node.ts', 'Temporal propagation staged'],
  ['src/modules/temporal/activities/nodes/scheduled-action.node.ts', 'Temporal propagation staged'],
  ['src/modules/temporal/activities/nodes/conditional.node.ts', 'Temporal propagation staged'],
  ['src/modules/temporal/activities/utils/variable-interpolation.util.ts', 'Temporal propagation staged'],
]);

/**
 * Every allowlist entry is known tenant-isolation debt that must SHRINK toward
 * go-live (ADR14 consequence). This ratchet fails the build if the list grows
 * past its recorded size, so a new global-pool bypass cannot be quietly
 * allowlisted in. Lower the ceiling whenever an entry is removed; raising it
 * requires a deliberate, justified edit in the PR.
 */
const ALLOWLIST_CEILING = 22;
if (ALLOWLIST.size > ALLOWLIST_CEILING) {
  console.error(
    `\n✖ tenant-db-context guard: ALLOWLIST grew to ${ALLOWLIST.size} entries ` +
      `(ceiling ${ALLOWLIST_CEILING}).\n` +
      `  The allowlist is tenant-isolation debt and must only shrink. Migrate the new\n` +
      `  file to the TenantDbContext seam instead of allowlisting it; if it is genuinely\n` +
      `  deferred, raise ALLOWLIST_CEILING in this script with justification in the PR.\n`,
  );
  process.exit(1);
}

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    const st = statSync(full);
    if (st.isDirectory()) out.push(...walk(full));
    else if (name.endsWith('.ts') && !name.endsWith('.spec.ts')) out.push(full);
  }
  return out;
}

const violations = [];
for (const file of walk(SRC)) {
  const rel = relative(ROOT, file);
  if (ALLOWLIST.has(rel)) continue;
  const lines = readFileSync(file, 'utf8').split('\n');
  lines.forEach((line, i) => {
    // Skip comment lines — references in JSDoc/prose (e.g. "replaces the former
    // @InjectRepository(Journey)") are documentation, not real pool access.
    const trimmed = line.trim();
    if (
      trimmed.startsWith('*') ||
      trimmed.startsWith('//') ||
      trimmed.startsWith('/*')
    ) {
      return;
    }
    const inject = INJECT_RE.exec(line);
    if (inject) {
      violations.push({ rel, line: i + 1, entity: inject[1], kind: '@InjectRepository' });
    }
    const direct = DIRECT_RE.exec(line);
    if (direct) {
      violations.push({ rel, line: i + 1, entity: direct[1], kind: 'direct pool getRepository' });
    }
  });
}

if (violations.length > 0) {
  console.error(
    '\n✖ tenant-db-context guard (ADR14 / EVO-1611 AC4): tenant-scoped table accessed\n' +
      '  through the GLOBAL pool instead of the TenantDbContext seam.\n',
  );
  for (const v of violations) {
    console.error(`  ${v.rel}:${v.line} — ${v.kind}(${v.entity})`);
  }
  console.error(
    '\n  Fix: resolve the repository through the seam so RLS scopes the query:\n' +
      '    HTTP/DI services → inject `TenantDbContext` and use `this.db.getRepository(Entity)`\n' +
      '    Temporal activities → `runActivityInTenantDbContext(tenantId, (m) => m.getRepository(Entity)…)`\n' +
      '  If the bypass is intentional and tenant-isolation is deferred, add the file to\n' +
      '  the documented ALLOWLIST in scripts/check-tenant-db-context.mjs with a reason.\n',
  );
  process.exit(1);
}

console.log(
  `✓ tenant-db-context guard: no global-pool access to the ${TENANT_ENTITIES.length} ` +
    `tenant-scoped entities outside the allowlist (${ALLOWLIST.size} files).`,
);
