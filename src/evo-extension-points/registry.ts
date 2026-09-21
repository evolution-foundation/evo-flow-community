import type { Request } from 'express';
import type { DynamicModule } from '@nestjs/common';
import type { DataSource, EntityManager } from 'typeorm';
import type { ClientInterceptors } from '@temporalio/client';
import type { WorkerInterceptors } from '@temporalio/worker';
import { ExtensionPointName } from './version';

export type CapabilityGateImpl = (
  name: string,
  context: Record<string, unknown>,
) => boolean;

export interface RuntimeContext {
  request_id: string;
  user_id: string | null;
  scope_id: string | null;
  feature_flags: Record<string, boolean>;
  [key: string]: unknown;
}

export type RuntimeContextImpl = (
  req: Request,
  defaultContext: RuntimeContext,
) => Promise<RuntimeContext> | RuntimeContext;

export interface PluginLoaderOptions {
  modules: DynamicModule[];
  onLoad?: (loaded: string[]) => void;
}

export type PluginLoaderImpl = () =>
  | PluginLoaderOptions
  | Promise<PluginLoaderOptions>;

export interface ThemeTokens {
  brand_name?: string;
  logo_url?: string;
  primary_color?: string;
  support_email?: string;
  sender_name?: string;
  [key: string]: string | undefined;
}

export type ThemeTokensImpl = (
  scopeId: string | null,
) => Promise<ThemeTokens> | ThemeTokens;

/**
 * DB-context seam (ADR14, story 10.1b). Runs `work` against an `EntityManager`
 * that is guaranteed to carry the tenant's RLS context. The community default is
 * a no-op passthrough to the global pool manager (single-tenant / OSS parity); an
 * enterprise overlay replaces it with a per-request transaction on a dedicated
 * connection that applies `SELECT set_config('app.current_tenant_id', $1, true)`
 * so Postgres RLS policies filter every query on that connection.
 *
 * The runtime never knows about tenant resolution: it passes the already-resolved
 * `tenantId` (from `runtime_context`, story 10.1) and its own `DataSource`. The
 * impl decides whether to open a transaction, set the GUC, or reject a missing
 * context. Used by both the HTTP path (via the `TenantDbContext` provider) and
 * Temporal activities (which pass `tenantId` from the workflow payload).
 */
export type TenantDbContextImpl = <T>(
  dataSource: DataSource,
  tenantId: string | null,
  work: (manager: EntityManager) => Promise<T>,
) => Promise<T>;

/**
 * Cache-key scope seam. Returns an opaque per-request scope suffix that the cache
 * layer folds into its Redis keys/index, so caches don't share one global
 * namespace across the active scope. The runtime never knows what the scope IS —
 * the community default returns `''` (single-scope / OSS parity: one shared
 * namespace, today's behaviour). An enterprise overlay returns a non-empty,
 * request-bound suffix so each scope gets its own cache namespace + index.
 *
 * Returning a string keeps the cache code trivial: `key = base + suffix`. Empty
 * suffix = unchanged keys (no migration, no behaviour change in standalone).
 */
export type CacheKeyScopeImpl = () => string;

/**
 * Opaque headers stamped on everything the process sends out: broker publishes
 * and calls to the CRM. The runtime never reads the map; callers' own headers
 * win on a key clash. Community default returns `{}`.
 */
export type OutboundHeadersImpl = () => Record<string, string>;

/**
 * Wraps the processing of one inbound broker message, given the headers it was
 * published with. Community default just runs `work`.
 */
export type InboundMessageContextImpl = <T>(
  headers: Record<string, string>,
  work: () => Promise<T>,
) => Promise<T>;

export interface TemporalInterceptorSet {
  client?: ClientInterceptors;
  worker?: Pick<WorkerInterceptors, 'activity' | 'workflowModules'>;
}

/**
 * Temporal interceptors for the campaign client and worker. Workflow
 * interceptors run inside the workflow sandbox, so they are handed over as
 * module paths (`worker.workflowModules`), not objects. Community default
 * returns `{}`.
 */
export type TemporalInterceptorsImpl = (
  dataSource: DataSource,
) => TemporalInterceptorSet;

export interface ExtensionPointImplementations {
  capability_gate: CapabilityGateImpl;
  runtime_context: RuntimeContextImpl;
  plugin_loader: PluginLoaderImpl;
  theme_tokens: ThemeTokensImpl;
  tenant_db_context: TenantDbContextImpl;
  cache_key_scope: CacheKeyScopeImpl;
  outbound_headers: OutboundHeadersImpl;
  inbound_message_context: InboundMessageContextImpl;
  temporal_interceptors: TemporalInterceptorsImpl;
}

const defaultCapabilityGate: CapabilityGateImpl = () => true;

const defaultRuntimeContext: RuntimeContextImpl = (_req, defaultContext) =>
  defaultContext;

const defaultPluginLoader: PluginLoaderImpl = () => ({ modules: [] });

const defaultThemeTokens: ThemeTokensImpl = () => ({});

// No-op passthrough: run the work on the global pool manager, no transaction, no
// GUC. Preserves single-tenant / OSS behavior exactly (community is untouched).
const defaultTenantDbContext: TenantDbContextImpl = (
  dataSource,
  _tenantId,
  work,
) => work(dataSource.manager);

// No scope suffix: caches keep their single global namespace (OSS / standalone
// parity). An enterprise overlay returns a request-bound suffix.
const defaultCacheKeyScope: CacheKeyScopeImpl = () => '';

const defaultOutboundHeaders: OutboundHeadersImpl = () => ({});

const defaultInboundMessageContext: InboundMessageContextImpl = (
  _headers,
  work,
) => work();

const defaultTemporalInterceptors: TemporalInterceptorsImpl = () => ({});

class ExtensionPointRegistry {
  private readonly implementations: ExtensionPointImplementations = {
    capability_gate: defaultCapabilityGate,
    runtime_context: defaultRuntimeContext,
    plugin_loader: defaultPluginLoader,
    theme_tokens: defaultThemeTokens,
    tenant_db_context: defaultTenantDbContext,
    cache_key_scope: defaultCacheKeyScope,
    outbound_headers: defaultOutboundHeaders,
    inbound_message_context: defaultInboundMessageContext,
    temporal_interceptors: defaultTemporalInterceptors,
  };

  replace<K extends ExtensionPointName>(
    name: K,
    impl: ExtensionPointImplementations[K],
  ): void {
    if (!(name in this.implementations)) {
      throw new Error(`Unknown extension point: ${String(name)}`);
    }
    if (typeof impl !== 'function') {
      throw new TypeError(
        `Extension point '${String(name)}' implementation must be a function`,
      );
    }
    this.implementations[name] = impl;
  }

  get<K extends ExtensionPointName>(name: K): ExtensionPointImplementations[K] {
    return this.implementations[name];
  }

  reset(): void {
    this.implementations.capability_gate = defaultCapabilityGate;
    this.implementations.runtime_context = defaultRuntimeContext;
    this.implementations.plugin_loader = defaultPluginLoader;
    this.implementations.theme_tokens = defaultThemeTokens;
    this.implementations.tenant_db_context = defaultTenantDbContext;
    this.implementations.cache_key_scope = defaultCacheKeyScope;
    this.implementations.outbound_headers = defaultOutboundHeaders;
    this.implementations.inbound_message_context = defaultInboundMessageContext;
    this.implementations.temporal_interceptors = defaultTemporalInterceptors;
  }
}

export const EvoExtensionPoints = new ExtensionPointRegistry();
