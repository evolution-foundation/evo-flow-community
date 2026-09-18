# Evo Flow — Extension Points

**Contract version:** `1.3.0`
**Stack:** NestJS 11 + TypeScript + TypeORM + BullMQ + Temporal

Evo Flow is the automation engine of the Evo CRM Community family. It powers journeys, segments, campaigns, events, and click-tracking. This document declares the **public extension points** that the runtime exposes so external consumers can compose, replace, or extend behavior without forking the codebase.

The contract is governed by `architecture.md` ADR13 (single source of truth for extension-point policy across the family). Any change to the API surface of an extension point requires a SemVer bump on that point and a deprecation window aligned with the Compatibility Promise below.

---

## Compatibility Promise

- **Backward compatibility is forever** within a major version of an extension point. Code written against `capability_gate@1.x` keeps working until a future `2.0.0` is announced.
- **Each extension point versions independently.** Bumping the major of `runtime_context` does not invalidate consumers of the other three.
- **Deprecation window of at least one minor release** is announced before any extension point removes a method, changes a return type, or adds a required argument. The deprecated symbol stays callable (with a runtime warning) for the duration of that window.
- **Additive changes are minor bumps.** Adding a new accepted name to `capability_gate`, a new optional key to `runtime_context`, or a new module slot to `plugin_loader` is a minor bump and never breaks an existing consumer.
- **Default implementations are no-ops** in the community runtime. Replacing them is the entire purpose of this contract; the community ships with placeholders that always allow / always return empty / never load anything.

---

## Registration API

Consumers register their implementation at boot time, before the NestJS application factory finishes wiring modules. The community runtime exposes a single registration surface:

```ts
import { EvoExtensionPoints } from './src/evo-extension-points';

EvoExtensionPoints.replace('capability_gate', myCapabilityGateImplementation);
EvoExtensionPoints.replace('runtime_context', myRuntimeContextEnricher);
EvoExtensionPoints.replace('plugin_loader', myPluginLoaderConfig);
EvoExtensionPoints.replace('theme_tokens', myThemeTokensProvider);
EvoExtensionPoints.replace('tenant_db_context', myTenantDbContextRunner);
```

`EvoExtensionPoints.replace(name, impl)` validates the shape of `impl` at registration time and throws if the contract is violated. The registry refuses to replace an unknown extension point name; the set of valid names is frozen on the version declared at the top of this document.

The default implementations are registered at process start. A consumer overrides them by calling `replace` before `NestFactory.create(AppModule)` resolves. After the application context is bootstrapped, replacements are rejected.

### Bootstrap (zero-fork registration)

The community runtime triggers consumer registration without any code change in this repository, via the `EVO_EXTENSIONS_BOOTSTRAP` environment variable. When set to a module specifier, the runtime imports that module at process start (before the module graph is built) and calls its exported `register(registry)` function, passing the live `EvoExtensionPoints` singleton:

```ts
// the consumer package (e.g. an enterprise overlay) exports:
export function register(registry: typeof EvoExtensionPoints): void {
  registry.replace('runtime_context', myEnricher);
  registry.replace('plugin_loader', () => ({ modules: [MyModule] }));
}
```

```bash
# enable the consumer (e.g. in the deployment image):
EVO_EXTENSIONS_BOOTSTRAP=@my-org/my-extension-package
```

When the variable is unset — the default OSS / standalone case — nothing is loaded and the no-op defaults stand. The registry instance is passed in so the consumer mutates the same singleton the runtime reads. `plugin_loader` modules returned by the registered factory are imported into `AppModule` at boot; `runtime_context` is applied per request by the globally-mounted `RuntimeContextMiddleware`.

---

## Extension points

### 1. `capability_gate`

**Version:** `1.0.0`
**Default:** always returns `true`.

```ts
type CapabilityGate = (name: string, context: Record<string, unknown>) => boolean;
```

The runtime calls this hook every time a route handler or service guarded by `@CapabilityGate('name')` runs. The community default permits everything; an external consumer can plug in feature-flag logic, license checks, or quota inspection.

Usage in a controller:

```ts
@Controller('journeys')
export class JourneysController {
  @CapabilityGate('journey.duplicate')
  @Post(':id/duplicate')
  duplicate(@Param('id') id: string) {
    // handler body
  }
}
```

Override:

```ts
EvoExtensionPoints.replace('capability_gate', (name, context) => {
  return myConsumer.isEnabled(name, context);
});
```

**Breaking-change policy:** changing the return type from `boolean`, renaming the function, or adding a required positional argument is a major bump. Adding a new accepted `name` to the runtime or a new key to `context` is a minor bump.

---

### 2. `runtime_context`

**Version:** `1.0.0`
**Default:** populates only `{ request_id, user_id }` from the incoming request. All other slots resolve to `null`.

```ts
interface RuntimeContext {
  request_id: string;
  user_id: string | null;
  scope_id: string | null;
  feature_flags: Record<string, boolean>;
  [key: string]: unknown;
}

type RuntimeContextEnricher = (req: Request, defaultContext: RuntimeContext) => Promise<RuntimeContext> | RuntimeContext;
```

The community runtime registers `RuntimeContextMiddleware` globally in `AppModule`. The middleware builds the default context from the JWT and request metadata, then hands the object to the enricher hook. The result is attached to the request and made available downstream through a request-scoped provider:

```ts
@Injectable({ scope: Scope.REQUEST })
export class MyService {
  constructor(@Inject(RUNTIME_CONTEXT) private readonly ctx: RuntimeContext) {}
}
```

Override:

```ts
EvoExtensionPoints.replace('runtime_context', async (req, defaultContext) => {
  if (!defaultContext.user_id) return defaultContext;
  return {
    ...defaultContext,
    scope_id: await myConsumer.resolveScope(req),
    feature_flags: await myConsumer.flagsFor(defaultContext.user_id),
  };
});
```

`user_id` is nullable in the default context (unauthenticated requests, system jobs). The consumer must guard against `null` before forwarding it to APIs that require an authenticated user.

**Breaking-change policy:** renaming `request_id` or `user_id`, changing their type, or removing the `defaultContext` parameter is a major bump. Adding a new optional key to the context (consumers must not assume it is absent) is a minor bump.

---

### 3. `plugin_loader`

**Version:** `1.0.0`
**Default:** loads zero plugins. The module is registered but the modules list is empty.

```ts
interface PluginLoaderOptions {
  modules: DynamicModule[];
  onLoad?: (loaded: string[]) => void;
}

type PluginLoaderFactory = () => PluginLoaderOptions | Promise<PluginLoaderOptions>;
```

The community runtime wires `PluginLoaderModule.forRootAsync({ useFactory })` in `AppModule`. The factory hook returns the list of NestJS dynamic modules that should be registered at boot. The default factory returns `{ modules: [] }`; replacement implementations return real modules.

Usage in `AppModule`:

```ts
@Module({
  imports: [
    PluginLoaderModule.forRootAsync({
      useFactory: () => EvoExtensionPoints.get('plugin_loader')(),
    }),
    // other modules
  ],
})
export class AppModule {}
```

Override:

```ts
EvoExtensionPoints.replace('plugin_loader', () => ({
  modules: [MyAnalyticsModule, MyComplianceModule],
  onLoad: (names) => console.log('Loaded plugins:', names.join(', ')),
}));
```

**Breaking-change policy:** changing the shape of `PluginLoaderOptions` (renaming `modules`, removing `onLoad`) is a major bump. Adding an optional callback slot to the options interface is a minor bump.

---

### 4. `theme_tokens`

**Version:** `1.0.0`
**Default:** returns an empty object `{}`.

```ts
interface ThemeTokens {
  brand_name?: string;
  logo_url?: string;
  primary_color?: string;
  support_email?: string;
  sender_name?: string;
  [key: string]: string | undefined;
}

type ThemeTokensProvider = (scope_id: string | null) => Promise<ThemeTokens> | ThemeTokens;
```

Evo Flow renders e-mail and SMS templates produced by campaigns and journeys. When the templates need branding tokens (sender name, logo, primary color, support address), the runtime calls `theme_tokens` with the current `scope_id` and merges the returned tokens into the template context. The community default returns nothing, so templates fall back to the placeholder values defined in the template itself.

Override:

```ts
EvoExtensionPoints.replace('theme_tokens', async (scope_id) => {
  if (!scope_id) return {};
  return myConsumer.loadThemeTokens(scope_id);
});
```

**Breaking-change policy:** renaming a documented key (`brand_name`, `logo_url`, etc.) or changing the function signature is a major bump. Adding a new optional key to `ThemeTokens` is a minor bump.

---

### 5. `tenant_db_context`

**Version:** `1.0.0` (added in contract `1.1.0`)
**Default:** no-op passthrough — runs the work on the global pool `EntityManager`, no transaction, no session config. Single-tenant / OSS behavior is unchanged.

```ts
type TenantDbContextImpl = <T>(
  dataSource: DataSource,
  tenantId: string | null,
  work: (manager: EntityManager) => Promise<T>,
) => Promise<T>;
```

The DB-context seam (ADR14). Tenant-scoped services resolve their `Repository`/`EntityManager` through the `TenantDbContext` provider rather than the global pool, so every query against a tenant-scoped table runs on a connection that carries the tenant's row-level-security context. The runtime stays ignorant of tenancy: it passes the already-resolved `tenantId` (from `runtime_context`) and its own `DataSource`; the impl decides whether to open a transaction, apply `SELECT set_config('app.current_tenant_id', $1, true)`, or reject a missing context.

Usage in a tenant-scoped service:

```ts
@Injectable()
export class JourneysService {
  constructor(private readonly db: TenantDbContext) {}

  findAll() {
    // Lands on the connection carrying app.current_tenant_id when an overlay is
    // active; on the global pool manager otherwise (community / single-tenant).
    return this.db.getRepository(Journey).find();
  }
}
```

Override (enterprise overlay — per-request transaction + RLS GUC):

```ts
EvoExtensionPoints.replace('tenant_db_context', (dataSource, tenantId, work) => {
  if (multiTenantEnabled && !tenantId) {
    // Fail explicitly — never run a tenant-scoped query with no context (which
    // RLS would silently return as zero rows).
    throw new TenantContextMissingError();
  }
  return dataSource.transaction(async (manager) => {
    await manager.query(`SELECT set_config('app.current_tenant_id', $1, true)`, [
      tenantId,
    ]);
    return work(manager);
  });
});
```

The HTTP path wraps the whole request handler in this through a per-request interceptor; **Temporal activities** reapply it via `runInTenantDbContext(dataSource, tenantId, work)`, reading `tenantId` from the workflow/activity payload. Community standalone never registers an override, so both paths run on the global pool unchanged.

**Breaking-change policy:** changing the signature (parameter order/types) or the contract that the impl receives an already-resolved `tenantId` is a major bump. The community default never opens a transaction, so OSS consumers see no behavior change.

---

### 6. `outbound_headers`

**Version:** `1.0.0` (added in contract `1.3.0`)
**Default:** returns `{}` — nothing extra is sent.

```ts
type OutboundHeadersImpl = () => Record<string, string>;
```

Opaque headers merged into every broker publish and every call to the CRM (`CrmClientService`, `CrmInboxDispatcher`). The runtime never reads the map. Headers set by the caller or the transport (`X-Service-Token`, `correlationId`, `messageId`, …) win on a key clash.

**Breaking-change policy:** changing where the map is applied, or letting it override the runtime's own headers, is a major bump.

---

### 7. `inbound_message_context`

**Version:** `1.0.0` (added in contract `1.3.0`)
**Default:** runs `work` unchanged.

```ts
type InboundMessageContextImpl = <T>(
  headers: Record<string, string>,
  work: () => Promise<T>,
) => Promise<T>;
```

Wraps the processing of every broker message (all topics, both adapters) with the headers it was published with, so a consumer can restore whatever `outbound_headers` put on the wire. It runs outside the consumer's own CLS scope, which inherits from it. Ack/nack still happens inside `work`; an error thrown before `work` runs leaves the message un-acked, exactly like a handler error.

**Breaking-change policy:** changing the signature or moving the wrap inside the ack policy is a major bump.

---

### 8. `temporal_interceptors`

**Version:** `1.0.0` (added in contract `1.3.0`)
**Default:** returns `{}` — the campaign client and worker run without interceptors.

```ts
interface TemporalInterceptorSet {
  client?: ClientInterceptors;
  worker?: Pick<WorkerInterceptors, 'activity' | 'workflowModules'>;
}
type TemporalInterceptorsImpl = (dataSource: DataSource) => TemporalInterceptorSet;
```

`client` is passed to the `Client` that starts campaign workflows; `worker` to the campaign `Worker.create`. Workflow interceptors run inside the workflow sandbox, so they are handed over as module paths (`workflowModules`), never as objects. `dataSource` is the application's, for activity interceptors that need it.

**Breaking-change policy:** changing the signature or the set of Temporal clients/workers it is applied to is a major bump.

---

## How to use as a consumer

The example below assembles a hypothetical consumer that registers all four hooks. It does not import or reference any private code; everything it needs is in this document and in the community runtime.

```ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { EvoExtensionPoints } from './src/evo-extension-points';
import { MyConsumerModule } from './my-consumer/my-consumer.module';

async function bootstrap() {
  EvoExtensionPoints.replace('capability_gate', (name, context) => {
    return MyConsumer.isEnabled(name, context);
  });

  EvoExtensionPoints.replace('runtime_context', async (req, defaultContext) => {
    if (!defaultContext.user_id) return defaultContext;
    return {
      ...defaultContext,
      scope_id: await MyConsumer.resolveScope(req),
      feature_flags: await MyConsumer.flagsFor(defaultContext.user_id),
    };
  });

  EvoExtensionPoints.replace('plugin_loader', () => ({
    modules: [MyConsumerModule],
  }));

  EvoExtensionPoints.replace('theme_tokens', async (scope_id) => {
    return scope_id ? MyConsumer.loadThemeTokens(scope_id) : {};
  });

  const app = await NestFactory.create(AppModule);
  await app.listen(3030);
}

bootstrap();
```

The consumer is not required to override all four. Each hook is independent and falls back to the community default when not replaced.

---

## Plugin Marketplace Readiness

The `plugin_loader` extension point is the future entry surface for a remote plugin marketplace. The community runtime ships the local-import flavor (modules are imported directly from the consumer's codebase). A future hosted marketplace must respect three constraints declared here as part of the contract:

- **Allowlist enforcement.** The operator of the installation declares which plugin identifiers are allowed to load. A plugin not in the allowlist is rejected at boot, regardless of signature.
- **Mandatory signing.** Every marketplace plugin ships with a signed manifest. The runtime verifies the signature against a public key held by the operator before loading. Unsigned or tampered plugins are refused.
- **SemVer enforcement.** A plugin declares the major version of each extension point it consumes. The runtime refuses to load a plugin that targets a major that the runtime does not support.

The runtime never auto-downloads, auto-updates, or executes remote plugins without an explicit configuration step by the operator.

---

## Cross-references

- `architecture.md` ADR13 — source of truth for extension-point policy across the Evo CRM Community family.
- Sister contracts in the same family: `evo-ai-crm-community/EXTENSION_POINTS.md`, `evo-ai-frontend-community/EXTENSION_POINTS.md`, `evo-ai-core-service-community/EXTENSION_POINTS.md`, `evo-auth-service-community/EXTENSION_POINTS.md`, `evo-ai-processor-community/EXTENSION_POINTS.md`.
- Reference no-op implementation lives under `evo-flow/src/evo-extension-points/` (delivered separately, by the implementation story that follows this contract).

---

## Versioning history

- **`1.3.0`** — Adds `outbound_headers`, `inbound_message_context` and `temporal_interceptors`: opaque transport for context that must cross the broker, Temporal and calls to the CRM. Additive minor bump — every default is a no-op.
- **`1.2.0`** — Adds `cache_key_scope`. Additive minor bump — the default returns `''`.
- **`1.1.0`** — Adds `tenant_db_context` (ADR14, story 10.1b): the DB-context seam that scopes tenant queries to an RLS-aware connection. Additive minor bump — the default is a no-op passthrough, so existing consumers are unaffected.
- **`1.0.0`** — Initial release of the contract. Declares `capability_gate`, `runtime_context`, `plugin_loader`, and `theme_tokens` as the four supported extension points.
