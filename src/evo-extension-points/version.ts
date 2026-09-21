export const EVO_EXTENSION_POINTS_VERSION = '1.3.0';

export const EXTENSION_POINT_VERSIONS = Object.freeze({
  capability_gate: '1.0.0',
  runtime_context: '1.0.0',
  plugin_loader: '1.0.0',
  theme_tokens: '1.0.0',
  // Added in contract 1.1.0 (ADR14, story 10.1b). Additive — no existing
  // consumer breaks; the default is a no-op passthrough.
  tenant_db_context: '1.0.0',
  // Added in contract 1.2.0. Per-request cache-key scope suffix. Additive —
  // default returns '' so cache keys are unchanged in standalone.
  cache_key_scope: '1.0.0',
  // Added in contract 1.3.0. Additive — defaults change nothing.
  outbound_headers: '1.0.0',
  inbound_message_context: '1.0.0',
  temporal_interceptors: '1.0.0',
} as const);

export type ExtensionPointName = keyof typeof EXTENSION_POINT_VERSIONS;
