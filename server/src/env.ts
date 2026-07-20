// Cloudflare Workers environment shape shared by SpaceHub (the DO) and the
// Worker entry (adapters/workers.ts). A dedicated module avoids a type-only
// import cycle between the two (workers.ts exports the SpaceHub class that
// space-hub.ts defines; space-hub.ts needs the env shape workers.ts owns).

export interface Secrets {
  APNS_KEY_P8?: string;
  APNS_KEY_ID?: string;
  APNS_TEAM_ID?: string;
  APNS_BUNDLE_ID?: string; // var, wrangler.jsonc
  APNS_HOST?: string; // var, wrangler.jsonc
  // vars, wrangler.jsonc; typed string|boolean because a Cloudflare dashboard
  // variable override always arrives as a STRING at runtime even though
  // wrangler.jsonc declares them boolean — see parseTransportFlag in
  // v1/contract/types.ts, the one shared strict parser for both switches
  // (v1-architecture.md §8.3).
  WS_TRANSPORT_ENABLED?: string | boolean;
  APNS_DELIVERY_ENABLED?: string | boolean;
  // Deployment-level gate on public space creation (5e). Default-safe: the
  // strict parser treats absent/unparseable as FALSE, so a misconfigured
  // deploy fails CLOSED (no unbounded DO/storage minting). wrangler.jsonc
  // sets it true for the shipped config; flip off to freeze creation.
  SPACE_CREATION_ENABLED?: string | boolean;
  // Pre-launch: bounded per-IP rate limit on top of SPACE_CREATION_ENABLED
  // (rate-limiter.ts's SpaceCreationRateLimiter DO). wrangler.jsonc ships a
  // real production bound (5/hour); the test pool overrides this to an
  // effectively-unlimited value so the wide test suite's many bootstrapSpace()
  // calls (which all share one bucket — the test harness sends no
  // cf-connecting-ip) never trip it — one dedicated test overrides it back
  // down to exercise the 429 path.
  SPACE_CREATION_RATE_LIMIT_PER_HOUR?: string | number;
}

export type WorkerEnv = Env & Secrets;
