// Cloudflare Workers entry for the v1 rebuild. ONE Durable Object class
// (`SpaceHub`) is bound; `UserStore` is gone entirely (v1-architecture.md
// §0/§11) — see wrangler.jsonc for the migration tombstoning it.
import { createApp } from "../app.ts";
import type { WorkerEnv } from "../env.ts";
import { SPACE_CREATION_RATE_LIMIT, SpaceCreationRateLimiter } from "../rate-limiter.ts";
import { SpaceHub } from "../realtime/space-hub.ts";
import { parseTransportFlag } from "../v1/contract/types.ts";

// Re-exported so wrangler (which binds Durable Object classes by name from
// this entry module, see wrangler.jsonc) can resolve SpaceHub and the
// (pre-launch) space-creation rate limiter.
export { SpaceHub, SpaceCreationRateLimiter };

function spaceHubStub(env: WorkerEnv, space: string) {
  return env.SPACE_HUB.getByName(space);
}

const app = createApp({
  spaceHub: (c, space) => spaceHubStub(c.env as WorkerEnv, space),
  // Space-creation gate (5e): a hard deployment-level on/off, enforced BEFORE
  // any DO is materialized. Default-safe — parseTransportFlag treats
  // absent/unparseable as false, so a misconfigured deploy cannot mint
  // unbounded DOs/storage.
  allowSpaceCreation: (c) => parseTransportFlag((c.env as WorkerEnv).SPACE_CREATION_ENABLED),
  // Pre-launch: a real bounded rate limit on top of the flag above, since
  // SPACE_CREATION_ENABLED=true (the shipped default) by itself allows
  // unlimited creates. Mechanism: a tiny dedicated, non-authoritative
  // Durable Object purely for counting (rate-limiter.ts) — one instance per
  // caller IP (env.SPACE_CREATION_RATE_LIMITER.getByName(ip)), a fixed-
  // window counter bounded at SPACE_CREATION_RATE_LIMIT_PER_HOUR (default 5)
  // creates per IP per hour. `cf-connecting-ip` is set by the Cloudflare
  // edge on all real internet traffic and cannot be spoofed by the sender;
  // it falls back to a shared "unknown" bucket only for traffic that never
  // passed through that edge (e.g. this repo's own test harness).
  checkSpaceCreationRateLimit: async (c) => {
    const env = c.env as WorkerEnv;
    const ip = c.req.header("cf-connecting-ip") ?? "unknown";
    const limit = Number(env.SPACE_CREATION_RATE_LIMIT_PER_HOUR) || SPACE_CREATION_RATE_LIMIT;
    return env.SPACE_CREATION_RATE_LIMITER.getByName(ip).allow(limit);
  },
  wsTransportEnabled: (c) => parseTransportFlag((c.env as WorkerEnv).WS_TRANSPORT_ENABLED),
  apnsDeliveryEnabled: (c) => parseTransportFlag((c.env as WorkerEnv).APNS_DELIVERY_ENABLED),
});

export default app;
