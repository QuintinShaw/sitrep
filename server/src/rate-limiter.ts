// Abuse-prevention rate limiter for POST /v1/spaces (pre-launch fix).
//
// SPACE_CREATION_ENABLED (env.ts) is a fail-closed, DEPLOY-LEVEL on/off
// switch — but once it's true (the shipped wrangler.jsonc default), it
// provides zero per-caller bound: any single caller could mint unbounded
// spaces/DOs. This is a small, DEDICATED, NON-AUTHORITATIVE Durable Object
// purely for counting: it holds no business state (no devices, no space
// data), so binding it alongside SpaceHub does not violate the "one
// SpaceHub is the single authority for its space" invariant this rebuild is
// built around (v1-architecture.md §0) — it isn't a SpaceHub and never
// touches one.
//
// Mechanism: one instance per rate-limit key (adapters/workers.ts keys by
// the caller's `cf-connecting-ip`, falling back to a shared "unknown"
// bucket only when that header is absent, which real Cloudflare edge
// traffic always sets). Each instance keeps a simple fixed-window counter:
// at most SPACE_CREATION_RATE_LIMIT creates per SPACE_CREATION_RATE_WINDOW_MS
// for that key, backed by one SQLite table so it survives the instance's
// own hibernation between requests. A rate-limiter DO cold start or an
// unlikely storage read/write hiccup only ever fails OPEN (allows the
// call) — this is an abuse-prevention aid, not a correctness-critical gate,
// so an occasional false-negative (a caller slips through) is an acceptable
// tradeoff against ever wrongly blocking a legitimate caller because of an
// internal error in the counter itself.
export const SPACE_CREATION_RATE_LIMIT = 5;
export const SPACE_CREATION_RATE_WINDOW_MS = 3600_000; // 1 hour

import { DurableObject } from "cloudflare:workers";
import type { WorkerEnv } from "./env.ts";

export class SpaceCreationRateLimiter extends DurableObject<WorkerEnv> {
  private migrated = false;

  private migrate(): void {
    if (this.migrated) return;
    this.migrated = true;
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS hits (ts INTEGER NOT NULL)`);
  }

  /** Returns true (and records this call) if under the bound for the
   * current window; false if the caller is over it. `limit`/`windowMs` are
   * passed in per-call (rather than hardcoded) so a test can exercise a
   * small bound deterministically without waiting a real hour — production
   * always calls with the constants above. */
  async allow(limit: number = SPACE_CREATION_RATE_LIMIT, windowMs: number = SPACE_CREATION_RATE_WINDOW_MS): Promise<boolean> {
    this.migrate();
    const now = Date.now();
    this.ctx.storage.sql.exec(`DELETE FROM hits WHERE ts < ?`, now - windowMs);
    const count = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM hits").toArray()[0].n;
    if (count >= limit) return false;
    this.ctx.storage.sql.exec(`INSERT INTO hits (ts) VALUES (?)`, now);
    return true;
  }
}
