// Pre-launch: a real bounded rate limit on top of SPACE_CREATION_ENABLED
// (rate-limiter.ts's SpaceCreationRateLimiter DO, wired in
// adapters/workers.ts). SPACE_CREATION_ENABLED alone is a deploy-level
// on/off with no per-caller bound once true (the shipped default) — this
// suite exercises the actual bound.
import { env, SELF } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";

const ORIGIN = "https://example.com";

async function createSpace(ip: string): Promise<Response> {
  return SELF.fetch(`${ORIGIN}/v1/spaces`, {
    method: "POST",
    headers: { "content-type": "application/json", "cf-connecting-ip": ip },
    body: JSON.stringify({ platform: "test", name: "mac" }),
  });
}

describe("POST /v1/spaces: bounded per-IP rate limit (pre-launch)", () => {
  afterEach(() => {
    // Restore the pool-wide effectively-unlimited override (vitest.config.ts)
    // so this test doesn't leak a tiny bound into the rest of the suite.
    env.SPACE_CREATION_RATE_LIMIT_PER_HOUR = 100_000;
  });

  it("exceeding the bound is rejected (429); staying under it succeeds; the bound is per-IP, not global", async () => {
    env.SPACE_CREATION_RATE_LIMIT_PER_HOUR = 3;
    // A dedicated IP for this test, isolated from any other test's bucket
    // (the wide suite's own bootstrapSpace() calls send no cf-connecting-ip
    // at all, so they share a separate "unknown" bucket untouched by this).
    const ip = "203.0.113.9";

    for (let i = 0; i < 3; i++) {
      const res = await createSpace(ip);
      expect(res.status).toBe(200);
    }
    const over = await createSpace(ip);
    expect(over.status).toBe(429);
    expect(await over.json()).toEqual({ error: "too many space creations, try again later" });

    // A DIFFERENT IP has its own, unaffected bucket (per-IP, not global).
    const otherIp = await createSpace("203.0.113.10");
    expect(otherIp.status).toBe(200);
  });

  it("SPACE_CREATION_ENABLED=false still 403s before the rate limiter is ever consulted", async () => {
    env.SPACE_CREATION_ENABLED = false;
    env.SPACE_CREATION_RATE_LIMIT_PER_HOUR = 3;
    const res = await createSpace("203.0.113.11");
    expect(res.status).toBe(403);
    env.SPACE_CREATION_ENABLED = true;
  });

  // Promoted from an adversarial review pass's
  // `adversarial-rate-limit-live.workers.ts`.
  it("no cf-connecting-ip header falls into a SHARED 'unknown' bucket (fail toward shared-limit, not unlimited fail-open, not outright rejected) — adapters/workers.ts's `c.req.header(\"cf-connecting-ip\") ?? \"unknown\"`", async () => {
    env.SPACE_CREATION_RATE_LIMIT_PER_HOUR = 3;
    const createNoHeader = () => SELF.fetch(`${ORIGIN}/v1/spaces`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ platform: "test", name: "mac" }) });

    const statuses: number[] = [];
    for (let i = 0; i < 4; i++) {
      const res = await createNoHeader(); // no header at all, simulating local dev
      statuses.push(res.status);
    }
    // First 3 succeed, 4th is blocked -- proves it's a REAL bounded shared
    // bucket (fail-closed-ish: bounded, not unlimited), not fail-open.
    expect(statuses).toEqual([200, 200, 200, 429]);

    // Any OTHER caller that also omits the header (e.g. a second local-dev
    // client, or literally any request behind a misconfigured/non-CF proxy
    // that strips cf-connecting-ip) shares that SAME bucket and gets
    // wrongly blocked/counted against a stranger's usage -- a real
    // multi-tenant fairness gap for the fallback case, though not a
    // security hole (bounded, not unlimited).
    const anotherNoHeaderCaller = await createNoHeader();
    expect(anotherNoHeaderCaller.status).toBe(429);
  });
});
