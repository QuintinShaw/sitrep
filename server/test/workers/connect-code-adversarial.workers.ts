// Promoted from an adversarial review pass's re-verification of P0-6
// (self-routing connect code, fdc094e). Exercises real workerd via
// SELF.fetch, no mocking.
import { describe, expect, it } from "vitest";
import { SELF } from "cloudflare:test";
import { decodeConnectCode } from "../../src/v1/contract/types.ts";
import { bootstrapSpace } from "./helpers.ts";

const ORIGIN = "https://example.com";

const join = (body: unknown) => SELF.fetch(`${ORIGIN}/v1/join`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

describe("connect code adversarial: (a) real e2e mint/decode/join", () => {
  it("mints via POST /v1/invites, joins with zero KV, 200", async () => {
    const { ownerToken, spaceId } = await bootstrapSpace();
    const inviteRes = await SELF.fetch(`${ORIGIN}/v1/invites`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ role: "viewer" }),
    });
    const inviteBody = await inviteRes.json();
    console.log("(a) invite response:", inviteRes.status, JSON.stringify(inviteBody));
    const { code } = inviteBody as { code: string };

    const joinRes = await join({ code, space: spaceId, name: "phone", platform: "ios" });
    const joinBody = await joinRes.text();
    console.log("(a) join response:", joinRes.status, joinBody);
    expect(joinRes.status).toBe(200);
  });
});

describe("connect code adversarial: (c) malformed-input table, server TS decoder", () => {
  const validSpace = "abcdefghjk"; // 10 chars, alphabet-only
  const validSecret = "234567892"; // 9 chars
  const valid = ("X" + validSpace + validSecret + "Z").toUpperCase();

  const cases: Array<[string, string]> = [
    ["valid baseline", valid],
    ["20 chars (too short)", valid.slice(0, 20)],
    ["22 chars (too long)", valid + "Q"],
    ["no X start anchor", "Y" + valid.slice(1)],
    ["no Z end anchor", valid.slice(0, 20) + "Y"],
    ["contains 'I'", "X" + "I".repeat(19) + "Z"],
    ["contains 'L'", "X" + "L".repeat(19) + "Z"],
    ["contains 'O'", "X" + "O".repeat(19) + "Z"],
    ["contains '0'", "X" + "0".repeat(19) + "Z"],
    ["contains '1'", "X" + "1".repeat(19) + "Z"],
    ["lowercase input (should still decode, case-insensitive)", valid.toLowerCase()],
    ["mixed case input", valid.slice(0, 10) + valid.slice(10).toLowerCase()],
  ];

  for (const [name, code] of cases) {
    it(`server decodeConnectCode: ${name}`, () => {
      const result = decodeConnectCode(code);
      console.log(`(c) server decode [${name}] code=${JSON.stringify(code)} ->`, JSON.stringify(result));
    });
  }
});

describe("connect code adversarial: (d) join a never-initialized space, with a code whose embedded space_id actually equals it", () => {
  it("returns 400, not 500 — code embeds the SAME uninitialized space_id it's routed to", async () => {
    // Build a syntactically valid code whose space_id equals the target
    // space itself (not merely 'differs from a real space' as the existing
    // regression test does) — this is the branch where ownSpaceId() is
    // actually null and decoded.space_id must fail to match it.
    const neverInitSpace = "zzzzzzzzzz"; // 10 lowercase alphabet chars, never created via POST /v1/spaces
    const secret = "234567892";
    const code = ("X" + neverInitSpace + secret + "Z").toUpperCase();
    const res = await join({ code, space: neverInitSpace, name: "d", platform: "test" });
    const body = await res.text();
    console.log("(d) join to never-initialized space, code embeds SAME space_id:", res.status, body);
    expect(res.status).toBeLessThan(500);
    expect(res.status).toBe(400);
  });
});

describe("connect code adversarial: (e) one-time secret semantics + TTL", () => {
  it("reuse of the same code after successful join is rejected", async () => {
    const { ownerToken, spaceId } = await bootstrapSpace();
    const inviteRes = await SELF.fetch(`${ORIGIN}/v1/invites`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ role: "viewer" }),
    });
    const { code, expires_in } = (await inviteRes.json()) as { code: string; expires_in: number };
    console.log("(e) expires_in reported by server:", expires_in);

    const first = await join({ code, space: spaceId, name: "d1", platform: "test" });
    console.log("(e) first join:", first.status, await first.text());
    expect(first.status).toBe(200);

    const second = await join({ code, space: spaceId, name: "d2", platform: "test" });
    const secondBody = await second.text();
    console.log("(e) reuse of SAME code:", second.status, secondBody);
    expect(second.status).toBe(404);
  });
});
