// The two presence stamp points that feed snapshot.presence
// (v1-architecture.md §7.1 / space_meta): ingest_last_seen on any reliable
// uplink, agent_last_seen on a SOURCE's GET /v1/automations poll (the
// resident `sitrep agent` heartbeat). Both are non-folded — they never bump
// space_revision.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace } from "./helpers.ts";

const ORIGIN = "https://example.com";

async function postEvents(token: string, events: unknown[]): Promise<any> {
  const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ events }),
  });
  return res.json();
}

describe("presence stamps are source-gated and non-folded (v1-architecture.md §7.1)", () => {
  async function presence(ownerToken: string): Promise<{ ingest_last_seen?: number; agent_last_seen?: number }> {
    const res = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    return ((await res.json()) as { presence: { ingest_last_seen?: number; agent_last_seen?: number } }).presence;
  }
  async function spaceRevision(ownerToken: string): Promise<number> {
    const res = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    return ((await res.json()) as { space_revision: number }).space_revision;
  }

  it("a source POST /v1/events advances ingest_last_seen, without bumping space_revision", async () => {
    const { source, ownerToken } = await bootstrapSpace();
    const before = await presence(ownerToken);
    expect(before.ingest_last_seen).toBeUndefined(); // never uplinked yet

    // A metric.frame is non-folded, so it stamps ingest but does NOT advance
    // space_revision — the cleanest isolation of the presence stamp.
    await postEvents(source.token, [
      { type: "metric.frame", id: "m1", ts: Date.now(), body: { device_id: source.device_id, metrics: [{ metric_id: "cpu", value: "1", ts: Date.now() }] } },
    ]);
    const after = await presence(ownerToken);
    expect(after.ingest_last_seen).toBeTypeOf("number");
    expect(await spaceRevision(ownerToken)).toBe(0); // presence stamp did not fold
  });

  it("a source GET /v1/automations advances agent_last_seen (the agent heartbeat), without bumping space_revision", async () => {
    const { source, ownerToken } = await bootstrapSpace();
    expect((await presence(ownerToken)).agent_last_seen).toBeUndefined();

    const res = await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${source.token}` } });
    expect(res.status).toBe(200);

    expect((await presence(ownerToken)).agent_last_seen).toBeTypeOf("number");
    expect(await spaceRevision(ownerToken)).toBe(0);
  });

  it("a non-source (owner) GET /v1/automations does NOT advance agent_last_seen", async () => {
    const { ownerToken } = await bootstrapSpace();
    // Owner is allowed to GET /v1/automations by the §3 matrix, but is not a
    // source, so it must not stamp the agent heartbeat.
    const res = await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(res.status).toBe(200);
    expect((await presence(ownerToken)).agent_last_seen).toBeUndefined();
  });

  it("a viewer cannot reach GET /v1/automations at all (403), so it never stamps agent_last_seen", async () => {
    const { viewer, ownerToken } = await bootstrapSpace();
    const res = await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${viewer.token}` } });
    expect(res.status).toBe(403);
    expect((await presence(ownerToken)).agent_last_seen).toBeUndefined();
  });
});
