// P0-1 (server half): REALTIME_ENABLED=false must actually gate /v3/realtime
// and its HTTP control-plane companion, /v3/automations, BEFORE any
// SpaceHub Durable Object is touched — otherwise a daemon with its local
// flag on keeps writing to SpaceHub while viewers read UserStore (dual
// authority). The rest of the suite runs with the test pool's
// REALTIME_ENABLED override (see vitest.config.ts) on, so this file flips
// `env.REALTIME_ENABLED` back to `false` per-test to exercise the gate.
import { env, listDurableObjectIds } from "cloudflare:test";
import { describe, expect, it, afterEach } from "vitest";
import { SELF } from "cloudflare:test";
import { bootstrapSpace, upgrade } from "./helpers";

const ORIGIN = "https://example.com";

describe("realtime kill switch", () => {
  afterEach(() => {
    // Restore the pool-wide override so later files/tests aren't affected
    // by this file's per-test flips.
    (env as unknown as { REALTIME_ENABLED: boolean }).REALTIME_ENABLED = true;
  });

  it("flag off: /v3/realtime returns 403 realtime_disabled and wakes no SpaceHub", async () => {
    const { viewer } = await bootstrapSpace();
    (env as unknown as { REALTIME_ENABLED: boolean }).REALTIME_ENABLED = false;

    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
    const res = await upgrade(viewer.token);
    expect(res.status).toBe(403);
    expect(res.webSocket).toBeNull();
    expect(await res.json()).toEqual({ error: "realtime_disabled" });
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
  });

  it("flag off: /v3/automations POST returns 403 realtime_disabled and wakes no SpaceHub", async () => {
    const { ownerToken } = await bootstrapSpace();
    (env as unknown as { REALTIME_ENABLED: boolean }).REALTIME_ENABLED = false;

    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
    const res = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ name: "disk watch", executor_kind: "script", schedule: { every_seconds: 60 } }),
    });
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ error: "realtime_disabled" });
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
  });

  it("flag off: /v3/automations PATCH and DELETE also return 403 realtime_disabled", async () => {
    const { ownerToken } = await bootstrapSpace();
    // create it first, while the flag is still on for this test.
    const createRes = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ automation_id: "auto-gate", name: "disk watch", executor_kind: "script", schedule: { every_seconds: 60 } }),
    });
    expect(createRes.status).toBe(200);

    (env as unknown as { REALTIME_ENABLED: boolean }).REALTIME_ENABLED = false;

    const patchRes = await SELF.fetch(`${ORIGIN}/v3/automations/auto-gate`, {
      method: "PATCH",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ state: "paused" }),
    });
    expect(patchRes.status).toBe(403);
    expect(await patchRes.json()).toEqual({ error: "realtime_disabled" });

    const deleteRes = await SELF.fetch(`${ORIGIN}/v3/automations/auto-gate`, {
      method: "DELETE",
      headers: { authorization: `Bearer ${ownerToken}` },
    });
    expect(deleteRes.status).toBe(403);
    expect(await deleteRes.json()).toEqual({ error: "realtime_disabled" });
  });

  it("flag on: /v3/realtime upgrade still works (explicit regression check)", async () => {
    const { source } = await bootstrapSpace();
    const res = await upgrade(source.token);
    expect(res.status).toBe(101);
    expect(res.webSocket).not.toBeNull();
    res.webSocket?.accept();
    res.webSocket?.close();
  });

  it("flag off: an unauthenticated request still gets 401, not 403 (no flag-state leak)", async () => {
    (env as unknown as { REALTIME_ENABLED: boolean }).REALTIME_ENABLED = false;

    const res = await upgrade(null);
    expect(res.status).toBe(401);
    const badToken = await upgrade("not-a-real-token");
    expect(badToken.status).toBe(401);
  });
});
