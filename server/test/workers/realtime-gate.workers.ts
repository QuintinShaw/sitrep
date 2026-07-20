// WS_TRANSPORT_ENABLED kill switch (v1-architecture.md §8.1): pure
// transport switch, checked ONLY by GET /v1/realtime. 503, never 403.
// Every other /v1 route (including POST /v1/events and POST /v1/automations)
// must behave identically regardless of this flag.
import { env, SELF } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { bootstrapSpace } from "./helpers.ts";

const ORIGIN = "https://example.com";

describe("WS_TRANSPORT_ENABLED kill switch", () => {
  afterEach(() => {
    env.WS_TRANSPORT_ENABLED = true; // restore the test-pool default (vitest.config.ts)
  });

  it("GET /v1/realtime returns 503 (not 403) when the flag is off", async () => {
    const { viewer } = await bootstrapSpace();
    env.WS_TRANSPORT_ENABLED = false;

    const res = await SELF.fetch(`${ORIGIN}/v1/realtime`, {
      headers: { authorization: `Bearer ${viewer.token}`, upgrade: "websocket" },
    });
    expect(res.status).toBe(503);
    expect(await res.json()).toEqual({ error: "transport_unavailable" });
  });

  it("GET /v1/snapshot reflects capabilities.ws_transport_enabled but still fully serves state either way", async () => {
    const { source, ownerToken } = await bootstrapSpace();

    await SELF.fetch(`${ORIGIN}/v1/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({
        events: [{ type: "task.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, task_id: "t1", kind: "started", occurred_at: Date.now() } }],
      }),
    });

    env.WS_TRANSPORT_ENABLED = false;
    const off = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    const offBody = (await off.json()) as { capabilities: { ws_transport_enabled: boolean }; tasks: unknown[]; presence: { sources_online: number } };
    expect(offBody.capabilities.ws_transport_enabled).toBe(false);
    expect(offBody.tasks).toHaveLength(1);
    expect(offBody.presence.sources_online).toBe(0); // no WS can be open when the transport is off

    env.WS_TRANSPORT_ENABLED = true;
    const on = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    const onBody = (await on.json()) as { capabilities: { ws_transport_enabled: boolean } };
    expect(onBody.capabilities.ws_transport_enabled).toBe(true);
  });

  it("POST /v1/events and POST /v1/automations are unaffected by the flag being off", async () => {
    const { source, ownerToken } = await bootstrapSpace();
    env.WS_TRANSPORT_ENABLED = false;

    const eventsRes = await SELF.fetch(`${ORIGIN}/v1/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({
        events: [{ type: "task.event", id: "e2", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, task_id: "t2", kind: "started", occurred_at: Date.now() } }],
      }),
    });
    expect(eventsRes.status).toBe(200);
    const eventsBody = (await eventsRes.json()) as { results: Array<{ status: string }> };
    expect(eventsBody.results[0].status).toBe("applied");

    const automationRes = await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ name: "n", executor_kind: "script", schedule: { every_seconds: 60 } }),
    });
    expect(automationRes.status).toBe(200);
  });
});
