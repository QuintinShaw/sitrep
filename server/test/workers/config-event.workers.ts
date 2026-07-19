// The /v3 HTTP automation control-plane companion: mints a config.event in
// the same durable transaction as the revision bump, and a retried request
// carrying the same Idempotency-Key must not mint twice (SPEC.md section
// 5.5). Also checks the automation reaches a subscribed viewer as a live
// delta, same as any other reliable event.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, connect, helloOffer, resume, subscribe } from "./helpers";

const ORIGIN = "https://example.com";

describe("/v3/automations control plane", () => {
  it("upserting twice with the same Idempotency-Key mints exactly one config.event", async () => {
    const { ownerToken } = await bootstrapSpace();
    const payload = {
      automation_id: "auto-1",
      name: "disk watch",
      executor_kind: "script",
      schedule: { every_seconds: 60 },
    };
    const headers = {
      authorization: `Bearer ${ownerToken}`,
      "content-type": "application/json",
      "idempotency-key": "req-abc",
    };
    const res1 = await SELF.fetch(`${ORIGIN}/v3/automations`, { method: "POST", headers, body: JSON.stringify(payload) });
    const body1 = (await res1.json()) as { revision: number };
    const res2 = await SELF.fetch(`${ORIGIN}/v3/automations`, { method: "POST", headers, body: JSON.stringify(payload) });
    const body2 = (await res2.json()) as { revision: number };
    expect(body2.revision).toBe(body1.revision);
  });

  it("an upsert reaches a subscribed viewer as a live delta carrying a config.event", async () => {
    const { viewer, ownerToken } = await bootstrapSpace();
    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");
    await subscribe(client);
    await resume(client, 0);

    const res = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ automation_id: "auto-2", name: "log rotate", executor_kind: "script", schedule: { every_seconds: 30 } }),
    });
    expect(res.status).toBe(200);

    const delta = await client.recv();
    expect(delta.type).toBe("delta");
    expect(delta.body.events[0].event_type).toBe("config.event");
    expect(delta.body.events[0].event.automation_id).toBe("auto-2");
    client.close();
  });

  it("rejects an upsert from a viewer-role token (only owner/admin may create)", async () => {
    const { viewer } = await bootstrapSpace();
    const res = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ name: "x", executor_kind: "script", schedule: { every_seconds: 5 } }),
    });
    expect(res.status).toBe(403);
  });
});
