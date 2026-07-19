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

  it("reusing an Idempotency-Key for a DIFFERENT operation returns 409 and mints nothing", async () => {
    const { ownerToken } = await bootstrapSpace();
    const headers = {
      authorization: `Bearer ${ownerToken}`,
      "content-type": "application/json",
      "idempotency-key": "req-reused",
    };
    const res1 = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers,
      body: JSON.stringify({ automation_id: "auto-x", name: "first op", executor_kind: "script", schedule: { every_seconds: 60 } }),
    });
    expect(res1.status).toBe(200);
    const body1 = (await res1.json()) as { revision: number };

    // Same key, different content — must be rejected, NOT silently
    // replayed as the first operation's result.
    const res2 = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers,
      body: JSON.stringify({ automation_id: "auto-y", name: "second op", executor_kind: "script", schedule: { every_seconds: 5 } }),
    });
    expect(res2.status).toBe(409);

    // Nothing was minted for the conflicting request: a fresh operation
    // lands at exactly body1.revision + 1.
    const res3 = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ automation_id: "auto-z", name: "third op", executor_kind: "script", schedule: { every_seconds: 30 } }),
    });
    const body3 = (await res3.json()) as { revision: number };
    expect(body3.revision).toBe(body1.revision + 1);
  });

  it("retrying a create WITHOUT an explicit automation_id under the same key replays the same automation, not 409", async () => {
    const { ownerToken } = await bootstrapSpace();
    const headers = {
      authorization: `Bearer ${ownerToken}`,
      "content-type": "application/json",
      "idempotency-key": "req-serverid",
    };
    // No automation_id in the body: the server picks one — deterministically
    // from the Idempotency-Key, so a retry of a dropped response must
    // replay the original result (same automation_id, same revision).
    const payload = JSON.stringify({ name: "no explicit id", executor_kind: "script", schedule: { every_seconds: 60 } });
    const res1 = await SELF.fetch(`${ORIGIN}/v3/automations`, { method: "POST", headers, body: payload });
    expect(res1.status).toBe(200);
    const body1 = (await res1.json()) as { revision: number; automation: { automation_id: string } };
    expect(body1.automation.automation_id.length).toBeGreaterThan(0);

    const res2 = await SELF.fetch(`${ORIGIN}/v3/automations`, { method: "POST", headers, body: payload });
    expect(res2.status).toBe(200);
    const body2 = (await res2.json()) as { revision: number; automation: { automation_id: string } };
    expect(body2.revision).toBe(body1.revision);
    expect(body2.automation.automation_id).toBe(body1.automation.automation_id);

    // Same key with DIFFERENT client content still conflicts.
    const res3 = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers,
      body: JSON.stringify({ name: "changed content", executor_kind: "script", schedule: { every_seconds: 5 } }),
    });
    expect(res3.status).toBe(409);
  });

  it("role matrix: viewer may PATCH and DELETE but not POST", async () => {
    const { viewer, ownerToken } = await bootstrapSpace();
    // Owner creates two automations for the viewer to act on.
    for (const id of ["auto-p", "auto-d"]) {
      const res = await SELF.fetch(`${ORIGIN}/v3/automations`, {
        method: "POST",
        headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
        body: JSON.stringify({ automation_id: id, name: id, executor_kind: "script", schedule: { every_seconds: 60 } }),
      });
      expect(res.status).toBe(200);
    }
    const viewerHeaders = { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" };

    // Viewer PATCH (pause + reschedule) succeeds — intentional design:
    // watcher schedules are editable from every device.
    const patchRes = await SELF.fetch(`${ORIGIN}/v3/automations/auto-p`, {
      method: "PATCH",
      headers: viewerHeaders,
      body: JSON.stringify({ state: "paused", schedule: { every_seconds: 120 } }),
    });
    expect(patchRes.status).toBe(200);
    const patched = (await patchRes.json()) as { automation: { state: string; schedule: { every_seconds: number } } };
    expect(patched.automation.state).toBe("paused");
    expect(patched.automation.schedule.every_seconds).toBe(120);

    // Viewer DELETE succeeds (aligned with /v2 canView's delete).
    const deleteRes = await SELF.fetch(`${ORIGIN}/v3/automations/auto-d`, { method: "DELETE", headers: viewerHeaders });
    expect(deleteRes.status).toBe(200);

    // Viewer POST (creation) stays forbidden — creation is trusted-device
    // only.
    const postRes = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers: viewerHeaders,
      body: JSON.stringify({ name: "nope", executor_kind: "script", schedule: { every_seconds: 5 } }),
    });
    expect(postRes.status).toBe(403);
  });

  it("DELETE of a nonexistent automation returns 404 without burning a revision", async () => {
    const { ownerToken } = await bootstrapSpace();
    const headers = { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" };
    const res1 = await SELF.fetch(`${ORIGIN}/v3/automations`, {
      method: "POST",
      headers,
      body: JSON.stringify({ automation_id: "auto-real", name: "real", executor_kind: "script", schedule: { every_seconds: 60 } }),
    });
    const body1 = (await res1.json()) as { revision: number };

    const missingRes = await SELF.fetch(`${ORIGIN}/v3/automations/no-such-automation`, { method: "DELETE", headers });
    expect(missingRes.status).toBe(404);

    // The 404 minted no config.event: the next real operation lands at
    // exactly body1.revision + 1.
    const res2 = await SELF.fetch(`${ORIGIN}/v3/automations/auto-real`, { method: "DELETE", headers });
    const body2 = (await res2.json()) as { revision: number };
    expect(body2.revision).toBe(body1.revision + 1);
  });
});
