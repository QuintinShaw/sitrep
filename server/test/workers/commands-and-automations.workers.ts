// Reverse control + automations control plane (v1-architecture.md §4.4,
// §5, §5.1, §6): command idempotency, automations idempotent create/409
// conflict, run relays run_now, bulk vs single message delete.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, connect, helloSource } from "./helpers.ts";

const ORIGIN = "https://example.com";

describe("POST /v1/tasks/:id/commands", () => {
  it("relays live to the owning source, and a retried Idempotency-Key does not enqueue twice", async () => {
    const { source, viewer } = await bootstrapSpace();
    // Establish the task's owning device (P0-3): the source starts build-1.
    await SELF.fetch(`${ORIGIN}/v1/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({ events: [{ type: "task.event", id: "s1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, task_id: "build-1", kind: "started", occurred_at: Date.now() } }] }),
    });
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);

    const send = () =>
      SELF.fetch(`${ORIGIN}/v1/tasks/build-1/commands`, {
        method: "POST",
        headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json", "idempotency-key": "pause-tap-1" },
        body: JSON.stringify({ action: "pause" }),
      });

    const first = await send();
    expect(first.status).toBe(200);
    const relayed = await sourceClient.recv();
    expect(relayed.type).toBe("command");
    expect(relayed.body.action).toBe("pause");
    expect(relayed.body.task_id).toBe("build-1");
    expect(relayed.body.origin).toBe("viewer");
    expect(relayed.body.issued_by_device_id).toBe(viewer.device_id);

    const second = await send();
    expect(second.status).toBe(200);
    // No second relay for the retried idempotency key: the source socket
    // should stay silent.
    await expect(sourceClient.expectSilence(150)).resolves.toBe(true);
    sourceClient.close();
  });

  it("400s on an invalid action or out-of-range ttl_ms", async () => {
    const { viewer } = await bootstrapSpace();
    const badAction = await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/commands`, {
      method: "POST",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ action: "cancel" }),
    });
    expect(badAction.status).toBe(400);

    const badTtl = await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/commands`, {
      method: "POST",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ action: "pause", ttl_ms: 0 }),
    });
    expect(badTtl.status).toBe(400);
  });
});

describe("automations control plane", () => {
  it("POST creates, PATCH/DELETE/run are open to viewer, POST create is owner-only", async () => {
    const { ownerToken, viewer } = await bootstrapSpace();

    const forbidden = await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ name: "n", executor_kind: "script", schedule: { every_seconds: 60 } }),
    });
    expect(forbidden.status).toBe(403);

    const created = await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json", "idempotency-key": "create-1" },
      body: JSON.stringify({ automation_id: "nightly", name: "Nightly", executor_kind: "script", schedule: { every_seconds: 86400 } }),
    });
    expect(created.status).toBe(200);
    const createdBody = (await created.json()) as { revision: number; automation: { automation_id: string } };
    expect(createdBody.automation.automation_id).toBe("nightly");

    // Idempotent replay: same key + same body -> identical revision.
    const replay = await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json", "idempotency-key": "create-1" },
      body: JSON.stringify({ automation_id: "nightly", name: "Nightly", executor_kind: "script", schedule: { every_seconds: 86400 } }),
    });
    const replayBody = (await replay.json()) as { revision: number };
    expect(replayBody.revision).toBe(createdBody.revision);

    // Same key, different fingerprint -> 409, mints nothing.
    const conflict = await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json", "idempotency-key": "create-1" },
      body: JSON.stringify({ automation_id: "nightly", name: "Nightly (renamed)", executor_kind: "script", schedule: { every_seconds: 43200 } }),
    });
    expect(conflict.status).toBe(409);

    // viewer may PATCH/DELETE/run an existing automation.
    const patched = await SELF.fetch(`${ORIGIN}/v1/automations/nightly`, {
      method: "PATCH",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ state: "paused" }),
    });
    expect(patched.status).toBe(200);
    const patchedBody = (await patched.json()) as { automation: { state: string } };
    expect(patchedBody.automation.state).toBe("paused");

    const patchMissing = await SELF.fetch(`${ORIGIN}/v1/automations/does-not-exist`, {
      method: "PATCH",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ state: "active" }),
    });
    expect(patchMissing.status).toBe(404);

    const deleted = await SELF.fetch(`${ORIGIN}/v1/automations/nightly`, { method: "DELETE", headers: { authorization: `Bearer ${viewer.token}` } });
    expect(deleted.status).toBe(200);
  });

  it("GET /v1/automations is source+owner, excludes viewer (v1-architecture.md §3)", async () => {
    const { source, viewer, ownerToken } = await bootstrapSpace();
    expect((await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${source.token}` } })).status).toBe(200);
    expect((await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${ownerToken}` } })).status).toBe(200);
    expect((await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${viewer.token}` } })).status).toBe(403);
  });

  it("POST /v1/automations/:id/run stamps run_requested_at, mints no command/config.event, and does NOT advance space_revision (§5.1)", async () => {
    const { ownerToken, viewer } = await bootstrapSpace();
    await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ automation_id: "nightly", name: "Nightly", executor_kind: "script", schedule: { every_seconds: 86400 } }),
    });

    const before = (await (await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as {
      space_revision: number;
      automations: Array<{ automation_id: string; run_requested_at?: number }>;
    };
    expect(before.automations[0].run_requested_at).toBeUndefined(); // never requested yet

    // 404 before writing on an unknown id (same discipline as PATCH/DELETE).
    const missing = await SELF.fetch(`${ORIGIN}/v1/automations/does-not-exist/run`, { method: "POST", headers: { authorization: `Bearer ${viewer.token}` } });
    expect(missing.status).toBe(404);

    // Works with NO source/agent connected (the whole point of the poll-field
    // fix): viewer stamps run_requested_at, gets 200 with no body.
    const runRes = await SELF.fetch(`${ORIGIN}/v1/automations/nightly/run`, { method: "POST", headers: { authorization: `Bearer ${viewer.token}` } });
    expect(runRes.status).toBe(200);
    expect((await runRes.text()).length).toBe(0); // 200 with no body

    const after = (await (await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as {
      space_revision: number;
      automations: Array<{ automation_id: string; run_requested_at?: number }>;
    };
    // run_requested_at is now stamped, and the revision did NOT advance
    // (no config.event minted).
    expect(after.automations[0].run_requested_at).toBeTypeOf("number");
    expect(after.space_revision).toBe(before.space_revision);

    // GET /v1/automations (source/owner) also surfaces run_requested_at.
    const listed = (await (await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as Array<{ automation_id: string; run_requested_at?: number }>;
    expect(listed.find((a) => a.automation_id === "nightly")?.run_requested_at).toBeTypeOf("number");

    // A retried run re-stamps (monotonic, idempotent) — still no revision bump.
    const run2 = await SELF.fetch(`${ORIGIN}/v1/automations/nightly/run`, { method: "POST", headers: { authorization: `Bearer ${viewer.token}` } });
    expect(run2.status).toBe(200);
    const after2 = (await (await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as { space_revision: number };
    expect(after2.space_revision).toBe(before.space_revision);
  });

  it("run_requested_at survives a PATCH (an edit must not clear a pending run)", async () => {
    const { ownerToken, viewer } = await bootstrapSpace();
    await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ automation_id: "nightly", name: "Nightly", executor_kind: "script", schedule: { every_seconds: 86400 } }),
    });
    await SELF.fetch(`${ORIGIN}/v1/automations/nightly/run`, { method: "POST", headers: { authorization: `Bearer ${viewer.token}` } });

    await SELF.fetch(`${ORIGIN}/v1/automations/nightly`, {
      method: "PATCH",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ state: "paused" }),
    });

    const listed = (await (await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as Array<{ automation_id: string; state: string; run_requested_at?: number }>;
    const a = listed.find((x) => x.automation_id === "nightly")!;
    expect(a.state).toBe("paused");
    expect(a.run_requested_at).toBeTypeOf("number"); // preserved across the edit
  });
});

describe("message delete: bulk vs single (v1-architecture.md §2.3)", () => {
  async function seedMessages(sourceToken: string, deviceId: string, ids: string[]) {
    await SELF.fetch(`${ORIGIN}/v1/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${sourceToken}`, "content-type": "application/json" },
      body: JSON.stringify({
        events: ids.map((id, i) => ({
          type: "message.event",
          id: `env-${id}`,
          ts: Date.now(),
          body: { device_id: deviceId, device_seq: i + 1, message_id: id, level: "info", text: id, occurred_at: Date.now() },
        })),
      }),
    });
  }

  it("DELETE /v1/messages/:id removes exactly one; DELETE /v1/messages clears all", async () => {
    const { source, ownerToken } = await bootstrapSpace();
    await seedMessages(source.token, source.device_id, ["m1", "m2", "m3"]);

    const oneDeleted = await SELF.fetch(`${ORIGIN}/v1/messages/m2`, { method: "DELETE", headers: { authorization: `Bearer ${ownerToken}` } });
    expect(oneDeleted.status).toBe(200);

    const afterOne = (await (await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as { messages: Array<{ message_id: string }> };
    expect(afterOne.messages.map((m) => m.message_id).sort()).toEqual(["m1", "m3"]);

    // Idempotent: deleting an already-gone id is still 200.
    const again = await SELF.fetch(`${ORIGIN}/v1/messages/m2`, { method: "DELETE", headers: { authorization: `Bearer ${ownerToken}` } });
    expect(again.status).toBe(200);

    const clearAll = await SELF.fetch(`${ORIGIN}/v1/messages`, { method: "DELETE", headers: { authorization: `Bearer ${ownerToken}` } });
    expect(clearAll.status).toBe(200);

    const afterAll = (await (await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as { messages: unknown[] };
    expect(afterAll.messages).toHaveLength(0);
  });
});
