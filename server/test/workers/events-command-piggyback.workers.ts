// POST /v1/events reverse-control piggyback (v1-architecture.md §4.1): the
// HTTP-transport equivalent of WS `command` frames, for an HTTP-only
// `sitrep run` source that has no WS to receive commands on.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, connect, helloSource, pendingCommandRows, spaceHubStub } from "./helpers.ts";

const ORIGIN = "https://example.com";

async function postEvents(token: string, events: unknown[], forTaskId?: string, ackCommandIds?: string[]): Promise<any> {
  const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ events, ...(forTaskId ? { for_task_id: forTaskId } : {}), ...(ackCommandIds ? { ack_command_ids: ackCommandIds } : {}) }),
  });
  return res.json();
}

async function queueCommand(viewerToken: string, taskId: string, action: string, idempotencyKey: string): Promise<Response> {
  return SELF.fetch(`${ORIGIN}/v1/tasks/${taskId}/commands`, {
    method: "POST",
    headers: { authorization: `Bearer ${viewerToken}`, "content-type": "application/json", "idempotency-key": idempotencyKey },
    body: JSON.stringify({ action }),
  });
}

let seqCounter = 0;

/** Establishes a task's owning device (P0-3): a source posts a `started`
 * task.event so a directed reverse-control command has a device to target.
 * A command for a task with no owning device is 404 "task not running". */
async function startTask(sourceToken: string, deviceId: string, taskId: string): Promise<any> {
  const seq = ++seqCounter;
  return postEvents(sourceToken, [{ type: "task.event", id: `start-${seq}`, ts: Date.now(), body: { device_id: deviceId, device_seq: seq, task_id: taskId, kind: "started", occurred_at: Date.now() } }]);
}

const taskEnvelope = (deviceId: string, deviceSeq: number, taskId = "build-1") => ({
  type: "task.event",
  id: `e${deviceSeq}`,
  ts: Date.now(),
  body: { device_id: deviceId, device_seq: deviceSeq, task_id: taskId, kind: "started", occurred_at: Date.now() },
});

describe("POST /v1/events — reverse-control command piggyback (v1-architecture.md §4.1)", () => {
  it("a pending command (directed to the task's owning device) is piggybacked on the next POST /v1/events", async () => {
    const { source, viewer } = await bootstrapSpace();
    // The source owns the task (started it); no source WS is connected, so the
    // directed command persists as a pending_commands row for that device.
    await startTask(source.token, source.device_id, "build-1");
    expect((await queueCommand(viewer.token, "build-1", "pause", "cmd-1")).status).toBe(200);

    const body = await postEvents(source.token, []);
    expect(body.commands).toBeDefined();
    expect(body.commands).toHaveLength(1);
    const cmd = body.commands[0];
    expect(cmd).toMatchObject({ origin: "viewer", action: "pause", task_id: "build-1", ttl_ms: 60000 });
    expect(typeof cmd.command_id).toBe("string");
    expect(typeof cmd.origin_ts).toBe("number");
  });

  it("a command for a task with no owning device is 404 'task not running' (P0-3)", async () => {
    const { viewer } = await bootstrapSpace();
    const res = await queueCommand(viewer.token, "never-started", "pause", "cmd-x");
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "task not running" });
  });

  it("an empty events:[] heartbeat POST still drains a pending command", async () => {
    const { source, viewer } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "stop", "cmd-hb");

    const body = await postEvents(source.token, []);
    expect(body.acked).toEqual([]);
    expect(body.results).toEqual([]);
    expect(body.commands).toHaveLength(1);
    expect(body.commands[0].action).toBe("stop");
  });

  it("P0-5 fetch-then-ack: a second POST WITHOUT an ack STILL redelivers the same command (the reviewer's live-repro fix)", async () => {
    const { source, viewer } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "pause", "cmd-once");

    const first = await postEvents(source.token, []);
    expect(first.commands).toHaveLength(1);
    expect(first.commands[0].command_id).toBe("cmd-once");

    // Simulates the exact bug an external review live-reproduced against
    // real workerd: the first response never reached (or was never acted
    // on by) the client — no ack_command_ids sent. A second, independent
    // poll from the SAME device must see the identical command again,
    // byte-for-byte — inclusion in a response never by itself marks a row
    // delivered (v1-architecture.md §1.4, §4.1).
    const second = await postEvents(source.token, []);
    expect(second.commands).toHaveLength(1);
    expect(second.commands[0]).toEqual(first.commands[0]);

    // A THIRD poll still redelivers it — this is intentional at-least-once
    // redelivery, not a bug, safe because pause/resume/stop are idempotent.
    const third = await postEvents(source.token, []);
    expect(third.commands).toHaveLength(1);
    expect(third.commands[0]).toEqual(first.commands[0]);

    // Only once the device explicitly acks (having durably handed the pause
    // off to its local process controller) does it stop reappearing.
    const acked = await postEvents(source.token, [], undefined, ["cmd-once"]);
    expect(acked.commands ?? []).toHaveLength(0);
    const after = await postEvents(source.token, []);
    expect(after.commands ?? []).toHaveLength(0);
  });

  it("ack_command_ids is processed BEFORE this same request's commands[] is computed — acking and polling in one call never returns what was just acked", async () => {
    const { source, viewer } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "pause", "cmd-ack-same-req");

    const first = await postEvents(source.token, []);
    expect(first.commands).toHaveLength(1);

    // Ack AND poll in the same request.
    const combined = await postEvents(source.token, [], undefined, ["cmd-ack-same-req"]);
    expect(combined.commands ?? []).toHaveLength(0);
  });

  it("an ack for an unknown/foreign/already-acked command_id is silently ignored (idempotent no-op, not an error)", async () => {
    const { source, viewer } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "pause", "cmd-real");

    // Ack a made-up id plus the real one in the same request — no error, and
    // the real one is retired.
    const res = await postEvents(source.token, [], undefined, ["does-not-exist", "cmd-real"]);
    expect(res.commands ?? []).toHaveLength(0);

    // Re-acking the same (already-acked) id again is still a harmless no-op.
    const again = await postEvents(source.token, [], undefined, ["cmd-real"]);
    expect(again.commands ?? []).toHaveLength(0);
  });

  it("an ack for a command_id owned by a DIFFERENT device does not retire it — that device still sees it", async () => {
    const { source, viewer, inviteAndJoin } = await bootstrapSpace();
    const source2 = await inviteAndJoin("source");
    await startTask(source.token, source.device_id, "build-1"); // owned by `source`, not source2
    await queueCommand(viewer.token, "build-1", "pause", "cmd-wrong-device");

    // source2 (not the owning device) tries to ack it — ignored, no effect.
    await postEvents(source2.token, [], undefined, ["cmd-wrong-device"]);

    // The real owning device still sees it, undelivered.
    const body = await postEvents(source.token, []);
    expect(body.commands).toHaveLength(1);
    expect(body.commands[0].command_id).toBe("cmd-wrong-device");
  });

  it("an expired command (past its 60s TTL) is not delivered", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "resume", "cmd-expired");

    // Backdate the pending_commands row's origin_ts so now > origin_ts+ttl_ms.
    const { runInDurableObject, env } = await import("cloudflare:test");
    await runInDurableObject(env.SPACE_HUB.getByName(spaceId), (_i, state) => {
      state.storage.sql.exec("UPDATE pending_commands SET origin_ts = ? WHERE command_id = 'cmd-expired'", Date.now() - 120_000);
    });

    const body = await postEvents(source.token, []);
    expect(body.commands ?? []).toHaveLength(0);
  });

  it("the WS push is a best-effort hint that does NOT consume; the HTTP drain still delivers it (MAJOR fix)", async () => {
    const { source, viewer } = await bootstrapSpace();
    // The source owns build-1 and holds a WS on the owning device.
    await startTask(source.token, source.device_id, "build-1");
    const wsSource = await connect(source.token);
    await helloSource(wsSource, source.device_id);

    // Queue -> relayOrQueueCommand pushes LIVE to the owning device's WS as a
    // best-effort hint, but does NOT mark delivered.
    await queueCommand(viewer.token, "build-1", "pause", "cmd-onetransport");
    const relayed = await wsSource.recv();
    expect(relayed.type).toBe("command");
    expect(relayed.body.action).toBe("pause");
    expect(relayed.body.command_id).toBe("cmd-onetransport");

    // Because the WS push did NOT consume it (and P0-5: inclusion never
    // consumes either), the HTTP for_task_id drain (the executor's poll)
    // STILL returns it. The command reaches the executor, not lost.
    const body = await postEvents(source.token, [], "build-1");
    expect(body.commands).toHaveLength(1);
    expect(body.commands[0].command_id).toBe("cmd-onetransport");

    // A second HTTP drain WITHOUT an ack still redelivers it (P0-5,
    // fetch-then-ack) — only an explicit ack retires it.
    const second = await postEvents(source.token, [], "build-1");
    expect(second.commands).toHaveLength(1);
    expect(second.commands[0].command_id).toBe("cmd-onetransport");

    // Acking stops it.
    const third = await postEvents(source.token, [], "build-1", ["cmd-onetransport"]);
    expect(third.commands ?? []).toHaveLength(0);
    wsSource.close();
  });

  it("a WS-connected-but-non-executing device does not consume the command (delivered stays 0)", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await startTask(source.token, source.device_id, "build-1");

    // The agent WS is on the owning device but is NOT the task executor.
    const agentWs = await connect(source.token);
    await helloSource(agentWs, source.device_id);

    // A directed pause is pushed to the agent WS (best-effort hint)...
    await queueCommand(viewer.token, "build-1", "pause", "cmd-noconsume");
    const pushed = await agentWs.recv();
    expect(pushed.type).toBe("command");

    // ...but the row is NOT consumed by the WS push — still delivered=0.
    let row = (await pendingCommandRows(stub)).find((r) => r.command_id === "cmd-noconsume")!;
    expect(row.delivered).toBe(0);

    // Even a WS reconnect (drainPendingCommands re-pushes) does not consume it.
    const agentWs2 = await connect(source.token);
    await helloSource(agentWs2, source.device_id); // supersedes agentWs; re-drains pending
    row = (await pendingCommandRows(stub)).find((r) => r.command_id === "cmd-noconsume")!;
    expect(row.delivered).toBe(0);

    // The HTTP for_task_id drain (the real executor) reads it too — but
    // under P0-5 fetch-then-ack, mere inclusion in the response STILL does
    // not set delivered=1. Only an explicit ack does.
    const drain = await postEvents(source.token, [], "build-1");
    expect(drain.commands).toHaveLength(1);
    row = (await pendingCommandRows(stub)).find((r) => r.command_id === "cmd-noconsume")!;
    expect(row.delivered).toBe(0);

    // The executor acks it after durably handing it off locally.
    await postEvents(source.token, [], "build-1", ["cmd-noconsume"]);
    row = (await pendingCommandRows(stub)).find((r) => r.command_id === "cmd-noconsume")!;
    expect(row.delivered).toBe(1);

    agentWs.close();
    agentWs2.close();
  });

  it("a command against a revoked owning device is 409 'owning device unavailable' (MINOR)", async () => {
    const { source, viewer, ownerToken } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1"); // owning device = source

    // Revoke the owning (source) device.
    const revoke = await SELF.fetch(`${ORIGIN}/v1/devices/${source.device_id}`, { method: "DELETE", headers: { authorization: `Bearer ${ownerToken}` } });
    expect(revoke.status).toBe(200);

    // A command for the task now has an owning device that no longer resolves.
    const res = await queueCommand(viewer.token, "build-1", "pause", "cmd-revoked");
    expect(res.status).toBe(409);
    expect(await res.json()).toEqual({ error: "owning device unavailable" });
  });

  it("POST /v1/automations/:id/run enqueues NO command on either transport (it is a field poll, §5.1)", async () => {
    const { source, ownerToken, viewer } = await bootstrapSpace();
    await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ automation_id: "nightly", name: "Nightly", executor_kind: "script", schedule: { every_seconds: 86400 } }),
    });
    const runRes = await SELF.fetch(`${ORIGIN}/v1/automations/nightly/run`, {
      method: "POST",
      headers: { authorization: `Bearer ${viewer.token}` },
    });
    expect(runRes.status).toBe(200);

    // Since run_now is no longer a command, an HTTP source polling gets no
    // command...
    const httpBody = await postEvents(source.token, []);
    expect(httpBody.commands ?? []).toHaveLength(0);

    // ...and a WS source that completes hello receives its post-hello
    // rate-state command but NO run_now command frame (the drain is empty).
    const wsSource = await connect(source.token);
    const { rate } = await helloSource(wsSource, source.device_id);
    expect(rate.body.origin).toBe("server"); // the throttle/resume_rate frame, not a run_now
    await expect(wsSource.expectSilence(150)).resolves.toBe(true);
    wsSource.close();
  });
});

describe("POST /v1/events for_task_id — task-scoped command draining (v1-architecture.md §4.1, multi-process fix)", () => {
  it("with for_task_id, only that task's command is returned; another task's command stays undelivered", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    // One device runs two concurrent tasks (two `sitrep run` processes), so
    // both are owned by source.device_id.
    await startTask(source.token, source.device_id, "task-A");
    await startTask(source.token, source.device_id, "task-B");
    // Two directed pending commands for the two tasks (no source WS connected).
    await queueCommand(viewer.token, "task-A", "pause", "cmd-A");
    await queueCommand(viewer.token, "task-B", "stop", "cmd-B");

    // A `sitrep run` process owning task-A polls with its for_task_id.
    const aBody = await postEvents(source.token, [], "task-A");
    expect(aBody.commands).toHaveLength(1);
    expect(aBody.commands[0]).toMatchObject({ task_id: "task-A", action: "pause" });

    // task-B's command must NOT have been drained or marked delivered — it
    // waits for the process that owns task-B.
    const stub = spaceHubStub(spaceId);
    const pending = await pendingCommandRows(stub);
    const bRow = pending.find((r) => r.command_id === "cmd-B")!;
    expect(bRow.delivered).toBe(0);

    // task-B's process now polls with its own for_task_id and gets it.
    const bBody = await postEvents(source.token, [], "task-B");
    expect(bBody.commands).toHaveLength(1);
    expect(bBody.commands[0]).toMatchObject({ task_id: "task-B", action: "stop" });
  });

  it("without for_task_id, the drain is the plain per-device behavior (all task commands)", async () => {
    const { source, viewer } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "task-A");
    await startTask(source.token, source.device_id, "task-B");
    await queueCommand(viewer.token, "task-A", "pause", "cmd-A2");
    await queueCommand(viewer.token, "task-B", "stop", "cmd-B2");

    const body = await postEvents(source.token, []); // no for_task_id
    expect(body.commands).toHaveLength(2);
    expect(body.commands.map((c: any) => c.task_id).sort()).toEqual(["task-A", "task-B"]);
  });
});

describe("pending_commands lazy expiry sweep (v1-architecture.md §1.4, fix 7)", () => {
  it("an expired row is swept on the next drain even though no NEW command arrives", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await startTask(source.token, source.device_id, "task-A");
    await queueCommand(viewer.token, "task-A", "pause", "cmd-stale");

    // Backdate origin_ts so the row is well past its 60s TTL.
    const { runInDurableObject, env } = await import("cloudflare:test");
    await runInDurableObject(env.SPACE_HUB.getByName(spaceId), (_i, state) => {
      state.storage.sql.exec("UPDATE pending_commands SET origin_ts = ? WHERE command_id = 'cmd-stale'", Date.now() - 120_000);
    });
    expect((await pendingCommandRows(stub)).some((r) => r.command_id === "cmd-stale")).toBe(true);

    // A drain that carries NO new command (an empty heartbeat) must still
    // sweep the stale row — relayOrQueueCommand is never called here.
    await postEvents(source.token, []);
    expect((await pendingCommandRows(stub)).some((r) => r.command_id === "cmd-stale")).toBe(false);
  });
});
