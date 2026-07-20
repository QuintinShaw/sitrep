// Promoted from an adversarial review pass targeting P0-5 fetch-then-ack
// (commit 33fa5cb), written independently of the existing
// events-command-piggyback.workers.ts suite to probe scenarios that suite
// does not cover: TTL/expiry races around ack, duplicate-ack DB-row
// idempotency, and cross-device ack safety under a race with the owning
// device's own poll. Run live against real workerd via vitest-pool-workers
// (`npm run test:workers`), not mocked.
import { SELF, runInDurableObject, env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, pendingCommandRows, spaceHubStub } from "./helpers.ts";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";

const ORIGIN = "https://example.com";

async function postEvents(token: string, events: unknown[], forTaskId?: string, ackCommandIds?: string[]): Promise<any> {
  const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ events, ...(forTaskId ? { for_task_id: forTaskId } : {}), ...(ackCommandIds ? { ack_command_ids: ackCommandIds } : {}) }),
  });
  return { status: res.status, body: await res.json() };
}

async function queueCommand(viewerToken: string, taskId: string, action: string, idempotencyKey: string): Promise<Response> {
  return SELF.fetch(`${ORIGIN}/v1/tasks/${taskId}/commands`, {
    method: "POST",
    headers: { authorization: `Bearer ${viewerToken}`, "content-type": "application/json", "idempotency-key": idempotencyKey },
    body: JSON.stringify({ action }),
  });
}

let seqCounter = 0;
async function startTask(sourceToken: string, deviceId: string, taskId: string): Promise<any> {
  const seq = ++seqCounter;
  return postEvents(sourceToken, [{ type: "task.event", id: `start-${seq}`, ts: Date.now(), body: { device_id: deviceId, device_seq: seq, task_id: taskId, kind: "started", occurred_at: Date.now() } }]);
}

describe("P0-5 adversarial: ack racing TTL expiry / GC", () => {
  it("(f) an ack for a command that has already expired (past TTL, still present in table pre-sweep) is a silent no-op — row stays gone from future polls either way, no crash", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "pause", "cmd-race-ttl");

    // Backdate origin_ts past TTL WITHOUT triggering a drain (so the row is
    // still physically present, not yet lazily swept).
    const stub = spaceHubStub(spaceId);
    await runInDurableObject(stub as any, (_i: any, state: any) => {
      state.storage.sql.exec("UPDATE pending_commands SET origin_ts = ? WHERE command_id = 'cmd-race-ttl'", Date.now() - 120_000);
    });
    let rows = await pendingCommandRows(stub);
    expect(rows.some((r) => r.command_id === "cmd-race-ttl")).toBe(true); // still physically present

    // Now the device (which never actually saw it, since it expired before
    // its first successful poll in this scenario) sends an ack for it anyway
    // -- simulates a delayed/duplicate ack arriving after expiry.
    const res = await postEvents(source.token, [], undefined, ["cmd-race-ttl"]);
    expect(res.status).toBe(200);
    expect(res.body.commands ?? []).toHaveLength(0);

    // Row must be gone (swept by the ack-triggering drain's lazy sweep) and
    // no crash / error surfaced.
    rows = await pendingCommandRows(stub);
    expect(rows.some((r) => r.command_id === "cmd-race-ttl")).toBe(false);
  });

  it("(f) ack for a command_id that was already GC'd (row physically deleted) is a silent no-op, not an error", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "resume", "cmd-gone");

    const stub = spaceHubStub(spaceId);
    // Hard-delete the row directly (simulates it having been swept already).
    await runInDurableObject(stub as any, (_i: any, state: any) => {
      state.storage.sql.exec("DELETE FROM pending_commands WHERE command_id = 'cmd-gone'");
    });
    expect((await pendingCommandRows(stub)).some((r) => r.command_id === "cmd-gone")).toBe(false);

    const res = await postEvents(source.token, [], undefined, ["cmd-gone"]);
    expect(res.status).toBe(200); // no crash, no 4xx/5xx
    expect(res.body.commands ?? []).toHaveLength(0);
  });

  it("(e) double-acking the same command_id in two separate requests is idempotent — second ack is a harmless no-op, UPDATE affects 0 rows", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "pause", "cmd-dup-ack");

    const first = await postEvents(source.token, []);
    expect(first.body.commands).toHaveLength(1);

    const ack1 = await postEvents(source.token, [], undefined, ["cmd-dup-ack"]);
    expect(ack1.status).toBe(200);
    const stub = spaceHubStub(spaceId);
    let row = (await pendingCommandRows(stub)).find((r) => r.command_id === "cmd-dup-ack")!;
    expect(row.delivered).toBe(1);

    // Second, redundant ack of the same id (e.g. daemon retried a send whose
    // response was lost, so it requeued the ack and sent it again).
    const ack2 = await postEvents(source.token, [], undefined, ["cmd-dup-ack"]);
    expect(ack2.status).toBe(200);
    expect(ack2.body.commands ?? []).toHaveLength(0);
    row = (await pendingCommandRows(stub)).find((r) => r.command_id === "cmd-dup-ack")!;
    expect(row.delivered).toBe(1); // unchanged, no error, no toggling back
  });

  it("(c) cross-device ack race: device B acks device A's command in the SAME instant A is polling — A still gets it, B's ack has zero effect on A's row", async () => {
    const { source, viewer, spaceId, inviteAndJoin } = await bootstrapSpace();
    const deviceB = await inviteAndJoin("source");
    await startTask(source.token, source.device_id, "build-1"); // owned by `source`
    await queueCommand(viewer.token, "build-1", "stop", "cmd-cross");

    // B (not the owner) attempts to ack concurrently with A's poll.
    const [ackAttempt, aPoll] = await Promise.all([postEvents(deviceB.token, [], undefined, ["cmd-cross"]), postEvents(source.token, [])]);
    expect(ackAttempt.status).toBe(200);
    expect(aPoll.status).toBe(200);
    // A must have received the command regardless of B's concurrent no-op ack.
    expect(aPoll.body.commands.some((c: any) => c.command_id === "cmd-cross")).toBe(true);

    const stub = spaceHubStub(spaceId);
    const row = (await pendingCommandRows(stub)).find((r) => r.command_id === "cmd-cross")!;
    expect(row.delivered).toBe(0); // still undelivered — only A's real ack can flip it
  });

  it("regression: 401 unauthenticated ack attempt has no effect on the row (auth boundary intact around the ack path)", async () => {
    const { source, viewer } = await bootstrapSpace();
    await startTask(source.token, source.device_id, "build-1");
    await queueCommand(viewer.token, "build-1", "pause", "cmd-noauth");

    const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
      method: "POST",
      headers: { authorization: `Bearer not-a-real-token`, "content-type": "application/json" },
      body: JSON.stringify({ events: [], ack_command_ids: ["cmd-noauth"] }),
    });
    expect(res.status).toBe(401);

    // The real device still sees it, undelivered.
    const body = await postEvents(source.token, []);
    expect(body.body.commands).toHaveLength(1);
    expect(body.body.commands[0].command_id).toBe("cmd-noauth");
  });
});
