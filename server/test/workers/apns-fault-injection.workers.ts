// Fault-injection fixes for the PushOutbox alarm subsystem
// (v1-apns-outbox.md §2/§7/§8.2, §6): universal anomaly self-heal,
// delivery-paused non-busy-loop, and cap-drop logging.
import { env, SELF } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  apnsJsonResponse,
  armAlarmNow,
  backdateExpiresAt,
  bootstrapSpace,
  clearAlarm,
  connect,
  fireAlarm,
  helloSource,
  outboxCountForDevice,
  outboxRows,
  scheduledAlarmAt,
  securityLog,
  seedOutboxRows,
  spaceHubStub,
  stubApnsFetch,
} from "./helpers.ts";

const ORIGIN = "https://example.com";

async function registerAlertToken(token: string, apnsToken: string) {
  await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
    method: "PUT",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ alert_token: apnsToken }),
  });
}

async function enqueueAlert(sourceToken: string, deviceId: string, seq: number) {
  await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${sourceToken}`, "content-type": "application/json" },
    body: JSON.stringify({
      events: [{ type: "message.event", id: `e${seq}`, ts: Date.now(), body: { device_id: deviceId, device_seq: seq, message_id: `m${seq}`, level: "info", text: "hi", occurred_at: Date.now() } }],
    }),
  });
}

async function enqueueTask(sourceToken: string, deviceId: string, seq: number, taskId: string, kind: string, extra: Record<string, unknown> = {}) {
  await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${sourceToken}`, "content-type": "application/json" },
    body: JSON.stringify({
      events: [{ type: "task.event", id: `t${seq}`, ts: Date.now(), body: { device_id: deviceId, device_seq: seq, task_id: taskId, kind, occurred_at: Date.now(), ...extra } }],
    }),
  });
}

describe("BLOCKER: universal anomaly self-heal (v1-apns-outbox.md §2/§7)", () => {
  it("a non-enqueue HTTP request (GET /v1/snapshot) re-arms a stranded row after a simulated setAlarm failure", async () => {
    const { source, viewer, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await registerAlertToken(viewer.token, "alert-heal");
    await enqueueAlert(source.token, source.device_id, 1);

    // Simulate the anomaly: the enqueue's fire-and-forget ensureAlarm failed
    // (or the DO was evicted before it ran) — a pending row with no alarm.
    await clearAlarm(stub);
    expect(await scheduledAlarmAt(stub)).toBeNull();
    expect((await outboxRows(stub)).some((r) => r.kind === "alert" && r.status === "pending")).toBe(true);

    // A NON-push-worthy request (a plain read) must re-arm it — this is the
    // recovery path the blocker is about. GET /v1/snapshot resolves the token
    // (resolveToken -> ensureAlarmIfPending) without enqueuing anything.
    await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(await scheduledAlarmAt(stub)).not.toBeNull();
  });

  it("a WS frame re-arms a stranded row (webSocketMessage self-heal)", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await registerAlertToken(viewer.token, "alert-heal-ws");
    await enqueueAlert(source.token, source.device_id, 1);
    await clearAlarm(stub);
    expect(await scheduledAlarmAt(stub)).toBeNull();

    // A viewer opens a WS and sends a frame; webSocketMessage's self-heal
    // re-arms the stranded row. helloOffer is the first frame.
    const ws = await connect(viewer.token);
    ws.send({ type: "hello", id: "h1", ts: Date.now(), body: { stage: "offer", device_id: viewer.device_id, role: "viewer", protocol_versions: [1] } });
    await ws.recv(); // hello accept
    // Give the async self-heal (awaited at the top of webSocketMessage) a beat.
    await new Promise((r) => setTimeout(r, 50));
    expect(await scheduledAlarmAt(stub)).not.toBeNull();
    ws.close();
  });
});

describe("MAJOR: APNS_DELIVERY_ENABLED=false does not busy-loop (v1-apns-outbox.md §8.2)", () => {
  afterEach(() => {
    env.APNS_DELIVERY_ENABLED = true;
  });

  it("a paused alarm advances eligible rows' next_attempt_at to the future instead of re-arming to a past instant", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await registerAlertToken(viewer.token, "alert-pause");

    env.APNS_DELIVERY_ENABLED = false;
    const apnsFetch = vi.fn(async () => apnsJsonResponse(200, {}));
    await stubApnsFetch(stub, apnsFetch);
    await enqueueAlert(source.token, source.device_id, 1);

    const before = Date.now();
    await armAlarmNow(stub);
    await fireAlarm(stub);

    const rows = await outboxRows(stub);
    const row = rows.find((r) => r.kind === "alert")!;
    expect(row.status).toBe("pending"); // still pending, not dispatched
    expect(apnsFetch).not.toHaveBeenCalled();
    // The key busy-loop fix: next_attempt_at is pushed into the future...
    expect(row.next_attempt_at).toBeGreaterThan(before);
    // ...and the re-armed alarm is therefore also in the future (a LATER
    // check), not an immediate refire.
    const alarmAt = await scheduledAlarmAt(stub);
    expect(alarmAt).not.toBeNull();
    expect(alarmAt!).toBeGreaterThan(before);

    // A second fire while still paused must not pull it back to the past.
    await fireAlarm(stub);
    const alarmAt2 = await scheduledAlarmAt(stub);
    expect(alarmAt2!).toBeGreaterThan(before);
  });

  it("while paused, an expired row and a generation-stale row both reach terminal; a live row is bumped +60s (no busy-loop)", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ push_to_start_token: "pts-pause", alert_token: "alert-pause2" }),
    });
    await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/live-activity-token`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ token: "la-pause" }),
    });

    // Drive a task through gen-1 progress then a gen-2 restart, so the gen-1
    // activity_update row is left pending-but-stale; plus a message.event for
    // a live alert row we'll backdate to expired.
    let seq = 1;
    const sendTask = (kind: string, extra: Record<string, unknown> = {}) =>
      enqueueTask(source.token, source.device_id, seq++, "build-1", kind, extra);
    await sendTask("started"); // gen 1 -> push_to_start(gen1) + catch-up activity_update(gen1)
    await sendTask("progress", { percent: 30 }); // coalesces into activity_update(gen1)
    await sendTask("done"); // gen 1 terminal
    await sendTask("started"); // gen 2 -> makes the gen-1 activity_update stale
    await enqueueAlert(source.token, source.device_id, 100); // a live alert row

    const staged = await outboxRows(stub);
    const staleUpdate = staged.find((r) => r.kind === "activity_update" && r.generation === 1)!;
    const liveStart = staged.find((r) => r.kind === "push_to_start" && r.generation === 2)!;
    const alertRow = staged.find((r) => r.kind === "alert")!;
    expect(staleUpdate).toBeDefined();
    expect(liveStart).toBeDefined();
    // Backdate the alert row's expires_at so it's due-and-expired.
    await backdateExpiresAt(stub, alertRow.push_id, 60_000);

    // Now pause delivery and fire the alarm.
    env.APNS_DELIVERY_ENABLED = false;
    const apnsFetch = vi.fn(async () => apnsJsonResponse(200, {}));
    await stubApnsFetch(stub, apnsFetch);

    const before = Date.now();
    await armAlarmNow(stub);
    await fireAlarm(stub);

    const after = await outboxRows(stub);
    // Non-network terminal decisions ran despite the pause: no APNs call made.
    expect(apnsFetch).not.toHaveBeenCalled();

    const staleAfter = after.find((r) => r.push_id === staleUpdate.push_id)!;
    expect(staleAfter.status).toBe("permanent_failure");
    expect(staleAfter.last_error).toBe("superseded_by_newer_generation");

    const expiredAfter = after.find((r) => r.push_id === alertRow.push_id)!;
    expect(expiredAfter.status).toBe("expired");

    // The live gen-2 push_to_start is NOT terminated — still pending, bumped
    // to the future (no busy-loop), and the alarm is in the future too.
    const liveAfter = after.find((r) => r.push_id === liveStart.push_id)!;
    expect(liveAfter.status).toBe("pending");
    expect(liveAfter.next_attempt_at).toBeGreaterThan(before);
    const alarmAt = await scheduledAlarmAt(stub);
    expect(alarmAt).not.toBeNull();
    expect(alarmAt!).toBeGreaterThan(before);
  });
});

describe("MINOR: makeRoomForInsert logs when an insert is dropped (v1-apns-outbox.md §6)", () => {
  it("device cap: oldest evictable rows are evicted to make room for a new alert", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await registerAlertToken(viewer.token, "alert-cap");

    // Fill the per-device cap (200) with evictable 'alert' rows.
    await seedOutboxRows(stub, viewer.device_id, "alert", 200, 1000);
    expect(await outboxCountForDevice(stub, viewer.device_id)).toBe(200);

    // A new alert enqueue (a source message.event) fans out to the viewer's
    // alert_token device; makeRoomForInsert evicts the oldest evictable row
    // and inserts, keeping that device at the cap (not exceeding it).
    await enqueueAlert(source.token, source.device_id, 1);
    expect(await outboxCountForDevice(stub, viewer.device_id)).toBe(200);
  });

  it("reject-when-exhausted: a device full of NON-evictable rows drops the new insert and logs it", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await registerAlertToken(viewer.token, "alert-cap-full");

    // Fill the per-device cap with push_to_start rows (never evictable).
    await seedOutboxRows(stub, viewer.device_id, "push_to_start", 200, 1000);
    expect(await outboxCountForDevice(stub, viewer.device_id)).toBe(200);

    // A source message.event fans an alert out to the viewer's alert_token
    // device (already at the cap with non-evictable rows).
    await enqueueAlert(source.token, source.device_id, 1);

    // No new row inserted (still at the cap; nothing evictable to make room)...
    expect(await outboxCountForDevice(stub, viewer.device_id)).toBe(200);
    expect((await outboxRows(stub)).some((r) => r.kind === "alert")).toBe(false);
    // ...and the drop is logged (never silent).
    const log = await securityLog(stub);
    expect(log.some((e) => e.event === "outbox_insert_dropped" && e.data.cap === "device")).toBe(true);
  });
});
