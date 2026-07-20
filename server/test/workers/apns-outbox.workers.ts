// PushOutbox state machine (docs/design/v1-apns-outbox.md): push-to-start
// at-most-once bias (ambiguous-dispatch grace), generation-staleness skip
// for activity_update/activity_end, permanent-error token cleanup, and
// Alarm re-arm on transient failure.
import { describe, expect, it, vi } from "vitest";
import {
  apnsJsonResponse,
  armAlarmNow,
  backdateDispatchStartedAt,
  backdateTerminalAt,
  bootstrapSpace,
  fireAlarm,
  hasScheduledAlarm,
  outboxRows,
  pushTokensRow,
  spaceHubStub,
  stubApnsFetch,
  sweepOutboxRetention,
} from "./helpers.ts";
import { env, SELF } from "cloudflare:test";

const ORIGIN = "https://example.com";

async function postEvents(token: string, events: unknown[]) {
  const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ events }),
  });
  return { status: res.status, body: (await res.json()) as any };
}

describe("PushOutbox: push_to_start at-most-once bias", () => {
  it("an ambiguous prior dispatch (dispatch_started_at > 60s old, still pending) is NOT retried", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);

    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ push_to_start_token: "push-to-start-token-1" }),
    });

    await postEvents(source.token, [
      { type: "task.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, task_id: "build-1", kind: "started", occurred_at: Date.now() } },
    ]);

    const rows = await outboxRows(stub);
    const startRow = rows.find((r) => r.kind === "push_to_start");
    expect(startRow).toBeDefined();
    expect(startRow!.status).toBe("pending");

    // Simulate a DO crash mid-dispatch: dispatch_started_at set, 70s old.
    await backdateDispatchStartedAt(stub, startRow!.push_id, 70_000);

    const apnsFetch = vi.fn(async () => apnsJsonResponse(200, {}));
    await stubApnsFetch(stub, apnsFetch);
    await armAlarmNow(stub); // deterministic: don't race ensureAlarm()'s fire-and-forget call
    await fireAlarm(stub);

    const after = await outboxRows(stub);
    const updated = after.find((r) => r.push_id === startRow!.push_id)!;
    expect(updated.status).toBe("permanent_failure");
    expect(updated.last_error).toBe("ambiguous_dispatch_outcome_not_retried");
    // Never dispatched a duplicate — duplicate Live Activities are worse
    // than one missed start push (v1-apns-outbox.md §4.1).
    expect(apnsFetch).not.toHaveBeenCalled();
  });
});

describe("PushOutbox: generation-staleness guard (activity_update/activity_end)", () => {
  it("a stale-generation activity_update is skipped at dispatch, never reaching APNs, while the new generation's push_to_start IS dispatched", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);

    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ push_to_start_token: "pts-token" }),
    });
    await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/live-activity-token`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ token: "la-token" }),
    });

    let seq = 1;
    const send = (kind: string, extra: Record<string, unknown> = {}) =>
      postEvents(source.token, [
        { type: "task.event", id: `e${seq}`, ts: Date.now(), body: { device_id: source.device_id, device_seq: seq++, task_id: "build-1", kind, occurred_at: Date.now(), ...extra } },
      ]);

    await send("started"); // generation 1 -> push_to_start(gen1) + activity_tokens catch-up push_update(gen1)
    await send("progress", { percent: 40 }); // generation 1 -> coalesces into an activity_update(gen1) row
    await send("done"); // generation 1 terminal -> activity_end(gen1) + alert
    await send("started"); // generation 2 (task was terminal) -> a fresh push_to_start(gen2)

    const rowsBeforeDispatch = await outboxRows(stub);
    const staleUpdate = rowsBeforeDispatch.find((r) => r.kind === "activity_update" && r.generation === 1);
    const freshStart = rowsBeforeDispatch.find((r) => r.kind === "push_to_start" && r.generation === 2);
    expect(staleUpdate, "expected a generation-1 activity_update row still pending when generation 2 started").toBeDefined();
    expect(freshStart, "expected a generation-2 push_to_start row").toBeDefined();

    const dispatchedTokens: string[] = [];
    await stubApnsFetch(stub, async (req: Request) => {
      dispatchedTokens.push(req.url.split("/").pop()!);
      return apnsJsonResponse(200, {});
    });
    await armAlarmNow(stub); // deterministic: don't race ensureAlarm()'s fire-and-forget call
    await fireAlarm(stub);

    const rowsAfter = await outboxRows(stub);
    const staleAfter = rowsAfter.find((r) => r.push_id === staleUpdate!.push_id)!;
    expect(staleAfter.status).toBe("permanent_failure");
    expect(staleAfter.last_error).toBe("superseded_by_newer_generation");
    expect(dispatchedTokens).not.toContain("la-token"); // the stale gen-1 update never reached "APNs"

    const freshAfter = rowsAfter.find((r) => r.push_id === freshStart!.push_id)!;
    expect(freshAfter.status).toBe("sent");
    expect(dispatchedTokens).toContain("pts-token");
  });
});

describe("PushOutbox: permanent APNs error cleans up the invalid token", () => {
  it("a BadDeviceToken response moves the row to permanent_failure and nulls the alert_token", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);

    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-bad" }),
    });

    await postEvents(source.token, [
      { type: "message.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, message_id: "m1", level: "error", text: "build broke", occurred_at: Date.now() } },
    ]);

    const before = await pushTokensRow(stub, viewer.device_id);
    expect(before?.alert_token).toBe("alert-token-bad");

    await stubApnsFetch(stub, async () => apnsJsonResponse(400, { reason: "BadDeviceToken" }));
    await armAlarmNow(stub); // deterministic: don't race ensureAlarm()'s fire-and-forget call
    await fireAlarm(stub);

    const rows = await outboxRows(stub);
    const alertRow = rows.find((r) => r.kind === "alert");
    expect(alertRow?.status).toBe("permanent_failure");
    expect(alertRow?.last_error).toBe("BadDeviceToken");

    const after = await pushTokensRow(stub, viewer.device_id);
    expect(after?.alert_token).toBeNull();
  });

  it("error-level messages use priority 10, info/warn use priority 5 (v1-apns-outbox.md §4.3)", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-1" }),
    });
    await postEvents(source.token, [
      { type: "message.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, message_id: "m-warn", level: "warn", text: "disk high", occurred_at: Date.now() } },
      { type: "message.event", id: "e2", ts: Date.now(), body: { device_id: source.device_id, device_seq: 2, message_id: "m-error", level: "error", text: "build broke", occurred_at: Date.now() } },
    ]);
    const rows = await outboxRows(stub);
    const warnRow = rows.find((r) => r.subject_id === "m-warn")!;
    const errorRow = rows.find((r) => r.subject_id === "m-error")!;
    expect(JSON.parse(warnRow.payload).priority).toBe(5);
    expect(JSON.parse(errorRow.payload).priority).toBe(10);
  });
});

describe("PushOutbox: transient failure retries and re-arms the alarm", () => {
  it("a 503 response keeps the row pending, increments attempts, and re-arms the alarm for a future retry", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-2" }),
    });
    await postEvents(source.token, [
      { type: "message.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, message_id: "m1", level: "info", text: "hi", occurred_at: Date.now() } },
    ]);

    await stubApnsFetch(stub, async () => new Response("", { status: 503 }));
    await armAlarmNow(stub); // deterministic: don't race ensureAlarm()'s fire-and-forget call
    await fireAlarm(stub);

    const rows = await outboxRows(stub);
    const row = rows.find((r) => r.kind === "alert")!;
    expect(row.status).toBe("pending");
    expect(row.attempts).toBe(1);
    expect(row.next_attempt_at).toBeGreaterThan(Date.now());
    expect(await hasScheduledAlarm(stub)).toBe(true);
  });

  it("honors Retry-After when present", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-3" }),
    });
    await postEvents(source.token, [
      { type: "message.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, message_id: "m1", level: "info", text: "hi", occurred_at: Date.now() } },
    ]);

    const before = Date.now();
    await stubApnsFetch(stub, async () => new Response("", { status: 429, headers: { "retry-after": "120" } }));
    await armAlarmNow(stub); // deterministic: don't race ensureAlarm()'s fire-and-forget call
    await fireAlarm(stub);

    const rows = await outboxRows(stub);
    const row = rows.find((r) => r.kind === "alert")!;
    expect(row.next_attempt_at).toBeGreaterThanOrEqual(before + 120_000);
  });
});

describe("APNS_DELIVERY_ENABLED=false pauses dispatch without affecting enqueue", () => {
  it("rows keep enqueuing and stay pending; the alarm just doesn't spend an attempt", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-4" }),
    });

    env.APNS_DELIVERY_ENABLED = false;
    try {
      const apnsFetch = vi.fn(async () => apnsJsonResponse(200, {}));
      await stubApnsFetch(stub, apnsFetch);
      await postEvents(source.token, [
        { type: "message.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, message_id: "m1", level: "info", text: "hi", occurred_at: Date.now() } },
      ]);
      await armAlarmNow(stub); // deterministic: don't race ensureAlarm()'s fire-and-forget call
      await fireAlarm(stub);

      const rows = await outboxRows(stub);
      expect(rows.find((r) => r.kind === "alert")?.status).toBe("pending");
      expect(apnsFetch).not.toHaveBeenCalled();
    } finally {
      env.APNS_DELIVERY_ENABLED = true;
    }
  });
});

describe("PushOutbox: retention sweep is keyed on terminal_at (v1-apns-outbox.md §6)", () => {
  it("a terminal row is stamped with terminal_at, and swept only once terminal_at is old enough", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-retain" }),
    });
    await postEvents(source.token, [
      { type: "message.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, message_id: "m1", level: "info", text: "hi", occurred_at: Date.now() } },
    ]);

    // Dispatch it to a 2xx -> the row becomes sent, and terminal_at is set.
    await stubApnsFetch(stub, async () => apnsJsonResponse(200, {}));
    await armAlarmNow(stub);
    await fireAlarm(stub);

    let rows = await outboxRows(stub);
    const sent = rows.find((r) => r.kind === "alert")!;
    expect(sent.status).toBe("sent");
    expect(sent.terminal_at).toBeTypeOf("number");
    expect(sent.terminal_at).not.toBeNull();

    // A fresh sweep must NOT delete a just-sent row (< 1h old).
    await sweepOutboxRetention(stub);
    rows = await outboxRows(stub);
    expect(rows.find((r) => r.push_id === sent.push_id)).toBeDefined();

    // Backdate terminal_at past the 1h sent-retention window -> swept.
    await backdateTerminalAt(stub, sent.push_id, 3600_000 + 60_000);
    await sweepOutboxRetention(stub);
    rows = await outboxRows(stub);
    expect(rows.find((r) => r.push_id === sent.push_id)).toBeUndefined();
  });
});

describe("PushOutbox: task done/failed emits ONLY activity_end, never an alert (v1-apns-outbox.md §4.3)", () => {
  it("a done event enqueues activity_end but NO alert row, even with an alert token registered", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);

    // Register BOTH a live-activity token (so activity_end can enqueue) and
    // an alert token (so the old auto-alert, if it still existed, WOULD
    // enqueue an alert row — proving its absence is deliberate).
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-done" }),
    });
    await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/live-activity-token`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ token: "la-token-done" }),
    });

    let seq = 1;
    const send = (kind: string, extra: Record<string, unknown> = {}) =>
      postEvents(source.token, [
        { type: "task.event", id: `e${seq}`, ts: Date.now(), body: { device_id: source.device_id, device_seq: seq++, task_id: "build-1", kind, occurred_at: Date.now(), ...extra } },
      ]);

    await send("started");
    await send("done", { message: "all good" });

    const rows = await outboxRows(stub);
    // activity_end present, alert absent for this task lifecycle.
    expect(rows.some((r) => r.kind === "activity_end")).toBe(true);
    expect(rows.some((r) => r.kind === "alert")).toBe(false);
  });

  it("a failed event likewise enqueues activity_end but NO alert row", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ alert_token: "alert-token-fail" }),
    });
    await SELF.fetch(`${ORIGIN}/v1/tasks/build-2/live-activity-token`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ token: "la-token-fail" }),
    });

    let seq = 1;
    const send = (kind: string, extra: Record<string, unknown> = {}) =>
      postEvents(source.token, [
        { type: "task.event", id: `e${seq}`, ts: Date.now(), body: { device_id: source.device_id, device_seq: seq++, task_id: "build-2", kind, occurred_at: Date.now(), ...extra } },
      ]);

    await send("started");
    await send("failed", { message: "boom" });

    const rows = await outboxRows(stub);
    expect(rows.some((r) => r.kind === "activity_end")).toBe(true);
    expect(rows.some((r) => r.kind === "alert")).toBe(false);

    // A script-emitted message.event, by contrast, DOES produce an alert —
    // confirming we only removed the task-lifecycle auto-alert.
    await postEvents(source.token, [
      { type: "message.event", id: "msg", ts: Date.now(), body: { device_id: source.device_id, device_seq: seq++, message_id: "m1", level: "error", text: "explicit", occurred_at: Date.now() } },
    ]);
    const afterMsg = await outboxRows(stub);
    expect(afterMsg.some((r) => r.kind === "alert")).toBe(true);
  });
});
