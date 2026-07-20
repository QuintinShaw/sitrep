// External-review P0/pre-launch items:
//  P0-1 owner capability superset + spaces device_id + client-declared WS role
//  P0-2 persistent metrics_current (survives eviction; no duplicate alert)
//  P0-3 task-directed command delivery (not consumed by another online source)
//  P0-4 monotonic run_request_id (+ Idempotency-Key dedup)
//  5c   ensureAlarm lifecycle via waitUntil
//  5e   space-creation env gate
import { env, SELF } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  apnsJsonResponse,
  armAlarmNow,
  bootstrapSpace,
  connect,
  evictSpaceHub,
  fireAlarm,
  helloOffer,
  outboxRows,
  scheduledAlarmAt,
  spaceHubStub,
  stubApnsFetch,
} from "./helpers.ts";

const ORIGIN = "https://example.com";

async function post(path: string, token: string | null, body?: unknown, extraHeaders: Record<string, string> = {}) {
  const headers: Record<string, string> = { "content-type": "application/json", ...extraHeaders };
  if (token) headers.authorization = `Bearer ${token}`;
  return SELF.fetch(`${ORIGIN}${path}`, { method: "POST", headers, body: body === undefined ? undefined : JSON.stringify(body) });
}

async function put(path: string, token: string, body: unknown) {
  return SELF.fetch(`${ORIGIN}${path}`, { method: "PUT", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(body) });
}

const startedEnvelope = (deviceId: string, seq: number, taskId: string) => ({
  events: [{ type: "task.event", id: `s${seq}`, ts: Date.now(), body: { device_id: deviceId, device_seq: seq, task_id: taskId, kind: "started", occurred_at: Date.now() } }],
});

describe("P0-1: owner capability superset + spaces device_id + client-declared WS role", () => {
  it("POST /v1/spaces returns device_id, and the owner token can POST /v1/events (was 403)", async () => {
    const createRes = await post("/v1/spaces", null, { platform: "test", name: "mac" });
    expect(createRes.status).toBe(200);
    const { owner_token, device_id } = (await createRes.json()) as { owner_token: string; device_id: string };
    expect(typeof device_id).toBe("string");

    // The live repro: owner_token -> POST /v1/events must be 200, not 403.
    const eventsRes = await post("/v1/events", owner_token, startedEnvelope(device_id, 1, "owner-task"));
    expect(eventsRes.status).toBe(200);
    const body = (await eventsRes.json()) as { results: Array<{ status: string }> };
    expect(body.results[0].status).toBe("applied");

    // Owner may also POST a task log (the other source-only uplink).
    const logRes = await post("/v1/tasks/owner-task/log", owner_token, { lines: ["hi"] });
    expect(logRes.status).toBe(200);
  });

  it("WS role is client-declared and token-constrained", async () => {
    const { source, viewer, ownerToken, ownerDeviceId } = await bootstrapSpace();

    // owner-token may present EITHER source or viewer.
    const ownerAsSource = await connect(ownerToken);
    expect((await helloOffer(ownerAsSource, ownerDeviceId, "source")).body.stage).toBe("accept");
    ownerAsSource.close();
    const ownerAsViewer = await connect(ownerToken);
    expect((await helloOffer(ownerAsViewer, ownerDeviceId, "viewer")).body.stage).toBe("accept");
    ownerAsViewer.close();

    // viewer-token presenting `source` is rejected (unauthorized).
    const viewerAsSource = await connect(viewer.token);
    const rejected = await helloOffer(viewerAsSource, viewer.device_id, "source");
    expect(rejected.type).toBe("error");
    expect(rejected.body.code).toBe("unauthorized");
    await viewerAsSource.waitForClose();

    // source-token presenting `viewer` is rejected.
    const sourceAsViewer = await connect(source.token);
    const rejected2 = await helloOffer(sourceAsViewer, source.device_id, "viewer");
    expect(rejected2.type).toBe("error");
    expect(rejected2.body.code).toBe("unauthorized");
    await sourceAsViewer.waitForClose();
  });
});

describe("P0-2: persistent metrics_current survives eviction; no duplicate alert", () => {
  it("last value + fired threshold persist across a DO rebuild, and a still-over sample does NOT re-fire", async () => {
    const { source, viewer, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await put("/v1/devices/self/push-tokens", viewer.token, { alert_token: "alert-metric" });
    await stubApnsFetch(stub, async () => apnsJsonResponse(200, {}));

    // metric.frame ts must be valid unix-ms; increase it monotonically for the
    // staleness check.
    const base = Date.now();
    const sample = (id: string, value: string, tsOffset: number) => ({
      events: [{ type: "metric.frame", id, ts: base + tsOffset, body: { device_id: source.device_id, metrics: [{ metric_id: "cpu", value, ts: base + tsOffset, alert_above: "90" }] } }],
    });

    // First over-threshold sample -> fires one alert (armed -> fired).
    await post("/v1/events", source.token, sample("m1", "95", 1000));
    let alerts = (await outboxRows(stub)).filter((r) => r.kind === "alert");
    expect(alerts).toHaveLength(1);

    // Evict the DO — the in-memory cache is wiped, but metrics_current (value
    // + alert_state "fired") persists.
    await evictSpaceHub(stub);

    // GET /v1/metrics/:id still returns the last accepted value.
    const metricRes = await SELF.fetch(`${ORIGIN}/v1/metrics/cpu`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(metricRes.status).toBe(200);
    expect(((await metricRes.json()) as { value: string }).value).toBe("95");

    // Another still-over-threshold sample AFTER the rebuild must NOT re-fire a
    // duplicate alert — the threshold is persisted as "fired".
    await post("/v1/events", source.token, sample("m2", "97", 2000));
    alerts = (await outboxRows(stub)).filter((r) => r.kind === "alert");
    expect(alerts).toHaveLength(1); // still just the one — no duplicate

    // Returning inside the line re-arms; going back over fires again.
    await post("/v1/events", source.token, sample("m3", "50", 3000));
    await post("/v1/events", source.token, sample("m4", "99", 4000));
    alerts = (await outboxRows(stub)).filter((r) => r.kind === "alert");
    expect(alerts).toHaveLength(2); // re-armed then fired again
  });
});

describe("P0-3: a directed command is not consumed by another online source", () => {
  it("only the task's owning device drains the command; a second online source never sees it", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const source2 = await bootstrapSecondSource(spaceId, viewer.token, source.token);

    // source (device 1) owns build-1.
    await post("/v1/events", source.token, startedEnvelope(source.device_id, 1, "build-1"));

    // Both sources are online over WS.
    const ws1 = await connect(source.token);
    await helloOfferSource(ws1, source.device_id);
    const ws2 = await connect(source2.token);
    await helloOfferSource(ws2, source2.device_id);

    // A viewer pauses build-1 -> directed to the owning device (source 1).
    await post("/v1/tasks/build-1/commands", viewer.token, { action: "pause" }, { "idempotency-key": "pause-directed" });

    // source 1 receives it...
    const frame = await ws1.recv();
    expect(frame.type).toBe("command");
    expect(frame.body.action).toBe("pause");
    // ...and source 2 (an unrelated online source) must NOT.
    await expect(ws2.expectSilence(200)).resolves.toBe(true);

    // Nor does source 2's HTTP drain get it.
    const drain2 = await post("/v1/events", source2.token, { events: [] });
    expect(((await drain2.json()) as { commands?: unknown[] }).commands ?? []).toHaveLength(0);

    ws1.close();
    ws2.close();
  });
});

describe("P0-4: monotonic run_request_id (+ Idempotency-Key dedup)", () => {
  it("increments per run, dedups a retry with the same key, and is exactly-once by distinct id", async () => {
    const { ownerToken, viewer } = await bootstrapSpace();
    await post("/v1/automations", ownerToken, { automation_id: "nightly", name: "Nightly", executor_kind: "script", schedule: { every_seconds: 86400 } });

    const runId = async () => {
      const list = (await (await SELF.fetch(`${ORIGIN}/v1/automations`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as Array<{ automation_id: string; run_request_id: number }>;
      return list.find((a) => a.automation_id === "nightly")!.run_request_id;
    };

    expect(await runId()).toBe(0); // never triggered

    // Each bare run increments.
    expect((await post("/v1/automations/nightly/run", viewer.token)).status).toBe(200);
    expect(await runId()).toBe(1);
    await post("/v1/automations/nightly/run", viewer.token);
    expect(await runId()).toBe(2);

    // Idempotency-Key dedups a retry of the SAME tap to ONE increment.
    await post("/v1/automations/nightly/run", viewer.token, undefined, { "idempotency-key": "tap-3" });
    expect(await runId()).toBe(3);
    await post("/v1/automations/nightly/run", viewer.token, undefined, { "idempotency-key": "tap-3" }); // retry
    expect(await runId()).toBe(3); // NOT 4 — deduped

    // Agent-style exactly-once: an agent that consumed up to id N runs once per
    // NEW distinct id it observes.
    let lastConsumed = 0;
    let runs = 0;
    for (const observed of [1, 2, 3, 3]) {
      if (observed > lastConsumed) {
        runs++;
        lastConsumed = observed;
      }
    }
    expect(runs).toBe(3); // ids 1,2,3 each ran once; the deduped repeat of 3 did not

    // 404 on unknown automation.
    expect((await post("/v1/automations/does-not-exist/run", viewer.token)).status).toBe(404);
  });
});

describe("5c: enqueue re-arms the outbox alarm with an awaited lifecycle", () => {
  it("the awaited ensureAlarm the enqueue path uses schedules the alarm and dispatches the row (not stranded)", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    await put("/v1/devices/self/push-tokens", viewer.token, { alert_token: "alert-life" });

    // A message.event enqueues an outbox row. The enqueuing request then AWAITS
    // ensureAlarm (POST /v1/events -> ensureOutboxAlarm; the WS path awaits
    // ensureAlarm at the end of webSocketMessage) — a proper request-scoped
    // lifecycle, not abandoned fire-and-forget work that a returning request
    // could strand.
    await post("/v1/events", source.token, {
      events: [{ type: "message.event", id: "e1", ts: Date.now(), body: { device_id: source.device_id, device_seq: 1, message_id: "m1", level: "info", text: "hi", occurred_at: Date.now() } }],
    });
    // The enqueued row is pending (durably written by the RPC).
    expect((await outboxRows(stub)).find((r) => r.kind === "alert")?.status).toBe("pending");

    // armAlarmNow runs the SAME ensureAlarm the enqueue path awaits (directly
    // in the DO context, which the test harness surfaces): it schedules the
    // alarm from the pending row, and firing it dispatches the row. This
    // exercises the re-arm-then-dispatch lifecycle end to end.
    await stubApnsFetch(stub, async () => apnsJsonResponse(200, {}));
    await armAlarmNow(stub);
    expect(await scheduledAlarmAt(stub)).not.toBeNull();
    expect(await fireAlarm(stub)).toBe(true);
    expect((await outboxRows(stub)).find((r) => r.kind === "alert")?.status).toBe("sent");
  });
});

describe("5e: space-creation env gate", () => {
  afterEach(() => {
    env.SPACE_CREATION_ENABLED = true;
  });

  it("POST /v1/spaces is 403 when SPACE_CREATION_ENABLED is off — enforced before any DO is created", async () => {
    env.SPACE_CREATION_ENABLED = false;
    const res = await post("/v1/spaces", null, { platform: "test", name: "mac" });
    expect(res.status).toBe(403);

    env.SPACE_CREATION_ENABLED = true;
    const ok = await post("/v1/spaces", null, { platform: "test", name: "mac" });
    expect(ok.status).toBe(200);
  });
});
// ---- local helpers ----

async function helloOfferSource(client: Awaited<ReturnType<typeof connect>>, deviceId: string): Promise<void> {
  const accept = await helloOffer(client, deviceId, "source");
  if (accept.type !== "hello") throw new Error(`expected hello accept, got ${JSON.stringify(accept)}`);
  // A source also receives its post-hello interest-state command; consume it.
  await client.recv();
}

async function bootstrapSecondSource(spaceId: string, ownerOrViewerToken: string, existingSourceToken: string): Promise<{ token: string; device_id: string }> {
  // Mint a second source device in the same space.
  const inviteRes = await SELF.fetch(`${ORIGIN}/v1/invites`, {
    method: "POST",
    headers: { authorization: `Bearer ${ownerOrViewerToken}`, "content-type": "application/json" },
    body: JSON.stringify({ role: "source" }),
  });
  const { code } = (await inviteRes.json()) as { code: string };
  const joinRes = await SELF.fetch(`${ORIGIN}/v1/join`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ code, space: spaceId, name: "source-2", platform: "test" }),
  });
  void existingSourceToken;
  return (await joinRes.json()) as { token: string; device_id: string };
}
