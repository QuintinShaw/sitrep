// Shared helpers for the vitest-pool-workers realtime test suite. Not a
// test file itself (doesn't match vitest.config.ts's `*.workers.ts`
// include... actually it does match the extension but has no test()/
// describe() calls, so it just runs as an empty, harmless module if vitest
// ever loads it directly).
import { env, evictDurableObject, runDurableObjectAlarm, runInDurableObject, SELF } from "cloudflare:test";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";

const ORIGIN = "https://example.com";

export function spaceHubStub(spaceId: string): DurableObjectStub<SpaceHub> {
  return env.SPACE_HUB.getByName(spaceId);
}

/** Tears down the DO's in-memory instance (preserving durable SQLite
 * storage) to simulate a hibernation/eviction — used to prove the
 * now-persistent metric_series/task_logs survive it, where the old
 * in-memory ring buffers would have been wiped. */
export async function evictSpaceHub(stub: DurableObjectStub<SpaceHub>): Promise<void> {
  await evictDurableObject(stub, { webSockets: "close" });
}

/** Replaces the DO's injectable `apnsFetch` field (see space-hub.ts) so no
 * test ever makes a real outbound call to Apple. */
export async function stubApnsFetch(stub: DurableObjectStub<SpaceHub>, fn: (req: Request) => Response | Promise<Response>): Promise<void> {
  await runInDurableObject(stub, (instance) => {
    (instance as unknown as { apnsFetch: typeof fn }).apnsFetch = fn;
  });
}

/** Directly invokes the private `ensureAlarm()` (see space-hub.ts) so tests
 * don't race the fire-and-forget call an ingest RPC makes — deterministic
 * setup for "now force the alarm to run" test steps. */
export async function armAlarmNow(stub: DurableObjectStub<SpaceHub>): Promise<void> {
  await runInDurableObject(stub, async (instance) => {
    await (instance as unknown as { ensureAlarm(): Promise<void> }).ensureAlarm();
  });
}

/** Immediately runs the DO's scheduled alarm (if any) and returns whether
 * one actually ran, per @cloudflare/vitest-pool-workers. */
export async function fireAlarm(stub: DurableObjectStub<SpaceHub>): Promise<boolean> {
  return runDurableObjectAlarm(stub);
}

/** Deletes any scheduled alarm to simulate the anomaly the self-heal
 * targets: a setAlarm() failure (caught+logged) or a DO eviction between an
 * enqueue commit and its fire-and-forget re-arm, leaving a pending outbox
 * row with no alarm scheduled. */
export async function clearAlarm(stub: DurableObjectStub<SpaceHub>): Promise<void> {
  await runInDurableObject(stub, async (_i, state) => {
    await state.storage.deleteAlarm();
  });
}

/** Returns the current scheduled alarm time (ms) or null — lets a test
 * assert the delivery-paused re-arm is in the FUTURE, not a past instant. */
export async function scheduledAlarmAt(stub: DurableObjectStub<SpaceHub>): Promise<number | null> {
  return runInDurableObject(stub, (_i, state) => state.storage.getAlarm());
}

/** Reads the DO's in-memory securityEventLog (100%-sampled logAlways
 * events) — the same seam the pre-v1 supersession tests used — so a test
 * can assert an event like `outbox_insert_dropped` fired. */
export async function securityLog(stub: DurableObjectStub<SpaceHub>): Promise<Array<{ event: string; data: Record<string, unknown> }>> {
  return runInDurableObject(stub, (instance) => (instance as unknown as { securityEventLog: Array<{ event: string; data: Record<string, unknown> }> }).securityEventLog);
}

/** Reads pending_commands rows directly (no HTTP route) so a test can
 * assert the lazy-expiry sweep removed a stale row, or that a wrong-task
 * command was left delivered=0. */
export async function pendingCommandRows(
  stub: DurableObjectStub<SpaceHub>,
): Promise<Array<{ command_id: string; delivered: number; payload: string; origin_ts: number; ttl_ms: number }>> {
  return runInDurableObject(
    stub,
    (_i, state) =>
      state.storage.sql.exec("SELECT command_id, delivered, payload, origin_ts, ttl_ms FROM pending_commands").toArray() as unknown as Array<{
        command_id: string;
        delivered: number;
        payload: string;
        origin_ts: number;
        ttl_ms: number;
      }>,
  );
}

/** Seeds N pending push_outbox rows of a given kind for a device — used to
 * drive the makeRoomForInsert cap-eviction / reject paths deterministically
 * without POSTing thousands of real events. */
export async function seedOutboxRows(stub: DurableObjectStub<SpaceHub>, deviceId: string, kind: string, count: number, createdAtBase = 1): Promise<void> {
  await runInDurableObject(stub, (_i, state) => {
    const now = Date.now();
    for (let n = 0; n < count; n++) {
      const pushId = `seed-${kind}-${n}-${crypto.randomUUID()}`;
      state.storage.sql.exec(
        `INSERT INTO push_outbox (push_id, kind, device_id, subject_id, generation, revision, coalesce_key, payload, status, attempts, next_attempt_at, dispatch_started_at, last_error, created_at, expires_at, terminal_at)
         VALUES (?, ?, ?, ?, NULL, 0, ?, '{}', 'pending', 0, ?, NULL, NULL, ?, ?, NULL)`,
        pushId,
        kind,
        deviceId,
        `subj-${n}`,
        pushId,
        now,
        createdAtBase + n,
        now + 3600_000,
      );
    }
  });
}

/** Row count in push_outbox scoped to one device — for cap assertions. */
export async function outboxCountForDevice(stub: DurableObjectStub<SpaceHub>, deviceId: string): Promise<number> {
  return runInDurableObject(
    stub,
    (_i, state) => (state.storage.sql.exec("SELECT count(*) as n FROM push_outbox WHERE device_id = ?", deviceId).toArray()[0] as { n: number }).n,
  );
}

export interface PushOutboxTestRow {
  push_id: string;
  kind: string;
  device_id: string;
  subject_id: string;
  generation: number | null;
  revision: number;
  coalesce_key: string;
  payload: string;
  status: string;
  attempts: number;
  next_attempt_at: number;
  dispatch_started_at: number | null;
  last_error: string | null;
  created_at: number;
  expires_at: number;
  terminal_at: number | null;
}

/** Reads `push_outbox` rows directly out of the DO's SQLite storage —
 * there is no HTTP route for this (push_outbox is an internal
 * implementation detail), so tests reach in via `runInDurableObject`. */
export async function outboxRows(stub: DurableObjectStub<SpaceHub>): Promise<PushOutboxTestRow[]> {
  return runInDurableObject(stub, (_instance, state) => (state.storage.sql.exec("SELECT * FROM push_outbox ORDER BY created_at").toArray() as unknown as PushOutboxTestRow[]));
}

/** Whether the DO currently has a scheduled alarm — used to assert
 * "APNS_DELIVERY_ENABLED=false still re-arms" / "a transient failure keeps
 * an alarm scheduled" without racing `fireAlarm`. */
export async function hasScheduledAlarm(stub: DurableObjectStub<SpaceHub>): Promise<boolean> {
  return runInDurableObject(stub, (_instance, state) => state.storage.getAlarm().then((a) => a !== null));
}

/** Reads one device's push_tokens row (device-level push-to-start/alert
 * tokens, v1-architecture.md §1.1) — used to assert permanent-error token
 * cleanup (v1-apns-outbox.md §4.3). */
export async function pushTokensRow(stub: DurableObjectStub<SpaceHub>, deviceId: string): Promise<{ push_to_start_token: string | null; alert_token: string | null } | undefined> {
  return runInDurableObject(stub, (_instance, state) =>
    (state.storage.sql.exec("SELECT push_to_start_token, alert_token FROM push_tokens WHERE device_id = ?", deviceId).toArray() as unknown as Array<{ push_to_start_token: string | null; alert_token: string | null }>)[0],
  );
}

/** Backdates a push_outbox row's `dispatch_started_at` to simulate a prior
 * dispatch attempt that never resolved (DO crash/eviction mid-flight) —
 * exercises the push_to_start ambiguous-dispatch grace window
 * (v1-apns-outbox.md §4.1) deterministically, without needing to actually
 * race a real crash. */
export async function backdateDispatchStartedAt(stub: DurableObjectStub<SpaceHub>, pushId: string, ageMs: number): Promise<void> {
  await runInDurableObject(stub, (_instance, state) => {
    state.storage.sql.exec("UPDATE push_outbox SET dispatch_started_at = ? WHERE push_id = ?", Date.now() - ageMs, pushId);
  });
}

export function apnsJsonResponse(status: number, body: Record<string, unknown>, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
}

/** Backdates a pending row's `expires_at` into the past so the next drain
 * treats it as expired — without waiting real wall-clock time. Used to
 * exercise the expiry terminal transition (incl. while APNs delivery is
 * paused). */
export async function backdateExpiresAt(stub: DurableObjectStub<SpaceHub>, pushId: string, ageMs: number): Promise<void> {
  await runInDurableObject(stub, (_instance, state) => {
    state.storage.sql.exec("UPDATE push_outbox SET expires_at = ? WHERE push_id = ?", Date.now() - ageMs, pushId);
  });
}

/** Backdates a terminal row's `terminal_at` (v1-apns-outbox.md §3) so the
 * §6 retention sweep — which keys on terminal_at, not next_attempt_at —
 * treats it as old enough to delete, without waiting real wall-clock time. */
export async function backdateTerminalAt(stub: DurableObjectStub<SpaceHub>, pushId: string, ageMs: number): Promise<void> {
  await runInDurableObject(stub, (_instance, state) => {
    state.storage.sql.exec("UPDATE push_outbox SET terminal_at = ? WHERE push_id = ?", Date.now() - ageMs, pushId);
  });
}

/** Directly runs the private retention sweep (v1-apns-outbox.md §6) — the
 * alarm normally runs it after draining, but exposing it lets a test assert
 * the sweep in isolation without staging a full dispatch. */
export async function sweepOutboxRetention(stub: DurableObjectStub<SpaceHub>): Promise<void> {
  await runInDurableObject(stub, (instance) => {
    (instance as unknown as { sweepOutboxRetention(): void }).sweepOutboxRetention();
  });
}

export interface JoinedDevice {
  token: string;
  device_id: string;
  role: string;
}

export interface Bootstrapped {
  spaceId: string;
  ownerToken: string;
  /** The owner device's id, returned by POST /v1/spaces (P0-1). Needed to
   * uplink events as the owner (body.device_id must match). */
  ownerDeviceId: string;
  source: JoinedDevice;
  viewer: JoinedDevice;
  inviteAndJoin: (role: "source" | "viewer") => Promise<JoinedDevice>;
}

/** Creates a fresh space (via the /v1 pairing flow) with one source device
 * and one viewer device already joined, plus a helper to mint more devices
 * of either role. */
export async function bootstrapSpace(): Promise<Bootstrapped> {
  const createRes = await SELF.fetch(`${ORIGIN}/v1/spaces`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ platform: "test", name: "owner-mac" }),
  });
  if (createRes.status !== 200) throw new Error(`space creation failed: ${createRes.status} ${await createRes.text()}`);
  const { space_id, owner_token, device_id: owner_device_id } = (await createRes.json()) as { space_id: string; owner_token: string; device_id: string };

  const inviteAndJoin = async (role: "source" | "viewer"): Promise<JoinedDevice> => {
    const inviteRes = await SELF.fetch(`${ORIGIN}/v1/invites`, {
      method: "POST",
      headers: { authorization: `Bearer ${owner_token}`, "content-type": "application/json" },
      body: JSON.stringify({ role }),
    });
    if (inviteRes.status !== 200) throw new Error(`invite failed: ${inviteRes.status} ${await inviteRes.text()}`);
    const { code } = (await inviteRes.json()) as { code: string };
    const joinRes = await SELF.fetch(`${ORIGIN}/v1/join`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code, space: space_id, name: `${role}-device`, platform: "test" }),
    });
    if (joinRes.status !== 200) throw new Error(`join failed: ${joinRes.status} ${await joinRes.text()}`);
    return (await joinRes.json()) as JoinedDevice;
  };

  const source = await inviteAndJoin("source");
  const viewer = await inviteAndJoin("viewer");
  return { spaceId: space_id, ownerToken: owner_token, ownerDeviceId: owner_device_id, source, viewer, inviteAndJoin };
}

/** Opens a /v1/realtime WebSocket authenticated with the given token.
 * Returns the raw upgrade Response so callers can assert on failed
 * upgrades (e.g. invalid token -> 401, no `webSocket`). */
export async function upgrade(token: string | null): Promise<Response> {
  const headers = new Headers({ upgrade: "websocket" });
  if (token !== null) headers.set("authorization", `Bearer ${token}`);
  return SELF.fetch(`${ORIGIN}/v1/realtime`, { headers });
}

/** Thin promise-based wrapper around a Workers-runtime client WebSocket:
 * queues incoming text frames so `recv()` can await the next one in order,
 * exactly as a real client would process them. */
export class WsClient {
  readonly ws: WebSocket;
  private queue: string[] = [];
  private waiters: Array<(v: string) => void> = [];
  private closed: { code: number; reason: string } | null = null;
  private closeWaiters: Array<(v: { code: number; reason: string }) => void> = [];

  constructor(ws: WebSocket) {
    this.ws = ws;
    ws.accept();
    ws.addEventListener("message", (evt: MessageEvent) => {
      const data = typeof evt.data === "string" ? evt.data : new TextDecoder().decode(evt.data as ArrayBuffer);
      const waiter = this.waiters.shift();
      if (waiter) waiter(data);
      else this.queue.push(data);
    });
    ws.addEventListener("close", (evt: CloseEvent) => {
      this.closed = { code: evt.code, reason: evt.reason };
      for (const w of this.closeWaiters.splice(0)) w(this.closed);
    });
  }

  send(envelope: unknown): void {
    this.ws.send(JSON.stringify(envelope));
  }

  sendRaw(text: string): void {
    this.ws.send(text);
  }

  recvRaw(timeoutMs = 10_000): Promise<string> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
      const waiter = (v: string) => {
        clearTimeout(timer);
        resolve(v);
      };
      const timer = setTimeout(() => {
        // Critical: drop this waiter on timeout, or a message that arrives
        // later (e.g. right after an expectSilence() timeout) would be
        // handed to this already-settled promise and silently vanish
        // instead of reaching the next real recv()/expectSilence() call.
        const idx = this.waiters.indexOf(waiter);
        if (idx !== -1) this.waiters.splice(idx, 1);
        reject(new Error(`timed out waiting for a message after ${timeoutMs}ms`));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  async recv(timeoutMs = 10_000): Promise<any> {
    return JSON.parse(await this.recvRaw(timeoutMs));
  }

  /** Resolves true if no message arrives within timeoutMs (used to assert
   * silence — e.g. no live delta before a resume reply). */
  async expectSilence(timeoutMs = 200): Promise<boolean> {
    try {
      const msg = await this.recvRaw(timeoutMs);
      throw new Error(`expected silence but received: ${msg}`);
    } catch (err) {
      if (err instanceof Error && err.message.startsWith("timed out")) return true;
      throw err;
    }
  }

  waitForClose(timeoutMs = 10_000): Promise<{ code: number; reason: string }> {
    if (this.closed) return Promise.resolve(this.closed);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out waiting for close")), timeoutMs);
      this.closeWaiters.push((v) => {
        clearTimeout(timer);
        resolve(v);
      });
    });
  }

  close(): void {
    try {
      this.ws.close();
    } catch {
      // already closed
    }
  }
}

export async function connect(token: string): Promise<WsClient> {
  const res = await upgrade(token);
  if (res.status !== 101 || !res.webSocket) {
    throw new Error(`expected a 101 upgrade, got ${res.status}: ${await res.text().catch(() => "")}`);
  }
  return new WsClient(res.webSocket);
}

let idCounter = 0;
export function nextId(): string {
  idCounter += 1;
  return `t${idCounter}`;
}

export async function helloOffer(client: WsClient, deviceId: string, role: "source" | "viewer"): Promise<any> {
  client.send({
    type: "hello",
    id: nextId(),
    ts: Date.now(),
    body: { stage: "offer", device_id: deviceId, role, protocol_versions: [1] },
  });
  return client.recv();
}

/** Completes a source's hello AND consumes the interest-state command the
 * server sends every source immediately after its accept (throttle when no
 * lease is active, resume_rate otherwise). Returns both so tests that care
 * about the initial rate state can assert on it. */
export async function helloSource(client: WsClient, deviceId: string): Promise<{ accept: any; rate: any }> {
  const accept = await helloOffer(client, deviceId, "source");
  const rate = await client.recv();
  if (rate.type !== "command" || rate.body.origin !== "server") {
    throw new Error(`expected the post-hello interest-state command, got: ${JSON.stringify(rate)}`);
  }
  return { accept, rate };
}

export async function subscribe(client: WsClient, topics?: Array<"task" | "metric" | "message">): Promise<any> {
  client.send({ type: "subscribe", id: nextId(), ts: Date.now(), body: topics ? { topics } : {} });
  return client.recv();
}

export async function unsubscribe(client: WsClient): Promise<any> {
  client.send({ type: "unsubscribe", id: nextId(), ts: Date.now(), body: {} });
  return client.recv();
}

export async function resume(client: WsClient, lastRevision: number): Promise<any> {
  client.send({ type: "resume", id: nextId(), ts: Date.now(), body: { last_revision: lastRevision } });
  return client.recv();
}

export async function sendTaskEvent(
  client: WsClient,
  deviceId: string,
  deviceSeq: number,
  overrides: Partial<{ task_id: string; kind: string; occurred_at: number; percent: number; title: string }> = {},
): Promise<any> {
  const body = {
    device_id: deviceId,
    device_seq: deviceSeq,
    task_id: overrides.task_id ?? "run-1",
    kind: overrides.kind ?? "started",
    occurred_at: overrides.occurred_at ?? Date.now(),
    ...(overrides.percent !== undefined ? { percent: overrides.percent } : {}),
    ...(overrides.title !== undefined ? { title: overrides.title } : {}),
  };
  client.send({ type: "task.event", id: nextId(), ts: Date.now(), body });
  return client.recv();
}

export async function sendMessageEvent(
  client: WsClient,
  deviceId: string,
  deviceSeq: number,
  overrides: Partial<{ message_id: string; level: "info" | "warn" | "error"; text: string; occurred_at: number }> = {},
): Promise<any> {
  const body = {
    device_id: deviceId,
    device_seq: deviceSeq,
    message_id: overrides.message_id ?? `msg-${deviceSeq}`,
    level: overrides.level ?? "info",
    text: overrides.text ?? "hello",
    occurred_at: overrides.occurred_at ?? Date.now(),
  };
  client.send({ type: "message.event", id: nextId(), ts: Date.now(), body });
  return client.recv();
}

/** Sends a metric.frame WITHOUT waiting for a reply — the protocol has no
 * ack for metric.frame on success (SPEC.md §12); only a rate_limited error
 * replies. Callers that expect the rate-limited path call `client.recv()`
 * themselves afterward. */
export function sendMetricFrame(client: WsClient, deviceId: string, metrics: Array<{ metric_id: string; value: string; ts: number; alert_above?: string; alert_below?: string }>): void {
  client.send({ type: "metric.frame", id: nextId(), ts: Date.now(), body: { device_id: deviceId, metrics } });
}
