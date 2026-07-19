// Cloudflare Workers entry. State lives in a Durable Object (SQLite-backed
// storage); v0 is single-tenant with one DO instance. Auth is a single bearer
// token in the AUTH_TOKEN secret. Live Activity pushes fire from inside the
// DO after each state change (via waitUntil, off the ingest critical path).
import { DurableObject } from "cloudflare:workers";
import { createApp, newToken, TOKEN_RE, type Command, type DeviceInfo, type Role, type SpaceRegistry } from "../app.ts";
import { endActivity, sendAlert, startActivity, updateActivity, type ApnsConfig } from "../apns.ts";
import { SpaceHub } from "../realtime/space-hub.ts";
import type { AutomationExecutorKind, AutomationState } from "../realtime/types.ts";

// Re-exported so wrangler (which binds Durable Object classes by name from
// this entry module, see wrangler.jsonc) can resolve SpaceHub, same as
// UserStore below.
export { SpaceHub };
import {
  appendTaskLog,
  EVENT_LOG_CAP,
  appendSeries,
  mergeMetric,
  metricViolation,
  newEventId,
  pushHistory,
  reduceMetric,
  reduceTask,
  selectSeries,
  violationText,
  type MetricSeries,
  type PresenceInfo,
  type SeriesPoint,
  type SeriesRange,
  type EventLogEntry,
  type MetricState,
  type MetricPreference,
  type SitrepEvent,
  type Store,
  type AutomationDef,
  type TaskState,
} from "../store.ts";

interface Secrets {
  AUTH_TOKEN?: string;
  APNS_KEY_P8?: string;
  APNS_KEY_ID?: string;
  APNS_TEAM_ID?: string;
  APNS_BUNDLE_ID?: string; // var, wrangler.jsonc
  APNS_HOST?: string; // var, wrangler.jsonc
}

type WorkerEnv = Env & Secrets;

export class UserStore extends DurableObject<WorkerEnv> {
  private apnsConfig(): ApnsConfig | null {
    const e = this.env;
    if (!e.APNS_KEY_P8 || !e.APNS_KEY_ID || !e.APNS_TEAM_ID || !e.APNS_BUNDLE_ID) return null;
    return {
      keyP8: e.APNS_KEY_P8,
      keyId: e.APNS_KEY_ID,
      teamId: e.APNS_TEAM_ID,
      bundleId: e.APNS_BUNDLE_ID,
      host: e.APNS_HOST || "api.sandbox.push.apple.com",
    };
  }

  async apply(events: SitrepEvent[]): Promise<void> {
    for (const ev of events) {
      const prev = await this.ctx.storage.get<TaskState>(`task:${ev.source_id}`);
      const task = reduceTask(prev, ev);
      if (task) {
        await this.ctx.storage.put(`task:${ev.source_id}`, task);
        // A second task.start for the same source (e.g. the script renaming
        // an already-started task) must not spawn a second Live Activity.
        if (ev.kind !== "task.start" || prev === undefined) {
          this.ctx.waitUntil(this.pushForEvent(ev, task));
        }
      }
      if (ev.kind === "message.send" && ev.text) {
        const level = ev.level ?? "info";
        this.ctx.waitUntil(this.pushAlert(ev.text, level));
        await this.appendLog({
          id: newEventId(),
          text: ev.text,
          level,
          ts: ev.ts,
          source: ev.source_id,
        });
      }
      switch (ev.kind) {
        case "task.log":
          if (ev.text) {
            const prevLog = await this.ctx.storage.get<string[]>(`tasklog:${ev.source_id}`);
            await this.ctx.storage.put(`tasklog:${ev.source_id}`, appendTaskLog(prevLog, ev.text));
          }
          break;
      }
      const metric = reduceMetric(ev);
      if (metric) {
        const pref = await this.ctx.storage.get<MetricPreference>(`metric-pref:${metric.key}`);
        const effective = pref ? mergeMetric(metric, { [metric.key]: pref }) : metric;
        const prev = await this.ctx.storage.get<MetricState>(`metric:${effective.key}`);
        // Threshold edge: notify once when the value crosses an alert line;
        // returning inside the lines re-arms automatically (prev not violated
        // → next update past the line notifies again).
        const viol = metricViolation(effective);
        if (viol && (!prev || !metricViolation(prev))) {
          const level = pref?.level ?? "warn";
          if (level !== "off") {
            const text = violationText(effective, viol);
            this.ctx.waitUntil(this.pushAlert(text, level));
            await this.appendLog({ id: newEventId(), text, level, ts: ev.ts, source: ev.source_id });
          }
        }
        await this.ctx.storage.put(`metric:${effective.key}`, pushHistory(prev, effective));
        const series = appendSeries(
          await this.ctx.storage.get<MetricSeries>(`series:${effective.key}`),
          ev.ts,
          effective.value,
        );
        if (series) await this.ctx.storage.put(`series:${effective.key}`, series);
      }
    }
  }

  private async pushForEvent(ev: SitrepEvent, task: TaskState): Promise<void> {
    const cfg = this.apnsConfig();
    if (!cfg) return;
    try {
      switch (ev.kind) {
        case "task.start": {
          const devices = await this.ctx.storage.list<string>({ prefix: "ptst:" });
          const statuses = await Promise.all(
            [...devices.values()].map((token) => startActivity(cfg, token, task)),
          );
          console.log(`apns start ${ev.source_id}: [${statuses.join(",")}]`);
          break;
        }
        case "task.progress":
        case "task.step": {
          // Scripts may re-emit identical progress; identical content-state
          // would waste the device's update budget for nothing.
          const fingerprint = `${task.percent}|${task.step}|${task.status}`;
          const lastPushed = await this.ctx.storage.get<string>(`lastpush:${ev.source_id}`);
          if (fingerprint === lastPushed) break;
          const token = await this.ctx.storage.get<string>(`latoken:${ev.source_id}`);
          const status = token ? await updateActivity(cfg, token, task) : "no-token";
          if (token) await this.ctx.storage.put(`lastpush:${ev.source_id}`, fingerprint);
          console.log(`apns update ${ev.source_id} p=${task.percent}: ${status}`);
          break;
        }
        case "task.done":
        case "task.fail": {
          const token = await this.ctx.storage.get<string>(`latoken:${ev.source_id}`);
          const status = token ? await endActivity(cfg, token, task) : "no-token";
          console.log(`apns end ${ev.source_id}: ${status}`);
          break;
        }
      }
    } catch (e) {
      console.error("apns push failed", ev.kind, String(e));
    }
  }

  private async pushAlert(text: string, level: "info" | "warn" | "error"): Promise<void> {
    const cfg = this.apnsConfig();
    if (!cfg) return;
    try {
      const tokens = await this.ctx.storage.list<string>({ prefix: "alert:" });
      const statuses = await Promise.all(
        [...tokens.values()].map((t) => sendAlert(cfg, t, text, level)),
      );
      console.log(`apns alert(${level}) -> [${statuses.join(",")}]`);
    } catch (e) {
      console.error("apns alert failed", String(e));
    }
  }

  async registerAlertToken(deviceId: string, alertToken: string): Promise<void> {
    const existing = await this.ctx.storage.list<string>({ prefix: "alert:" });
    for (const [key, token] of existing) {
      if (token === alertToken && key !== `alert:${deviceId}`) {
        await this.ctx.storage.delete(key);
      }
    }
    await this.ctx.storage.put(`alert:${deviceId}`, alertToken);
  }

  async registerDevice(deviceId: string, pushToStartToken: string): Promise<void> {
    // The same physical device may re-register under a fresh device_id (e.g.
    // after a reinstall wipes its stored UUID). Dedupe by token value or the
    // device would receive every start push N times.
    const existing = await this.ctx.storage.list<string>({ prefix: "ptst:" });
    for (const [key, token] of existing) {
      if (token === pushToStartToken && key !== `ptst:${deviceId}`) {
        await this.ctx.storage.delete(key);
      }
    }
    await this.ctx.storage.put(`ptst:${deviceId}`, pushToStartToken);
  }

  // ---- device auth & pairing (docs/design/pairing-and-control.md) ----

  private async sha256(s: string): Promise<string> {
    const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
    return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
  }

  async resolveToken(token: string): Promise<{ deviceId: string; role: Role } | null> {
    const hash = await this.sha256(token);
    const deviceId = await this.ctx.storage.get<string>(`tokhash:${hash}`);
    if (!deviceId) return null;
    const dev = await this.ctx.storage.get<DeviceInfo>(`dev:${deviceId}`);
    if (!dev) return null;
    // last_seen updates are coarse (hourly) to keep writes cheap.
    const now = new Date().toISOString();
    if (!dev.last_seen || Date.parse(now) - Date.parse(dev.last_seen) > 3600_000) {
      await this.ctx.storage.put(`dev:${deviceId}`, { ...dev, last_seen: now });
    }
    return { deviceId, role: dev.role };
  }

  private spaceId(): Promise<string | undefined> {
    return this.ctx.storage.get<string>("space_id");
  }

  async initSpace(spaceId: string, ownerToken: string, platform: string, name: string): Promise<boolean> {
    if ((await this.spaceId()) !== undefined) return false;
    await this.ctx.storage.put("space_id", spaceId);
    const deviceId = crypto.randomUUID();
    const dev: DeviceInfo = {
      id: deviceId,
      name,
      role: "owner",
      platform,
      created_at: new Date().toISOString(),
    };
    await this.ctx.storage.put(`dev:${deviceId}`, dev);
    await this.ctx.storage.put(`tokhash:${await this.sha256(ownerToken)}`, deviceId);
    return true;
  }

  async createInvite(role: "viewer" | "source") {
    // Connect code: X + 14 random chars + Z — pure noise between two fixed
    // anchors so scanners can verify shape instantly. Confusable-free set.
    const alphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    const raw = crypto.getRandomValues(new Uint8Array(14));
    const code = "X" + [...raw].map((b) => alphabet[b % alphabet.length]).join("") + "Z";
    await this.ctx.storage.put(`invite:${code}`, { role, created_at: Date.now() });
    return { code, expires_in: 600 };
  }

  async join(spaceId: string, code: string, name: string, platform: string) {
    // Invite existence is the guard: only a trusted device could have minted
    // one in this DO, so bogus space ids fail here. (No spaceId check — the
    // legacy "default" space predates space_id records.)
    const invite = await this.ctx.storage.get<{ role: "viewer" | "source"; created_at: number }>(
      `invite:${code}`,
    );
    if (!invite || Date.now() - invite.created_at > 600_000) return null;
    await this.ctx.storage.delete(`invite:${code}`); // single-use
    const token = newToken(spaceId);
    const deviceId = crypto.randomUUID();
    const dev: DeviceInfo = {
      id: deviceId,
      name,
      role: invite.role,
      platform,
      created_at: new Date().toISOString(),
    };
    await this.ctx.storage.put(`dev:${deviceId}`, dev);
    await this.ctx.storage.put(`tokhash:${await this.sha256(token)}`, deviceId);
    return { token, device_id: deviceId, role: invite.role };
  }

  async devices(): Promise<DeviceInfo[]> {
    const m = await this.ctx.storage.list<DeviceInfo>({ prefix: "dev:" });
    return [...m.values()];
  }

  async revokeDevice(id: string): Promise<void> {
    await this.ctx.storage.delete(`dev:${id}`);
    // Token hash index entries for this device die lazily: resolveToken
    // fails on the missing dev record. Sweep the index anyway.
    const hashes = await this.ctx.storage.list<string>({ prefix: "tokhash:" });
    for (const [k, v] of hashes) {
      if (v === id) await this.ctx.storage.delete(k);
    }
  }

  // ---- reverse control ----

  async enqueueCommand(sourceId: string, action: Command): Promise<void> {
    const key = `cmd:${sourceId}`;
    const queue = (await this.ctx.storage.get<{ a: Command; ts: number }[]>(key)) ?? [];
    queue.push({ a: action, ts: Date.now() });
    await this.ctx.storage.put(key, queue);
  }

  async drainCommands(sourceIds: string[]): Promise<Record<string, Command[]>> {
    const out: Record<string, Command[]> = {};
    for (const id of sourceIds) {
      const key = `cmd:${id}`;
      const queue = await this.ctx.storage.get<{ a: Command; ts: number }[]>(key);
      if (!queue?.length) continue;
      // Stale commands (undelivered >60s) are dropped, not executed late.
      const fresh = queue.filter((q) => Date.now() - q.ts < 60_000).map((q) => q.a);
      if (fresh.length) out[id] = fresh;
      await this.ctx.storage.delete(key);
    }
    return out;
  }

  async registerActivityToken(sourceId: string, token: string): Promise<void> {
    await this.ctx.storage.put(`latoken:${sourceId}`, token);
    // Token registration races the first progress events; push the current
    // state immediately so the activity catches up.
    const cfg = this.apnsConfig();
    const task = await this.ctx.storage.get<TaskState>(`task:${sourceId}`);
    if (cfg && task) {
      this.ctx.waitUntil(
        (task.status === "running"
          ? updateActivity(cfg, token, task)
          : endActivity(cfg, token, task)
        ).then((s) => console.log(`apns catchup ${sourceId}: ${s}`)),
      );
    }
  }

  /** Debug snapshot: which push tokens exist (values truncated). */
  async debugTokens(full = false): Promise<Record<string, string>> {
    const out: Record<string, string> = {};
    for (const prefix of ["ptst:", "latoken:", "alert:"]) {
      const m = await this.ctx.storage.list<string>({ prefix });
      for (const [k, v] of m) out[k] = full ? v : `${v.slice(0, 8)}… (${v.length})`;
    }
    return out;
  }

  async tasks(): Promise<TaskState[]> {
    const m = await this.ctx.storage.list<TaskState>({ prefix: "task:" });
    return [...m.values()];
  }

  async metrics(): Promise<MetricState[]> {
    const m = await this.ctx.storage.list<MetricState>({ prefix: "metric:" });
    return [...m.values()];
  }

  async deleteTask(sourceId: string): Promise<void> {
    await this.ctx.storage.delete(`task:${sourceId}`);
    await this.ctx.storage.delete(`latoken:${sourceId}`);
    await this.ctx.storage.delete(`lastpush:${sourceId}`);
    await this.ctx.storage.delete(`tasklog:${sourceId}`);
  }

  async taskLog(sourceId: string): Promise<string[]> {
    return (await this.ctx.storage.get<string[]>(`tasklog:${sourceId}`)) ?? [];
  }

  async deleteMetric(key: string): Promise<void> {
    await this.ctx.storage.delete(`metric:${key}`);
    await this.ctx.storage.delete(`series:${key}`);
  }

  async metricSeries(key: string, range: SeriesRange): Promise<SeriesPoint[]> {
    return selectSeries(await this.ctx.storage.get<MetricSeries>(`series:${key}`), range);
  }

  async stampPresence(kind: "ingest" | "agent", ts: string): Promise<void> {
    await this.ctx.storage.put(`presence:${kind}`, ts);
  }

  async presence(): Promise<PresenceInfo> {
    return {
      ingest_last_seen: await this.ctx.storage.get<string>("presence:ingest"),
      agent_last_seen: await this.ctx.storage.get<string>("presence:agent"),
    };
  }

  async deleteEvents(ids: string[] | "all"): Promise<void> {
    if (ids === "all") {
      await this.ctx.storage.delete("evlog");
      return;
    }
    const log = (await this.ctx.storage.get<EventLogEntry[]>("evlog")) ?? [];
    await this.ctx.storage.put("evlog", log.filter((e) => !e.id || !ids.includes(e.id)));
  }

  async automations(): Promise<AutomationDef[]> {
    const m = await this.ctx.storage.list<AutomationDef>({ prefix: "automation:" });
    return [...m.values()];
  }

  async putAutomation(automation: AutomationDef): Promise<void> {
    await this.ctx.storage.put(`automation:${automation.id}`, automation);
  }

  async patchAutomation(
    id: string,
    patch: Partial<Pick<AutomationDef, "name" | "every_s" | "enabled" | "last_run" | "run_requested_at">>,
  ): Promise<AutomationDef | undefined> {
    const automation = await this.ctx.storage.get<AutomationDef>(`automation:${id}`);
    if (!automation) return undefined;
    const next = { ...automation, ...patch, updated_at: new Date().toISOString() };
    await this.ctx.storage.put(`automation:${id}`, next);
    return next;
  }

  async deleteAutomation(id: string): Promise<void> {
    await this.ctx.storage.delete(`automation:${id}`);
  }

  private async appendLog(entry: EventLogEntry): Promise<void> {
    const log = (await this.ctx.storage.get<EventLogEntry[]>("evlog")) ?? [];
    log.unshift(entry);
    if (log.length > EVENT_LOG_CAP) log.length = EVENT_LOG_CAP;
    await this.ctx.storage.put("evlog", log);
  }

  async eventLog(): Promise<EventLogEntry[]> {
    return (await this.ctx.storage.get<EventLogEntry[]>("evlog")) ?? [];
  }

  async metricPrefs(): Promise<Record<string, MetricPreference>> {
    const m = await this.ctx.storage.list<MetricPreference>({ prefix: "metric-pref:" });
    return Object.fromEntries([...m.entries()].map(([k, v]) => [k.slice("metric-pref:".length), v]));
  }

  async setMetricPref(metricId: string, pref: MetricPreference | null): Promise<void> {
    if (pref === null) await this.ctx.storage.delete(`metric-pref:${metricId}`);
    else await this.ctx.storage.put(`metric-pref:${metricId}`, pref);
  }
}

function stub(env: WorkerEnv, space: string) {
  return env.USER_STORE.getByName(space);
}

function doStore(env: WorkerEnv, space: string): Store {
  const s = stub(env, space);
  return {
    apply: (events) => s.apply(events),
    tasks: () => s.tasks(),
    metrics: () => s.metrics(),
    taskLog: (id) => s.taskLog(id),
    deleteTask: (id) => s.deleteTask(id),
    deleteMetric: (key) => s.deleteMetric(key),
    eventLog: () => s.eventLog(),
    metricPrefs: () => s.metricPrefs(),
    setMetricPref: (metricId, pref) => s.setMetricPref(metricId, pref),
    automations: () => s.automations(),
    putAutomation: (automation) => s.putAutomation(automation),
    patchAutomation: (id, patch) => s.patchAutomation(id, patch),
    deleteAutomation: (id) => s.deleteAutomation(id),
    metricSeries: (key, range) => s.metricSeries(key, range),
    stampPresence: (kind, ts) => s.stampPresence(kind, ts),
    presence: () => s.presence(),
    deleteEvents: (ids) => s.deleteEvents(ids),
  };
}

function doRegistry(env: WorkerEnv, space: string): SpaceRegistry {
  const s = stub(env, space);
  return {
    initSpace: (id, tok, platform, name) => s.initSpace(id, tok, platform, name),
    resolveToken: (token) => s.resolveToken(token),
    createInvite: (role) => s.createInvite(role),
    join: (space, code, name, platform) => s.join(space, code, name, platform),
    devices: () => s.devices(),
    revokeDevice: (id) => s.revokeDevice(id),
    registerDevice: (id, token) => s.registerDevice(id, token),
    registerAlertToken: (id, token) => s.registerAlertToken(id, token),
    registerActivityToken: (sourceId, token) => s.registerActivityToken(sourceId, token),
    enqueueCommand: (sourceId, action) => s.enqueueCommand(sourceId, action),
    drainCommands: (sourceIds) => s.drainCommands(sourceIds),
  };
}

const app = createApp({
  store: (c, space) => doStore(c.env as WorkerEnv, space),
  registry: (c, space) => doRegistry(c.env as WorkerEnv, space),
  defer: (c, work) => c.executionCtx.waitUntil(work),
  authToken: (c) => (c.env as WorkerEnv).AUTH_TOKEN,
  publishInvite: async (c, code, space) => {
    await (c.env as WorkerEnv).INVITE_DIR.put(code, space, { expirationTtl: 600 });
  },
  lookupInvite: (c, code) => (c.env as WorkerEnv).INVITE_DIR.get(code),
});

app.get("/debug/tokens", async (c: any) => {
  const env = c.env as WorkerEnv;
  const auth = c.req.header("authorization");
  if (!env.AUTH_TOKEN || auth !== `Bearer ${env.AUTH_TOKEN}`) {
    return c.json({ error: "unauthorized" }, 401);
  }
  return c.json(await stub(env, "default").debugTokens(c.req.query("full") === "1"));
});

// ---- /v3: realtime WebSocket + its HTTP control-plane companion ----
//
// Both routes ride the SAME `/v3/*` `authenticate` middleware registered in
// app.ts (st2 token parsing, unchanged from /v2) so an invalid or
// unresolvable token gets its 401 from that shared, already-tested path —
// this handler runs at all ONLY once a token has resolved to a real
// device/role, and it is the only place SpaceHub is ever touched.

function spaceHubStub(env: WorkerEnv, space: string) {
  return env.SPACE_HUB.getByName(space);
}

app.get("/v3/realtime", async (c: any) => {
  const env = c.env as WorkerEnv;
  const raw = c.req.raw as Request;
  if ((raw.headers.get("upgrade") ?? "").toLowerCase() !== "websocket") {
    return c.text("expected websocket upgrade", 426);
  }
  // authenticate() already ran (see app.use("/v3/*", authenticate) in
  // app.ts): an invalid/unresolvable token never reaches this line, and no
  // Durable Object of any kind is touched for it beyond the UserStore
  // lookup authenticate() itself performs — SpaceHub is instantiated only
  // by the single stub.fetch() call below, on the success path.
  const deviceId: string | undefined = c.get("deviceId");
  const role = c.get("role") as Role;
  if (!deviceId) {
    // The legacy bare-admin token (self-host, no registry) resolves to a
    // role but no device identity; the realtime protocol requires one.
    return c.json({ error: "realtime requires a paired device token" }, 401);
  }
  const realtimeRole: "source" | "viewer" = role === "source" ? "source" : "viewer";

  const headers = new Headers(raw.headers);
  headers.set("x-sitrep-device-id", deviceId);
  headers.set("x-sitrep-role", realtimeRole);
  const forwardReq = new Request(raw.url, { method: raw.method, headers });

  // Exactly one forwarded operation per inbound upgrade request: the DO's
  // own fetch() creates the WebSocketPair and calls ctx.acceptWebSocket
  // once (see SpaceHub#fetch).
  return spaceHubStub(env, c.get("space")).fetch(forwardReq);
});

function parseAutomationUpsert(body: any): { name: string; executor_kind: AutomationExecutorKind; every_seconds: number } | null {
  const kind = body?.executor_kind;
  const everySeconds = Number(body?.schedule?.every_seconds);
  if (!body?.name || !["script", "agent", "hybrid"].includes(kind) || !Number.isFinite(everySeconds)) return null;
  return { name: String(body.name).slice(0, 256), executor_kind: kind, every_seconds: Math.max(1, everySeconds | 0) };
}

// /v3/automations role matrix (protocol-owner ruling, aligned with the
// existing product decision "watcher schedules are editable from every
// device; creation only from trusted devices" — same split /v2 makes
// between canEmit-guarded POST and canView-guarded PATCH/DELETE):
//   POST   (create/upsert) -> owner/admin only
//   PATCH  (pause/resume, schedule.every_seconds) -> owner/admin/viewer
//          (viewer editing the schedule is INTENTIONAL, not an oversight)
//   DELETE -> owner/admin/viewer (aligned with /v2 canView's delete)
// A conflict result from mintConfigEvent (same Idempotency-Key, different
// operation content) maps to 409.

function mintResultResponse(c: any, result: { revision: number; automation: AutomationState | null; conflict?: boolean }) {
  if (result.conflict) {
    return c.json({ error: "idempotency key was already used for a different operation" }, 409);
  }
  return c.json({ revision: result.revision, automation: result.automation });
}

/** Deterministic automation_id for a POST that omitted one: SHA-256 of the
 * Idempotency-Key, hex, truncated to 32 chars (fits the 1-128 char id cap
 * and stays collision-safe for this cardinality). Same key -> same id, so
 * a retried create replays instead of fingerprint-conflicting. */
async function deriveAutomationId(idempotencyKey: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(idempotencyKey));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 32);
}

const internalError = (c: any, e: unknown) => {
  console.log(JSON.stringify({ level: "error", event: "v3_http_unhandled", message: String(e) }));
  return c.json({ error: "internal error" }, 500);
};

app.post("/v3/automations", async (c: any) => {
  const role = c.get("role") as Role;
  if (!["admin", "owner"].includes(role)) return c.json({ error: "forbidden" }, 403);
  const body = await c.req.json().catch(() => null);
  const parsed = parseAutomationUpsert(body);
  if (!parsed) return c.json({ error: "name, executor_kind and schedule.every_seconds required" }, 400);
  const idempotencyKey = c.req.header("idempotency-key") ?? null;
  // The idempotency fingerprint must cover only CLIENT-provided content: a
  // server-generated random id would enter the fingerprint and make an
  // honest retry of a dropped POST (same key, no explicit automation_id)
  // fingerprint-mismatch into a spurious 409. When the client omits
  // automation_id but supplies an Idempotency-Key, derive the id
  // deterministically from that key, so the retry reconstructs the exact
  // same automation (including its id) and replays cleanly.
  const automationId =
    typeof body.automation_id === "string" && body.automation_id
      ? body.automation_id
      : idempotencyKey
        ? await deriveAutomationId(idempotencyKey)
        : crypto.randomUUID();
  const automation: AutomationState = {
    automation_id: automationId,
    name: parsed.name,
    executor_kind: parsed.executor_kind,
    schedule: { kind: "interval", every_seconds: parsed.every_seconds },
    state: body.state === "paused" ? "paused" : "active",
  };
  try {
    const stub = spaceHubStub(c.env as WorkerEnv, c.get("space"));
    const result = await stub.mintConfigEvent(idempotencyKey, {
      kind: "automation.upserted",
      automation_id: automation.automation_id,
      automation,
    });
    return mintResultResponse(c, result);
  } catch (e) {
    return internalError(c, e);
  }
});

app.patch("/v3/automations/:id", async (c: any) => {
  const role = c.get("role") as Role;
  if (!["admin", "owner", "viewer"].includes(role)) return c.json({ error: "forbidden" }, 403);
  try {
    const stub = spaceHubStub(c.env as WorkerEnv, c.get("space"));
    const id = c.req.param("id");
    const existing = (await stub.automationsSnapshot()).find((a: AutomationState) => a.automation_id === id);
    if (!existing) return c.json({ error: "not found" }, 404);
    const body = await c.req.json().catch(() => null);
    const next: AutomationState = {
      ...existing,
      ...(body?.state === "active" || body?.state === "paused" ? { state: body.state } : {}),
      ...(body?.schedule?.every_seconds !== undefined
        ? { schedule: { kind: "interval" as const, every_seconds: Math.max(1, Number(body.schedule.every_seconds) | 0) } }
        : {}),
    };
    const idempotencyKey = c.req.header("idempotency-key") ?? null;
    const result = await stub.mintConfigEvent(idempotencyKey, { kind: "automation.upserted", automation_id: id, automation: next });
    return mintResultResponse(c, result);
  } catch (e) {
    return internalError(c, e);
  }
});

app.delete("/v3/automations/:id", async (c: any) => {
  const role = c.get("role") as Role;
  if (!["admin", "owner", "viewer"].includes(role)) return c.json({ error: "forbidden" }, 403);
  try {
    const stub = spaceHubStub(c.env as WorkerEnv, c.get("space"));
    const id = c.req.param("id");
    // 404 before minting: deleting a nonexistent automation must not burn a
    // revision on a no-op config.event (same existence-check pattern PATCH
    // uses).
    const existing = (await stub.automationsSnapshot()).find((a: AutomationState) => a.automation_id === id);
    if (!existing) return c.json({ error: "not found" }, 404);
    const idempotencyKey = c.req.header("idempotency-key") ?? null;
    const result = await stub.mintConfigEvent(idempotencyKey, { kind: "automation.removed", automation_id: id });
    return mintResultResponse(c, result);
  } catch (e) {
    return internalError(c, e);
  }
});

export default app;
