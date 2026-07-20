// SpaceHub: the ONE Durable Object per Sitrep space (docs/design/
// v1-architecture.md §0/§1). It is the single authority for a space's
// business state (DeviceRegistry + StateStore + PushOutbox + CommandStore,
// §1.1-§1.4) AND the realtime WS transport (proto/realtime/SPEC.md,
// unchanged). Every /v1 route — WS or HTTP — resolves to exactly one
// SpaceHub instance and touches no other Durable Object class.
//
// Concurrency note read before editing (carried over from the
// realtime-integration porting base): every handler that must behave as
// "one durable transaction" is written as a sequence of synchronous
// `this.ctx.storage.sql.exec` calls with NO `await` between them — DO
// storage coalesces consecutive synchronous writes, and nothing else can
// interleave into a running synchronous function. `ensureAlarm()` is
// deliberately called WITHOUT awaiting it from inside these methods
// (fire-and-forget, per v1-apns-outbox.md §2) so it never becomes an
// `await` in the middle of a transaction.

import { DurableObject } from "cloudflare:workers";
import {
  activityEndAps,
  activityUpdateAps,
  alertAps,
  backoffMs,
  buildApnsRequest,
  classifyApnsResponse,
  MAX_TRANSIENT_ATTEMPTS,
  pushToStartAps,
  type ApnsConfig,
  type PushTaskView,
} from "../apns.ts";
import type { WorkerEnv } from "../env.ts";
import { appendSeries, appendTaskLog, emptySeries, selectSeries, type MetricSeries, type SeriesPoint } from "../series.ts";
import {
  AMBIGUOUS_DISPATCH_GRACE_MS,
  CONNECT_CODE_ALPHABET,
  CONNECT_CODE_SECRET_LEN,
  decodeConnectCode,
  METRIC_CACHE_MAX_METRICS,
  METRIC_FRAME_RATE_PER_SEC_PER_DEVICE,
  METRICS_CURRENT_DOWNSAMPLE_MS,
  parseTransportFlag,
  PUSH_OUTBOX_DEVICE_ROW_CAP,
  PUSH_OUTBOX_SPACE_ROW_CAP,
  type Capabilities,
  type DeviceRole as V1DeviceRole,
  type EventResultStatus,
  type MessageLevel,
  type MetricSeriesPoint,
  type PendingCommand,
  type Presence,
  type PushKind,
  type SeriesRange,
  type Snapshot,
  type TaskCommandAction,
} from "../v1/contract/types.ts";
import { type ConnAttachment, isConnAttachment } from "./attachment.ts";
import { chunkDeltaEvents, chunkSnapshot } from "./chunking.ts";
import { authorizeClientEnvelope, parseEnvelope } from "./guards.ts";
import {
  ERROR_SEMANTICS,
  HEARTBEAT_INTERVAL_MS,
  LEASE_DEFAULT_MS,
  EVENT_LOG_RETENTION_REVISIONS,
  MESSAGE_WINDOW,
  PROTOCOL_VERSION,
  SUPPORTED_PROTOCOL_VERSIONS,
  makeEnvelope,
  makeStandardError,
  newEnvelopeId,
} from "./protocol.ts";
import {
  rowToAutomationState,
  rowToDeltaEventItem,
  rowToMessageRecord,
  rowToTaskState,
  type AutomationRow,
  type EventLogRow,
  type MessageRow,
  type TaskRow,
} from "./rows.ts";
import type {
  AnyEnvelope,
  AutomationState,
  CommandBody,
  ConfigEventBody,
  ConfigEventKind,
  DeltaBody,
  DisplayHints,
  ErrorCode,
  HelloOfferBody,
  InterestRenewBody,
  MessageEventBody,
  MessageType,
  MetricFrameBody,
  MetricSample,
  ResumeBody,
  SubscribeBody,
  TaskEventBody,
} from "./types.ts";

/** Deterministic JSON with recursively sorted object keys — used for
 * idempotency fingerprints (automations) and nowhere else. */
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`);
    return `{${entries.join(",")}}`;
  }
  return JSON.stringify(value);
}

async function sha256Hex(s: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Per-threshold alert edge-state (v1-architecture.md §1.2.0, P0-2). Persisted
 * in metrics_current.alert_state so a `fired` threshold stays fired across a DO
 * rebuild — the next sample past the line does not re-fire a duplicate alert. */
type ThresholdEdge = "armed" | "fired";
type AlertState = Partial<Record<"above" | "below", ThresholdEdge>>;

/** The persisted current state of one metric (metrics_current row), plus its
 * in-memory hot-cache representation. */
interface CurrentMetric {
  value: string;
  fields: MetricSample;
  alertState: AlertState;
}

/** Recomputes per-threshold edge-state from a new sample and the metric's
 * previously-persisted `alert_state`. Returns the next state and, if a
 * threshold transitioned `armed → fired` on this sample, the crossing to alert
 * on (above takes priority, matching the old single-direction behavior). A
 * threshold already `fired` does NOT re-fire until the value returns inside the
 * line (`armed`) — this is the persisted-edge-state duplicate-alert fix. */
function computeAlertEdges(sample: MetricSample, prev: AlertState): { nextState: AlertState; fired?: { line: string; dir: "above" | "below" } } {
  const v = Number(sample.value);
  const next: AlertState = {};
  let fired: { line: string; dir: "above" | "below" } | undefined;
  for (const dir of ["above", "below"] as const) {
    const line = dir === "above" ? sample.alert_above : sample.alert_below;
    if (line === undefined) continue; // no threshold configured for this direction
    const ln = Number(line);
    const violated = Number.isFinite(v) && Number.isFinite(ln) && (dir === "above" ? v > ln : v < ln);
    if (violated) {
      next[dir] = "fired";
      if (prev[dir] !== "fired") fired = fired ?? { line, dir }; // armed/undefined → fired edge
    } else {
      next[dir] = "armed";
    }
  }
  return { nextState: next, fired };
}

export interface DeviceInfo {
  id: string;
  name: string;
  role: "owner" | "viewer" | "source";
  platform?: string;
  created_at: string;
  last_seen?: string;
}

interface PushOutboxSqlRow extends Record<string, SqlStorageValue> {
  push_id: string;
  kind: PushKind;
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

const ALERT_TITLES: Record<MessageLevel, string> = { info: "Sitrep", warn: "Sitrep Warning", error: "Sitrep Error" };

/** The command actions representable in the HTTP `PendingCommand` shape
 * (v1-architecture.md §4.1) — all carry a `task_id`. `run_now` (which
 * carries `automation_id`, not `task_id`) is deliberately NOT here, so the
 * HTTP piggyback leaves it for the WS drain rather than consuming it. */
const TASK_COMMAND_ACTIONS: ReadonlySet<string> = new Set(["pause", "resume", "stop"]);

/** How far into the future to push an eligible pending row's
 * `next_attempt_at` when APNS delivery is paused (v1-apns-outbox.md §8.2),
 * so the alarm re-arms to a LATER re-check instead of busy-looping. */
const DELIVERY_PAUSED_RECHECK_MS = 60_000;

export class SpaceHub extends DurableObject<WorkerEnv> {
  private static readonly SCHEMA_VERSION = 1;

  /** In-memory HOT CACHE of the persistent `metrics_current` table
   * (v1-architecture.md §1.2.0, P0-2). No longer the source of truth: every
   * accepted metric.frame UPSERTs metrics_current, and this cache is
   * rehydrated read-through from it (losing it on eviction costs at most a
   * lazy re-read, never data). LRU-capped at METRIC_CACHE_MAX_METRICS. */
  private metricsCurrentCache = new Map<string, CurrentMetric>();

  /** P0-7 downsample buffer (v1-architecture.md §1.2.0): routine (non-edge-
   * transition) metric.frame samples are coalesced here — last-value-wins —
   * instead of UPSERTing metrics_current on every sample. Flushed as one
   * batch by the shared ensureAlarm()/alarm() machinery, at most once per
   * METRICS_CURRENT_DOWNSAMPLE_MS. An alert edge transition (armed<->fired)
   * always bypasses this and persists synchronously/immediately instead —
   * see applyMetricFrame. Purely a write-timing detail: readMetricCurrent
   * always reads the hot cache first, which IS updated on every sample
   * regardless of this buffer, so reads are never stale because of it. */
  private pendingMetricsFlush = new Map<string, CurrentMetric>();
  /** When the buffered flush above is next due (ms epoch), or null when
   * nothing is buffered. Set once when the FIRST routine update lands after
   * the previous flush (or DO start); NOT pushed later by subsequent
   * buffered updates within the same window, so a busy metric never delays
   * its own flush past METRICS_CURRENT_DOWNSAMPLE_MS from its first buffered
   * sample. */
  private metricsFlushDueAt: number | null = null;

  /** Per-device metric.frame rate limiter (v1-architecture.md §4.3): shared
   * by the WS path and POST /v1/events, replacing the old per-WebSocket
   * WeakMap limiter so it survives hibernation. */
  private perDeviceMetricRate = new Map<string, number[]>();

  private hotPathCounter = 0;
  private ranMigrationDdl = false;

  logSampler: (n: number) => boolean = (n) => n % 100 === 0;
  private securityEventLog: Array<{ event: string; data: Record<string, unknown> }> = [];

  /** Injectable network boundary for APNs dispatch, mirroring the
   * `logSampler` pattern above: production defaults to a real `fetch`, and
   * tests override this field directly via `runInDurableObject` so no
   * outbound network call ever needs to happen in a test run. */
  apnsFetch: (req: Request) => Promise<Response> = (req) => fetch(req);

  constructor(ctx: DurableObjectState, env: WorkerEnv) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    this.migrate();
  }

  private migrate(): void {
    if (this.schemaVersion() >= SpaceHub.SCHEMA_VERSION) return;
    this.ranMigrationDdl = true;
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS _schema_migrations (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        version INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS space_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      -- DeviceRegistry (v1-architecture.md §1.1)
      CREATE TABLE IF NOT EXISTS devices (
        device_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        platform TEXT,
        created_at TEXT NOT NULL,
        last_seen TEXT
      );
      CREATE TABLE IF NOT EXISTS token_hashes (
        token_hash TEXT PRIMARY KEY,
        device_id TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_token_hashes_device ON token_hashes(device_id);
      -- P0-6 self-routing connect code (v1-architecture.md section 10.5):
      -- space_id is now implicit (this row lives in the SpaceHub instance
      -- that IS that space), so the PK moves from the old opaque code to
      -- the one-time secret embedded in the code's tail.
      CREATE TABLE IF NOT EXISTS invites (
        secret TEXT PRIMARY KEY,
        role TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS push_tokens (
        device_id TEXT PRIMARY KEY,
        push_to_start_token TEXT,
        alert_token TEXT,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS activity_tokens (
        task_id TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        token TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );

      -- StateStore (v1-architecture.md §1.2)
      CREATE TABLE IF NOT EXISTS event_log (
        revision INTEGER PRIMARY KEY,
        event_type TEXT NOT NULL,
        device_id TEXT,
        device_seq INTEGER,
        occurred_at INTEGER NOT NULL,
        payload TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_event_log_device ON event_log(device_id, device_seq);
      CREATE TABLE IF NOT EXISTS dedup (
        device_id TEXT NOT NULL,
        device_seq INTEGER NOT NULL,
        revision INTEGER NOT NULL,
        PRIMARY KEY (device_id, device_seq)
      );
      CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        title TEXT,
        state TEXT NOT NULL,
        percent INTEGER,
        step TEXT,
        message TEXT,
        updated_at INTEGER NOT NULL,
        display TEXT,
        generation INTEGER NOT NULL DEFAULT 1,
        -- device_id of the source running this task (P0-3): the first
        -- started's body.device_id, reset on each new-generation start.
        -- Reverse-control commands are directed to this device, not broadcast.
        owning_device_id TEXT
      );
      CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        level TEXT NOT NULL,
        text TEXT NOT NULL,
        occurred_at INTEGER NOT NULL,
        revision INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_messages_revision ON messages(revision);
      CREATE TABLE IF NOT EXISTS automations (
        automation_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        executor_kind TEXT NOT NULL,
        every_seconds INTEGER NOT NULL,
        state TEXT NOT NULL,
        last_run_at INTEGER,
        -- monotonic run counter (P0-4): incremented by POST .../run; the agent
        -- runs once when it advances beyond its last-consumed value.
        run_request_id INTEGER NOT NULL DEFAULT 0,
        run_requested_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS leases (
        device_id TEXT PRIMARY KEY,
        expires_at INTEGER NOT NULL,
        topics TEXT NOT NULL
      );
      -- Persistent current metric value + per-threshold alert edge-state
      -- (v1-architecture.md §1.2.0, P0-2). Authoritative: GET /v1/snapshot and
      -- GET /v1/metrics/:id read this, so a rebuilt DO keeps the last accepted
      -- value AND a fired threshold stays fired (no duplicate alert). The
      -- in-memory Map is a hot cache rehydrated from here. Not revisioned.
      CREATE TABLE IF NOT EXISTS metrics_current (
        metric_id TEXT PRIMARY KEY,
        value TEXT NOT NULL,          -- last folded value (proto metric value string)
        fields TEXT NOT NULL,         -- JSON MetricSample: last folded label/display/target/min/max/alert lines/ts
        alert_state TEXT NOT NULL,    -- JSON: per-threshold edge-state, e.g. {"above":"fired","below":"armed"}
        updated_at INTEGER NOT NULL
      );
      -- Persistent tiered metric history (v1-architecture.md §1.2.1);
      -- non-folded, never bumps space_revision, survives DO eviction.
      CREATE TABLE IF NOT EXISTS metric_series (
        metric_id TEXT NOT NULL,
        tier TEXT NOT NULL,           -- 'raw' | 'hour' | 'day'
        points TEXT NOT NULL,         -- JSON array of {t: unix_ms, v: number}, oldest first
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (metric_id, tier)
      );
      -- Persistent per-task log tail (v1-architecture.md §1.2.2); non-folded,
      -- source-uplinked via POST /v1/tasks/:id/log, survives DO eviction.
      CREATE TABLE IF NOT EXISTS task_logs (
        task_id TEXT PRIMARY KEY,
        lines TEXT NOT NULL,          -- JSON array of strings, oldest first, capped TASK_LOG_WINDOW
        updated_at INTEGER NOT NULL
      );

      -- PushOutbox (v1-apns-outbox.md §3, frozen column set)
      CREATE TABLE IF NOT EXISTS push_outbox (
        push_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        device_id TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        generation INTEGER,
        revision INTEGER NOT NULL,
        coalesce_key TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL,
        dispatch_started_at INTEGER,
        last_error TEXT,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        terminal_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_push_outbox_dispatch ON push_outbox(status, next_attempt_at);
      CREATE INDEX IF NOT EXISTS idx_push_outbox_device ON push_outbox(device_id, status);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_push_outbox_start_dedup
        ON push_outbox(device_id, subject_id, generation) WHERE kind = 'push_to_start';
      CREATE UNIQUE INDEX IF NOT EXISTS idx_push_outbox_update_coalesce
        ON push_outbox(coalesce_key) WHERE kind = 'activity_update' AND status = 'pending';

      -- CommandStore (v1-architecture.md §1.4)
      CREATE TABLE IF NOT EXISTS pending_commands (
        command_id TEXT PRIMARY KEY,
        target_device_id TEXT,
        origin_ts INTEGER NOT NULL,
        ttl_ms INTEGER NOT NULL,
        payload TEXT NOT NULL,
        delivered INTEGER NOT NULL DEFAULT 0
      );

      -- shared idempotency ledger (v1-architecture.md §1.5)
      CREATE TABLE IF NOT EXISTS http_idempotency (
        idempotency_key TEXT PRIMARY KEY,
        fingerprint TEXT NOT NULL,
        revision INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      );
    `);
    this.ctx.storage.sql.exec(
      `INSERT INTO _schema_migrations (id, version) VALUES (1, ?)
       ON CONFLICT(id) DO UPDATE SET version = excluded.version`,
      SpaceHub.SCHEMA_VERSION,
    );
  }

  private schemaVersion(): number {
    try {
      const row = this.ctx.storage.sql.exec<{ version: number }>("SELECT version FROM _schema_migrations WHERE id = 1").toArray()[0];
      return row?.version ?? 0;
    } catch (e) {
      if (String(e).includes("no such table")) return 0;
      throw e;
    }
  }

  // =========================================================================
  // DeviceRegistry (v1-architecture.md §1.1) — control-plane RPC surface
  // =========================================================================

  /** Creates the space + its owner device. Returns the owner device's
   * `device_id` (P0-1) so the response can carry it — `device_seq` is scoped
   * to `(device_id, space)`, so the creating Mac must know its own id to
   * uplink events. Returns null if the space already exists (id collision). */
  async initSpace(spaceId: string, ownerToken: string, platform: string, name: string): Promise<string | null> {
    const already = this.ctx.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key = 'space_initialized'").toArray()[0];
    if (already) return null;
    const deviceId = crypto.randomUUID();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(`INSERT INTO devices (device_id, name, role, platform, created_at) VALUES (?, ?, 'owner', ?, ?)`, deviceId, name, platform, now);
    this.ctx.storage.sql.exec(`INSERT INTO token_hashes (token_hash, device_id) VALUES (?, ?)`, await sha256Hex(ownerToken), deviceId);
    this.ctx.storage.sql.exec(`INSERT INTO space_meta (key, value) VALUES ('space_initialized', '1'), ('space_id', ?)`, spaceId);
    return deviceId;
  }

  async resolveToken(token: string): Promise<{ deviceId: string; role: V1DeviceRole } | null> {
    const hash = await sha256Hex(token);
    const row = this.ctx.storage.sql
      .exec<{ device_id: string }>("SELECT device_id FROM token_hashes WHERE token_hash = ?", hash)
      .toArray()[0];
    if (!row) return null;
    const dev = this.ctx.storage.sql.exec<{ role: string; last_seen: string | null }>("SELECT role, last_seen FROM devices WHERE device_id = ?", row.device_id).toArray()[0];
    if (!dev) return null;
    const now = new Date().toISOString();
    if (!dev.last_seen || Date.parse(now) - Date.parse(dev.last_seen) > 3600_000) {
      this.ctx.storage.sql.exec(`UPDATE devices SET last_seen = ? WHERE device_id = ?`, now, row.device_id);
    }
    // Anomaly self-heal on the universal HTTP entry point: every
    // authenticated /v1 request resolves its token here first, so folding
    // the check in re-arms a stranded outbox row on ANY request — including
    // a pure read that enqueues nothing — with no extra RPC round-trip
    // (v1-apns-outbox.md §2/§7).
    await this.ensureAlarmIfPending();
    return { deviceId: row.device_id, role: dev.role as V1DeviceRole };
  }

  /** Null if this SpaceHub instance was never initialized (no POST
   * /v1/spaces ever created it) — e.g. a `join`/`createInvite` call routed
   * here via a `space` value that names no real space (a malicious or
   * stale guess at a space_id). Callers must treat null as "no such space"
   * rather than throwing, since `env.SPACE_HUB.getByName(...)` happily
   * resolves a stub for ANY name, initialized or not. */
  private ownSpaceId(): string | null {
    return this.ctx.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key = 'space_id'").toArray()[0]?.value ?? null;
  }

  /** P0-6 self-routing (v1-architecture.md §10.5): mints the FULL 21-char
   * layout — 'X' + this SpaceHub's own space_id (already known — it's the
   * space answering this request), uppercased, + a freshly random 9-char
   * secret + 'Z'. The invites row is keyed by the (lowercase, canonical)
   * secret only — space_id is implicit in which SpaceHub instance holds the
   * row, so there is nothing left to look up via KV. */
  async createInvite(role: "viewer" | "source"): Promise<{ code: string; expires_in: number }> {
    // Always initialized by this point: reaching here required an already-
    // authenticated device token, which itself requires initSpace() to have
    // already run (see resolveToken/token_hashes) — unlike join(), which is
    // unauthenticated and can be routed at an uninitialized space.
    const spaceId = this.ownSpaceId()!;
    const raw = crypto.getRandomValues(new Uint8Array(CONNECT_CODE_SECRET_LEN));
    const secret = [...raw].map((b) => CONNECT_CODE_ALPHABET[b % CONNECT_CODE_ALPHABET.length]).join("");
    const code = "X" + spaceId.toUpperCase() + secret.toUpperCase() + "Z";
    this.ctx.storage.sql.exec(`INSERT INTO invites (secret, role, created_at) VALUES (?, ?, ?)`, secret, role, Date.now());
    return { code, expires_in: 600 };
  }

  /** `token` is minted by the caller (Worker layer, via the same `newToken`
   * contract helper `POST /v1/spaces` uses) and passed in as a plain
   * string — a DO RPC argument must be structured-cloneable, so the token
   * MINTING function itself cannot cross this boundary, only its result.
   *
   * P0-6 self-routing (v1-architecture.md §10.5): the Worker has already
   * routed to THIS SpaceHub directly from the request's `space` field, with
   * zero KV lookup (app.ts). This method re-decodes `code` itself as a
   * defense-in-depth structural check, in routing/validation order:
   *   1. malformed shape (wrong length/anchors/alphabet) -> 400
   *   2. code's embedded space_id doesn't match THIS space -> 400
   *   3. secret not found / expired / already redeemed in this SpaceHub's
   *      own DeviceRegistry.invites -> 404
   */
  async join(
    code: string,
    name: string,
    platform: string,
    token: string,
  ): Promise<
    | { ok: true; deviceId: string; role: string }
    | { ok: false; status: 400; error: "malformed code" }
    | { ok: false; status: 400; error: "code does not match space" }
    | { ok: false; status: 404; error: "invite invalid or expired" }
  > {
    const decoded = decodeConnectCode(code);
    if (!decoded) return { ok: false, status: 400, error: "malformed code" };
    // ownSpaceId() is null when `space` named no real, initialized space
    // (env.SPACE_HUB.getByName resolves a stub for ANY name) — decoded.space_id
    // can never equal null, so this correctly falls into the same
    // "does not match" branch rather than throwing.
    if (decoded.space_id !== this.ownSpaceId()) return { ok: false, status: 400, error: "code does not match space" };

    const invite = this.ctx.storage.sql.exec<{ role: string; created_at: number }>("SELECT role, created_at FROM invites WHERE secret = ?", decoded.secret).toArray()[0];
    if (!invite || Date.now() - invite.created_at > 600_000) return { ok: false, status: 404, error: "invite invalid or expired" };
    this.ctx.storage.sql.exec(`DELETE FROM invites WHERE secret = ?`, decoded.secret); // single-use
    const deviceId = crypto.randomUUID();
    const now = new Date().toISOString();
    this.ctx.storage.sql.exec(`INSERT INTO devices (device_id, name, role, platform, created_at) VALUES (?, ?, ?, ?, ?)`, deviceId, name, invite.role, platform, now);
    this.ctx.storage.sql.exec(`INSERT INTO token_hashes (token_hash, device_id) VALUES (?, ?)`, await sha256Hex(token), deviceId);
    return { ok: true, deviceId, role: invite.role };
  }

  async devices(): Promise<DeviceInfo[]> {
    const rows = this.ctx.storage.sql
      .exec<{ device_id: string; name: string; role: string; platform: string | null; created_at: string; last_seen: string | null }>(
        "SELECT * FROM devices ORDER BY created_at",
      )
      .toArray();
    return rows.map((r) => ({
      id: r.device_id,
      name: r.name,
      role: r.role as DeviceInfo["role"],
      ...(r.platform ? { platform: r.platform } : {}),
      created_at: r.created_at,
      ...(r.last_seen ? { last_seen: r.last_seen } : {}),
    }));
  }

  /** Revokes a device AND force-closes every live WS for it in the same
   * operation (v1-architecture.md §10.2, owner ruling B4) — a revoked
   * device must not keep streaming on an already-open socket. */
  async revokeDevice(id: string): Promise<void> {
    this.ctx.storage.sql.exec(`DELETE FROM devices WHERE device_id = ?`, id);
    this.ctx.storage.sql.exec(`DELETE FROM token_hashes WHERE device_id = ?`, id);
    this.ctx.storage.sql.exec(`DELETE FROM push_tokens WHERE device_id = ?`, id);
    this.ctx.storage.sql.exec(`DELETE FROM activity_tokens WHERE device_id = ?`, id);
    for (const ws of this.ctx.getWebSockets(id)) {
      try {
        this.send(ws, "error", { code: "unauthenticated", message: "device credential was revoked", ...ERROR_SEMANTICS.unauthenticated });
      } catch {
        // socket may already be gone
      }
      try {
        ws.close(1008, "revoked");
      } catch {
        // already closed
      }
    }
    this.logAlways("device_revoked", { device_id: id });
  }

  async registerPushTokens(deviceId: string, tokens: { push_to_start_token?: string; alert_token?: string }): Promise<void> {
    // Dedup by token VALUE across other devices (the same physical device
    // may re-register under a fresh device_id, e.g. after a reinstall) so
    // one physical device never receives a push twice under two identities.
    if (tokens.push_to_start_token) {
      this.ctx.storage.sql.exec(`UPDATE push_tokens SET push_to_start_token = NULL WHERE push_to_start_token = ? AND device_id != ?`, tokens.push_to_start_token, deviceId);
    }
    if (tokens.alert_token) {
      this.ctx.storage.sql.exec(`UPDATE push_tokens SET alert_token = NULL WHERE alert_token = ? AND device_id != ?`, tokens.alert_token, deviceId);
    }
    const now = Date.now();
    this.ctx.storage.sql.exec(
      `INSERT INTO push_tokens (device_id, push_to_start_token, alert_token, updated_at) VALUES (?, ?, ?, ?)
       ON CONFLICT(device_id) DO UPDATE SET
         push_to_start_token = COALESCE(excluded.push_to_start_token, push_tokens.push_to_start_token),
         alert_token = COALESCE(excluded.alert_token, push_tokens.alert_token),
         updated_at = excluded.updated_at`,
      deviceId,
      tokens.push_to_start_token ?? null,
      tokens.alert_token ?? null,
      now,
    );
  }

  /** Registers the per-activity Live Activity token for one task. One row
   * per task_id (last-write-wins across devices — v1-architecture.md
   * §1.1's documented, not-fixed-in-v1 limitation). Immediately enqueues a
   * catch-up push (activity_update if running, activity_end if already
   * terminal) so the newly-registered activity isn't stuck showing nothing
   * until the next state change — mirrors the pre-v1 registerActivityToken
   * catch-up behavior. */
  async registerActivityToken(taskId: string, deviceId: string, token: string): Promise<void> {
    this.ctx.storage.sql.exec(
      `INSERT INTO activity_tokens (task_id, device_id, token, updated_at) VALUES (?, ?, ?, ?)
       ON CONFLICT(task_id) DO UPDATE SET device_id = excluded.device_id, token = excluded.token, updated_at = excluded.updated_at`,
      taskId,
      deviceId,
      token,
      Date.now(),
    );
    const task = this.ctx.storage.sql.exec<TaskRow>("SELECT * FROM tasks WHERE task_id = ?", taskId).toArray()[0];
    if (task) {
      const view = this.taskRowToPushView(task);
      const revision = this.getRevision();
      if (task.state === "running") this.enqueueActivityUpdate(deviceId, taskId, task.generation, revision, view);
      else this.enqueueActivityEnd(deviceId, taskId, task.generation, revision, view);
      await this.ensureAlarm();
    }
  }

  async stampAgentSeen(): Promise<void> {
    this.stampSpaceMetaNow("agent_last_seen");
  }

  // =========================================================================
  // WebSocket entry point (proto/realtime/SPEC.md, unchanged)
  // =========================================================================

  async fetch(request: Request): Promise<Response> {
    if ((request.headers.get("upgrade") ?? "").toLowerCase() !== "websocket") {
      return new Response("expected websocket upgrade", { status: 426 });
    }
    // Trust boundary: these headers are set by the Worker AFTER it already
    // validated the sr1 token (adapters/workers.ts) — never derived from
    // anything the client sent inside the WebSocket connection itself.
    // `x-sitrep-role` now carries the token's HTTP role (owner/viewer/source),
    // NOT a pre-mapped WS role: the WS role is client-declared in the hello
    // offer and constrained by this token role in handlePreHello (P0-1).
    const deviceId = request.headers.get("x-sitrep-device-id");
    const tokenRole = request.headers.get("x-sitrep-role");
    if (!deviceId || (tokenRole !== "owner" && tokenRole !== "viewer" && tokenRole !== "source")) {
      return new Response("missing trusted identity", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server, [deviceId]);
    const attachment: ConnAttachment = {
      deviceId,
      // Provisional WS role until the hello offer declares it (constrained by
      // tokenRole). A viewer-capable token defaults to viewer pre-hello; a
      // source-only token to source. Overwritten in handlePreHello.
      role: tokenRole === "source" ? "source" : "viewer",
      tokenRole,
      connectedAt: Date.now(),
      helloDone: false,
      subscribedThisConn: false,
      deltaEligible: false,
    };
    server.serializeAttachment(attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    // Anomaly self-heal (v1-apns-outbox.md §2/§7) — re-arm a stranded outbox
    // row before this frame's own work. The await is before the synchronous
    // dispatch transaction, so it does not break dispatchMessage's atomicity.
    await this.ensureAlarmIfPending();
    try {
      this.dispatchMessage(ws, message);
    } catch (e) {
      this.logAlways("unhandled_error", { message: String(e), stack: (e as Error)?.stack });
      try {
        this.reply(ws, "internal_error", "unexpected server error", undefined);
      } catch {
        // socket may be broken
      }
    }
    // Re-arm the outbox alarm AFTER the frame's work, with an awaited
    // lifecycle (5c): any push_outbox row the frame just enqueued gets its
    // alarm scheduled within this (async) invocation, never on abandoned
    // fire-and-forget async work. The sync dispatch transaction above is
    // already committed, so this await breaks no atomicity.
    await this.ensureAlarm();
  }

  private dispatchMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    if (typeof message !== "string") return;
    if (message === "ping" || message === "pong") return;

    const raw = ws.deserializeAttachment();
    if (!isConnAttachment(raw)) {
      ws.close(1011, "missing connection identity");
      return;
    }
    const att = raw;

    if (!att.helloDone) {
      this.handlePreHello(ws, att, message);
      return;
    }

    const parsed = parseEnvelope(message);
    if (parsed.kind === "unknown_type") return;
    if (parsed.kind === "error") {
      this.reply(ws, parsed.code, parsed.message, undefined);
      return;
    }
    const envelope = parsed.envelope;

    if (envelope.type === "hello") {
      this.reply(ws, "hello_required", "stage:accept is server-only", envelope.id);
      ws.close(1008, "hello_required");
      return;
    }

    const authz = authorizeClientEnvelope(att.role, envelope);
    if (!authz.ok) {
      this.reply(ws, authz.code, `role ${att.role} may not send ${envelope.type}`, envelope.id);
      if (ERROR_SEMANTICS[authz.code].fatal) ws.close(1008, authz.code);
      return;
    }

    this.logHotPath("frame", { type: envelope.type, device_id: att.deviceId });

    switch (envelope.type) {
      case "resume":
        this.handleResume(ws, att, envelope.body as ResumeBody, envelope.id);
        break;
      case "subscribe":
        this.handleSubscribe(ws, att, envelope.body as SubscribeBody, envelope.id);
        break;
      case "unsubscribe":
        this.handleUnsubscribe(ws, att, envelope.id);
        break;
      case "interest.renew":
        this.handleInterestRenew(ws, att, envelope.body as InterestRenewBody, envelope.id);
        break;
      case "task.event": {
        const body = envelope.body as TaskEventBody;
        if (body.device_id !== att.deviceId) {
          this.reply(ws, "unauthorized", "device_id does not match the authenticated identity", envelope.id);
          break;
        }
        this.applyTaskEvent(body);
        this.send(ws, "ack", { acked: [{ device_id: body.device_id, device_seq: body.device_seq }] });
        break;
      }
      case "message.event": {
        const body = envelope.body as MessageEventBody;
        if (body.device_id !== att.deviceId) {
          this.reply(ws, "unauthorized", "device_id does not match the authenticated identity", envelope.id);
          break;
        }
        this.applyMessageEvent(body);
        this.send(ws, "ack", { acked: [{ device_id: body.device_id, device_seq: body.device_seq }] });
        break;
      }
      case "metric.frame": {
        const body = envelope.body as MetricFrameBody;
        if (body.device_id !== att.deviceId) {
          this.reply(ws, "unauthorized", "device_id does not match the authenticated identity", envelope.id);
          break;
        }
        const result = this.ingestMetricFrame(body);
        if (result.status === "rejected") {
          this.reply(ws, "rate_limited", "metric.frame exceeded 10/s for this device", envelope.id);
        }
        break;
      }
      case "command":
        this.handleCommand(ws, att, envelope.body as CommandBody, envelope.ts, envelope.id);
        break;
      case "ack":
      case "error":
        break;
      default:
        this.reply(ws, "malformed", `unexpected type ${envelope.type}`, envelope.id);
    }
  }

  async webSocketClose(): Promise<void> {
    // Interest leases and the event log are keyed by device/space, not by
    // connection — nothing to clean up on close beyond letting the socket go.
  }

  async webSocketError(_ws: WebSocket, error: unknown): Promise<void> {
    this.logAlways("ws_error", { message: String(error) });
  }

  // ---- hello / handshake ----

  private handlePreHello(ws: WebSocket, att: ConnAttachment, message: string): void {
    const parsed = parseEnvelope(message);
    const offerBody =
      parsed.kind === "ok" && parsed.envelope.type === "hello" && (parsed.envelope.body as HelloOfferBody).stage === "offer"
        ? (parsed.envelope.body as HelloOfferBody)
        : null;
    if (!offerBody) {
      this.reply(ws, "hello_required", "hello{stage:offer} must be the first frame", undefined);
      ws.close(1008, "hello_required");
      return;
    }

    // WS role is client-declared and token-constrained (P0-1): a source-token
    // may present only `source`, a viewer-token only `viewer`, an owner-token
    // EITHER. A device presenting a role its token does not permit is rejected
    // at the handshake. The declared+validated role becomes this connection's
    // WS role for all downstream authorization.
    const declaredRole = offerBody.role;
    const permitted = att.tokenRole === "owner" ? declaredRole === "source" || declaredRole === "viewer" : declaredRole === att.tokenRole;
    if (!permitted) {
      this.reply(ws, "unauthorized", `token role ${att.tokenRole} may not present WS role ${declaredRole}`, parsed.kind === "ok" ? parsed.envelope.id : undefined);
      ws.close(1008, "unauthorized");
      return;
    }
    att.role = declaredRole;

    const intersection = offerBody.protocol_versions.filter((v) => SUPPORTED_PROTOCOL_VERSIONS.includes(v));
    if (intersection.length === 0) {
      this.reply(ws, "version_unsupported", "no shared protocol version", parsed.kind === "ok" ? parsed.envelope.id : undefined);
      ws.close(1008, "version_unsupported");
      return;
    }
    const negotiated = Math.max(...intersection);

    const newSessionId = crypto.randomUUID();
    for (const other of this.ctx.getWebSockets(att.deviceId)) {
      if (other === ws) continue;
      const otherAtt = other.deserializeAttachment();
      if (isConnAttachment(otherAtt) && otherAtt.helloDone) {
        this.logAlways("superseded", {
          device_id: att.deviceId,
          role: att.role,
          superseded_session_id: otherAtt.sessionId,
          superseding_session_id: newSessionId,
        });
        this.send(other, "error", { code: "superseded", message: "device completed hello on a newer connection", ...ERROR_SEMANTICS.superseded });
        other.close(1008, "superseded");
      }
    }

    att.helloDone = true;
    att.sessionId = newSessionId;
    ws.serializeAttachment(att);

    this.send(ws, "hello", {
      stage: "accept",
      protocol_version: negotiated,
      session_id: att.sessionId,
      heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
    });

    if (att.role === "source") {
      this.sendCurrentRateState(ws);
      this.drainPendingCommands(ws, att.deviceId);
    }
    this.reconcileLeaseEdge();
  }

  // ---- resume / snapshot / delta ----

  private handleResume(ws: WebSocket, att: ConnAttachment, body: ResumeBody, envelopeId: string): void {
    if (!att.subscribedThisConn) {
      this.reply(ws, "malformed", "resume must follow subscribe on the same connection", envelopeId);
      return;
    }
    const N = body.last_revision;
    const R = this.getRevision();

    if (N > R) {
      this.reply(ws, "revision_unavailable", `last_revision ${N} exceeds current revision ${R}`, envelopeId);
      return;
    }

    if (N === 0) {
      this.sendSnapshotChunks(ws, R);
    } else if (N === R) {
      this.send(ws, "delta", { from_revision: N, to_revision: N, events: [] });
    } else {
      const minRetained = this.minRetainedRevision();
      if (minRetained !== null && minRetained <= N + 1) {
        this.sendChainedDeltas(ws, N, this.eventLogRange(N, R));
      } else {
        this.sendSnapshotChunks(ws, R);
      }
    }

    att.deltaEligible = true;
    ws.serializeAttachment(att);
  }

  private sendSnapshotChunks(ws: WebSocket, revision: number): void {
    const arrays = this.buildSnapshotArrays();
    const chunks = chunkSnapshot(revision, arrays);
    for (const chunk of chunks) this.send(ws, "snapshot", chunk);
  }

  private sendChainedDeltas(ws: WebSocket, fromRevision: number, rows: EventLogRow[]): void {
    const events = rows.map(rowToDeltaEventItem);
    const deltas = chunkDeltaEvents(fromRevision, events);
    for (const delta of deltas) this.send(ws, "delta", delta);
  }

  private buildSnapshotArrays() {
    const tasks = this.ctx.storage.sql.exec<TaskRow>("SELECT * FROM tasks ORDER BY task_id").toArray().map(rowToTaskState);
    const automations = this.ctx.storage.sql
      .exec<AutomationRow>("SELECT * FROM automations ORDER BY automation_id")
      .toArray()
      .map(rowToAutomationState);
    const messageRows = this.ctx.storage.sql
      .exec<MessageRow>("SELECT * FROM messages ORDER BY revision DESC LIMIT ?", MESSAGE_WINDOW)
      .toArray();
    messageRows.reverse();
    const messages = messageRows.map(rowToMessageRecord);
    const metrics = this.allCurrentMetrics();
    return { tasks, metrics, messages, automations };
  }

  /** Metrics come from the persistent metrics_current table (P0-2), so a
   * rebuilt DO still serves every last-accepted value — not a volatile Map
   * alone. Overlaid with the in-memory hot cache (P0-7): a routine sample
   * updates the cache on every accept, but its SQL persistence may lag up to
   * METRICS_CURRENT_DOWNSAMPLE_MS behind (the debounce) — the cache is
   * always at least as fresh as the persisted row for any metric_id it still
   * holds, so GET /v1/snapshot never shows stale data purely because of the
   * write-timing optimization. */
  private allCurrentMetrics(): MetricSample[] {
    const byId = new Map<string, MetricSample>();
    for (const r of this.ctx.storage.sql.exec<{ metric_id: string; fields: string }>("SELECT metric_id, fields FROM metrics_current").toArray()) {
      byId.set(r.metric_id, JSON.parse(r.fields) as MetricSample);
    }
    for (const [metricId, cur] of this.metricsCurrentCache) byId.set(metricId, cur.fields);
    return [...byId.values()].sort((a, b) => (a.metric_id < b.metric_id ? -1 : a.metric_id > b.metric_id ? 1 : 0));
  }

  private minRetainedRevision(): number | null {
    const row = this.ctx.storage.sql.exec<{ m: number | null }>("SELECT MIN(revision) as m FROM event_log").toArray()[0];
    return row?.m ?? null;
  }

  private eventLogRange(fromExclusive: number, toInclusive: number): EventLogRow[] {
    return this.ctx.storage.sql
      .exec<EventLogRow>("SELECT * FROM event_log WHERE revision > ? AND revision <= ? ORDER BY revision", fromExclusive, toInclusive)
      .toArray();
  }

  // =========================================================================
  // Shared ingest — the ONE path both GET /v1/realtime and POST /v1/events
  // call (v1-architecture.md §4). No second reducer anywhere in this class.
  // =========================================================================

  /** Called by the WS task.event handler AND (via the Worker's RPC call)
   * POST /v1/events — v1-architecture.md §4. The caller has already parsed,
   * authorized (role=source), and verified body.device_id against the
   * authenticated identity; this method is the actual state transition. */
  applyTaskEvent(body: TaskEventBody): { revision: number; duplicate: boolean } {
    this.stampSpaceMetaNow("ingest_last_seen");
    const existing = this.ctx.storage.sql
      .exec<{ revision: number }>("SELECT revision FROM dedup WHERE device_id = ? AND device_seq = ?", body.device_id, body.device_seq)
      .toArray()[0];
    if (existing) return { revision: existing.revision, duplicate: true };

    const revision = this.getRevision() + 1;
    const payload = JSON.stringify(body);
    this.ctx.storage.sql.exec(
      `INSERT INTO event_log (revision, event_type, device_id, device_seq, occurred_at, payload) VALUES (?, 'task.event', ?, ?, ?, ?)`,
      revision,
      body.device_id,
      body.device_seq,
      body.occurred_at,
      payload,
    );
    this.ctx.storage.sql.exec(`INSERT INTO dedup (device_id, device_seq, revision) VALUES (?, ?, ?)`, body.device_id, body.device_seq, revision);
    const { generation, isNewGeneration, view } = this.foldTaskEvent(body);
    this.setRevision(revision);
    this.pruneEventLog(revision);

    if (isNewGeneration) {
      this.enqueuePushToStartRows(body.task_id, generation, revision, view);
    } else if (body.kind === "progress" || body.kind === "step") {
      const registrant = this.activityRegistrant(body.task_id);
      if (registrant) this.enqueueActivityUpdate(registrant.device_id, body.task_id, generation, revision, view);
    } else if (body.kind === "done" || body.kind === "failed") {
      // A task reaching done/failed fires ONLY activity_end — never an extra
      // alert push (v1-apns-outbox.md §4.3, owner ruling). The only thing
      // that produces a user-facing alert is a script explicitly emitting a
      // message.event (or a metric threshold edge); auto-notify-on-failure
      // is deferred to v1.1.
      const registrant = this.activityRegistrant(body.task_id);
      if (registrant) this.enqueueActivityEnd(registrant.device_id, body.task_id, generation, revision, view);
    }

    this.broadcastDelta({ from_revision: revision - 1, to_revision: revision, events: [{ event_type: "task.event", event: body }] });
    this.reconcileLeaseEdge();

    return { revision, duplicate: false };
  }

  applyMessageEvent(body: MessageEventBody): { revision: number; duplicate: boolean } {
    this.stampSpaceMetaNow("ingest_last_seen");
    const existing = this.ctx.storage.sql
      .exec<{ revision: number }>("SELECT revision FROM dedup WHERE device_id = ? AND device_seq = ?", body.device_id, body.device_seq)
      .toArray()[0];
    if (existing) return { revision: existing.revision, duplicate: true };

    const revision = this.getRevision() + 1;
    const payload = JSON.stringify(body);
    this.ctx.storage.sql.exec(
      `INSERT INTO event_log (revision, event_type, device_id, device_seq, occurred_at, payload) VALUES (?, 'message.event', ?, ?, ?, ?)`,
      revision,
      body.device_id,
      body.device_seq,
      body.occurred_at,
      payload,
    );
    this.ctx.storage.sql.exec(`INSERT INTO dedup (device_id, device_seq, revision) VALUES (?, ?, ?)`, body.device_id, body.device_seq, revision);
    this.foldMessageEvent(body, revision);
    this.setRevision(revision);
    this.pruneEventLog(revision);

    const priority: 5 | 10 = body.level === "error" ? 10 : 5;
    this.enqueueAlertBroadcast(body.message_id, ALERT_TITLES[body.level], body.text, body.level, priority, revision);

    this.broadcastDelta({ from_revision: revision - 1, to_revision: revision, events: [{ event_type: "message.event", event: body }] });
    this.reconcileLeaseEdge();

    return { revision, duplicate: false };
  }

  /** metric.frame: best-effort, never persisted, never revisioned
   * (v1-architecture.md §1.2/§4.2). Rate limiting happens in the caller
   * (checkMetricRateLimit) so both transports report the outcome the same
   * way (WS: `error{rate_limited}`; HTTP: `results[i].status:"rejected"`). */
  applyMetricFrame(deviceId: string, metrics: MetricSample[]): { accepted: MetricSample[]; staleCount: number } {
    this.stampSpaceMetaNow("ingest_last_seen");
    const accepted: MetricSample[] = [];
    let staleCount = 0;
    for (const sample of metrics) {
      // Staleness + edge-state read from the PERSISTENT metrics_current
      // (P0-2), not volatile memory — so a rebuilt DO doesn't lose the last
      // ts or re-arm an already-fired threshold.
      const current = this.readMetricCurrent(sample.metric_id);
      if (current && sample.ts <= current.fields.ts) {
        staleCount++;
        continue;
      }
      const { nextState, fired } = computeAlertEdges(sample, current?.alertState ?? {});
      if (fired) {
        const label = sample.label ?? sample.metric_id;
        const text =
          fired.dir === "above"
            ? `${label} ${sample.value}, above alert line ${fired.line}`
            : `${label} ${sample.value}, below alert line ${fired.line}`;
        this.enqueueAlertBroadcast(sample.metric_id, ALERT_TITLES.warn, text, "warn", 5, this.getRevision());
      }
      // Edge-detection is immediate and unconditional (P0-7): the hot cache
      // reflects this recomputed row on EVERY accepted sample, regardless of
      // whether the SQL persistence below is deferred. This is what keeps
      // readMetricCurrent (and therefore GET /v1/metrics/:id, and the next
      // sample's own edge computation) always fresh even under the
      // downsample.
      const cur: CurrentMetric = { value: sample.value, fields: sample, alertState: nextState };
      this.cacheMetricCurrent(sample.metric_id, cur);
      const edgeChanged = JSON.stringify(current?.alertState ?? {}) !== JSON.stringify(nextState);
      // `current === null` means readMetricCurrent found this metric_id in
      // NEITHER the hot cache NOR metrics_current itself — i.e. this is the
      // first sample ever accepted for this metric_id (existence is checked
      // read-through, so it survives a DO eviction that dropped the cache).
      // Existence must never be deferred behind the debounce (protocol-owner
      // ruling): a first-ever sample bypasses the buffer exactly like an
      // alert edge transition, so GET /v1/metrics/:id never 404s for a
      // metric that was genuinely reported, even if the DO is evicted inside
      // the 10s window right after.
      const isFirstEverSample = current === null;
      if (edgeChanged || isFirstEverSample) {
        // Alert edge transition (armed->fired or fired->cleared), or this
        // metric_id's first-ever persist: write immediately, no debounce, in
        // the same transaction as this sample's acceptance (P0-7) — the
        // alert-firing/edge-state invariant, and the existence invariant,
        // must never lag, even by the downsample window below.
        this.upsertMetricsCurrentRow(sample.metric_id, cur);
        this.pendingMetricsFlush.delete(sample.metric_id); // supersedes any buffered routine update
      } else {
        // Routine sample: coalesce into the per-space debounce buffer
        // instead of writing metrics_current on every sample.
        this.scheduleMetricsCurrentFlush(sample.metric_id, cur);
      }
      // metric_series append is unaffected by the downsample above — it
      // remains a best-effort append on every accepted sample (§1.2.1).
      this.appendMetricSeries(sample.metric_id, sample.ts, Number(sample.value));
      accepted.push(sample);
    }
    if (accepted.length > 0) this.broadcastMetricFrame({ device_id: deviceId, metrics: accepted });
    return { accepted, staleCount };
  }

  /** The ONE metric.frame entry point both transports call — WS's
   * dispatchMessage and (via RPC) POST /v1/events's per-item handling
   * (v1-architecture.md §4/§4.3). Combines the shared per-device rate limit
   * with the shared apply/broadcast logic so there is exactly one place
   * that decides "applied" vs "stale" vs "rejected". */
  ingestMetricFrame(body: MetricFrameBody): { status: EventResultStatus; error?: { code: string; message: string } } {
    if (!this.checkMetricRateLimit(body.device_id)) {
      return { status: "rejected", error: { code: "rate_limited", message: "metric.frame exceeded 10/s for this device" } };
    }
    const { accepted, staleCount } = this.applyMetricFrame(body.device_id, body.metrics);
    if (accepted.length > 0) return { status: "applied" };
    if (staleCount > 0) return { status: "stale" };
    return { status: "applied" };
  }

  /** Per-device sliding-window limiter (v1-architecture.md §4.3): 10
   * metric.frame envelopes / second, shared by WS and HTTP. Returns false
   * (caller must reject) when the device is over the cap. */
  private checkMetricRateLimit(deviceId: string): boolean {
    const now = Date.now();
    const recent = (this.perDeviceMetricRate.get(deviceId) ?? []).filter((t) => now - t < 1000);
    if (recent.length >= METRIC_FRAME_RATE_PER_SEC_PER_DEVICE) {
      this.perDeviceMetricRate.set(deviceId, recent);
      return false;
    }
    recent.push(now);
    this.perDeviceMetricRate.set(deviceId, recent);
    return true;
  }

  /** Read-through of metrics_current (P0-2): hot cache first, else the DB row
   * (rehydrating the cache), else null (metric genuinely never reported — the
   * authoritative 404 for GET /v1/metrics/:id). */
  private readMetricCurrent(metricId: string): CurrentMetric | null {
    const cached = this.metricsCurrentCache.get(metricId);
    if (cached) {
      // LRU touch.
      this.metricsCurrentCache.delete(metricId);
      this.metricsCurrentCache.set(metricId, cached);
      return cached;
    }
    const row = this.ctx.storage.sql
      .exec<{ value: string; fields: string; alert_state: string }>("SELECT value, fields, alert_state FROM metrics_current WHERE metric_id = ?", metricId)
      .toArray()[0];
    if (!row) return null;
    const cur: CurrentMetric = { value: row.value, fields: JSON.parse(row.fields) as MetricSample, alertState: JSON.parse(row.alert_state) as AlertState };
    this.cacheMetricCurrent(metricId, cur);
    return cur;
  }

  /** UPSERTs ONE metrics_current row (P0-7: the only place that writes this
   * table — called either immediately for an alert edge transition, or in a
   * batch from flushMetricsCurrent for coalesced routine updates). Evicts
   * the least-recently-updated row first if this INSERT would introduce a
   * NEW metric_id beyond the shared METRIC_CACHE_MAX_METRICS cap — the same
   * cap number the in-memory hot cache uses, so there is exactly one
   * cardinality limit, not two independently-drifting ones. An UPDATE (the
   * metric_id already has a row) never evicts anything; only a genuinely new
   * row can push the table over the cap. */
  private upsertMetricsCurrentRow(metricId: string, cur: CurrentMetric): void {
    const exists = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM metrics_current WHERE metric_id = ?", metricId).toArray()[0].n > 0;
    if (!exists) {
      const count = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM metrics_current").toArray()[0].n;
      if (count >= METRIC_CACHE_MAX_METRICS) {
        this.ctx.storage.sql.exec(`DELETE FROM metrics_current WHERE metric_id = (SELECT metric_id FROM metrics_current ORDER BY updated_at ASC LIMIT 1)`);
      }
    }
    this.ctx.storage.sql.exec(
      `INSERT INTO metrics_current (metric_id, value, fields, alert_state, updated_at) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(metric_id) DO UPDATE SET value = excluded.value, fields = excluded.fields, alert_state = excluded.alert_state, updated_at = excluded.updated_at`,
      metricId,
      cur.value,
      JSON.stringify(cur.fields),
      JSON.stringify(cur.alertState),
      Date.now(),
    );
  }

  private cacheMetricCurrent(metricId: string, cur: CurrentMetric): void {
    this.metricsCurrentCache.delete(metricId);
    if (this.metricsCurrentCache.size >= METRIC_CACHE_MAX_METRICS) {
      const oldest = this.metricsCurrentCache.keys().next().value;
      if (oldest !== undefined) this.metricsCurrentCache.delete(oldest);
    }
    this.metricsCurrentCache.set(metricId, cur);
  }

  /** Buffers ONE routine (non-edge-transition) metrics_current update
   * (P0-7) — last-value-wins within the debounce window, synchronous, no
   * SQL write here. `metricsFlushDueAt` is set only when this is the FIRST
   * buffered update since the previous flush, so ensureAlarm() (called by
   * the caller's own post-batch re-arm, same as the push_outbox alarm) picks
   * it up as the next wake time. */
  private scheduleMetricsCurrentFlush(metricId: string, cur: CurrentMetric): void {
    if (this.pendingMetricsFlush.size === 0) {
      this.metricsFlushDueAt = Date.now() + METRICS_CURRENT_DOWNSAMPLE_MS;
    }
    this.pendingMetricsFlush.set(metricId, cur);
  }

  /** Flushes every buffered routine update as one batch (P0-7) — called from
   * the shared alarm() handler. A metric_id with a single routine update and
   * no follow-up sample is still flushed here within the window, never left
   * unpersisted indefinitely. */
  private flushMetricsCurrent(): void {
    if (this.pendingMetricsFlush.size === 0) return;
    for (const [metricId, cur] of this.pendingMetricsFlush) {
      this.upsertMetricsCurrentRow(metricId, cur);
    }
    this.pendingMetricsFlush.clear();
    this.metricsFlushDueAt = null;
  }

  /** Best-effort append into the PERSISTENT metric_series table
   * (v1-architecture.md §1.2.1). Reads this metric's three tier rows, folds
   * the sample via the pure appendSeries, and writes the affected tiers
   * back. CRITICAL: never bumps space_revision, emits no delta — this is
   * derived history on the non-folded metric.frame path, same discipline as
   * the presence markers (the whole method issues only metric_series writes,
   * never a setRevision/broadcastDelta). Non-finite values are dropped by
   * appendSeries itself. Survives DO eviction because it's SQLite, not the
   * old in-memory cache. */
  private appendMetricSeries(metricId: string, ts: number, value: number): void {
    if (!Number.isFinite(value)) return;
    const prev = this.readMetricSeries(metricId);
    const next = appendSeries(prev, ts, value);
    const now = Date.now();
    for (const tier of ["raw", "hour", "day"] as const) {
      this.ctx.storage.sql.exec(
        `INSERT INTO metric_series (metric_id, tier, points, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT(metric_id, tier) DO UPDATE SET points = excluded.points, updated_at = excluded.updated_at`,
        metricId,
        tier,
        JSON.stringify(next[tier]),
        now,
      );
    }
  }

  private readMetricSeries(metricId: string): MetricSeries {
    const rows = this.ctx.storage.sql.exec<{ tier: string; points: string }>("SELECT tier, points FROM metric_series WHERE metric_id = ?", metricId).toArray();
    if (rows.length === 0) return emptySeries();
    const series = emptySeries();
    for (const row of rows) {
      if (row.tier === "raw" || row.tier === "hour" || row.tier === "day") {
        series[row.tier] = JSON.parse(row.points) as SeriesPoint[];
      }
    }
    return series;
  }

  private broadcastMetricFrame(body: MetricFrameBody): void {
    const now = Date.now();
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (!isConnAttachment(att) || att.role !== "viewer" || !att.helloDone) continue;
      if (!this.deviceHasMetricInterest(att.deviceId, now)) continue;
      this.send(ws, "metric.frame", body);
    }
  }

  private broadcastDelta(delta: DeltaBody): void {
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (!isConnAttachment(att) || att.role !== "viewer" || !att.deltaEligible) continue;
      if (!this.deviceHasActiveLease(att.deviceId)) continue;
      this.send(ws, "delta", delta);
    }
  }

  // ---- task.event folding + generation tracking (v1-architecture.md §1.2) ----

  private foldTaskEvent(body: TaskEventBody): { generation: number; isNewGeneration: boolean; view: PushTaskView } {
    const prev = this.ctx.storage.sql.exec<TaskRow>("SELECT * FROM tasks WHERE task_id = ?", body.task_id).toArray()[0];

    const state = body.kind === "done" ? "done" : body.kind === "failed" ? "failed" : "running";
    const title = body.title && body.title.length > 0 ? body.title : (prev?.title ?? null);
    let percent = prev?.percent ?? null;
    if (state === "running" && body.percent !== undefined) percent = body.percent;
    else if (body.kind === "done") percent = 100;
    const step = body.kind === "done" || body.kind === "failed" ? null : (body.step ?? prev?.step ?? null);
    const message = body.kind === "done" || body.kind === "failed" ? (body.message ?? null) : null;
    const display = body.display ? JSON.stringify(body.display) : (prev?.display ?? null);

    const isAbsentOrTerminal = !prev || prev.state === "done" || prev.state === "failed";
    const isNewGeneration = body.kind === "started" && isAbsentOrTerminal;
    const generation = isNewGeneration ? (prev?.generation ?? 0) + 1 : (prev?.generation ?? 1);

    // owning_device_id (P0-3): the first `started`'s device on each new
    // generation; preserved for every other event so a directed command
    // always reaches the device actually running the task. A re-`started`
    // within the same generation does not change it.
    const owningDeviceId = isNewGeneration ? body.device_id : (prev?.owning_device_id ?? null);

    this.ctx.storage.sql.exec(
      `INSERT INTO tasks (task_id, device_id, title, state, percent, step, message, updated_at, display, generation, owning_device_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(task_id) DO UPDATE SET
         device_id = excluded.device_id, title = excluded.title, state = excluded.state,
         percent = excluded.percent, step = excluded.step, message = excluded.message,
         updated_at = excluded.updated_at, display = excluded.display, generation = excluded.generation,
         owning_device_id = excluded.owning_device_id`,
      body.task_id,
      body.device_id,
      title,
      state,
      percent,
      step,
      message,
      body.occurred_at,
      display,
      generation,
      owningDeviceId,
    );

    const parsedDisplay: DisplayHints | undefined = display ? JSON.parse(display) : undefined;
    return {
      generation,
      isNewGeneration,
      view: {
        task_id: body.task_id,
        ...(title !== null ? { title } : {}),
        state: state as "running" | "done" | "failed",
        ...(percent !== null ? { percent } : {}),
        ...(step !== null ? { step } : {}),
        ...(message !== null ? { message } : {}),
        ...(parsedDisplay ? { display: parsedDisplay } : {}),
        started_at_epoch_s: isNewGeneration ? Math.floor(body.occurred_at / 1000) : null,
      },
    };
  }

  private taskRowToPushView(row: TaskRow): PushTaskView {
    return {
      task_id: row.task_id,
      ...(row.title !== null ? { title: row.title } : {}),
      state: row.state as "running" | "done" | "failed",
      ...(row.percent !== null ? { percent: row.percent } : {}),
      ...(row.step !== null ? { step: row.step } : {}),
      ...(row.message !== null ? { message: row.message } : {}),
      ...(row.display !== null ? { display: JSON.parse(row.display) } : {}),
    };
  }

  private foldMessageEvent(body: MessageEventBody, revision: number): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO messages (message_id, device_id, level, text, occurred_at, revision) VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(message_id) DO NOTHING`,
      body.message_id,
      body.device_id,
      body.level,
      body.text,
      body.occurred_at,
      revision,
    );
    this.ctx.storage.sql.exec(
      `DELETE FROM messages WHERE revision < (
         SELECT revision FROM messages ORDER BY revision DESC LIMIT 1 OFFSET ?
       )`,
      MESSAGE_WINDOW - 1,
    );
  }

  private activityRegistrant(taskId: string): { device_id: string; token: string } | null {
    return this.ctx.storage.sql.exec<{ device_id: string; token: string }>("SELECT device_id, token FROM activity_tokens WHERE task_id = ?", taskId).toArray()[0] ?? null;
  }

  // =========================================================================
  // PushOutbox (docs/design/v1-apns-outbox.md) — enqueue side
  // =========================================================================

  /** Enforces the space-wide and per-device row caps (§6) by evicting the
   * oldest EVICTABLE pending rows (activity_update/alert only — push_to_start
   * and activity_end are never evicted to make room). Returns false if the
   * caller's insert must be dropped because the cap still can't be
   * satisfied after evicting every eligible row. */
  private makeRoomForInsert(deviceId: string): boolean {
    const evictOldest = (limit: number, scopeToDevice: boolean): void => {
      if (limit <= 0) return;
      if (scopeToDevice) {
        this.ctx.storage.sql.exec(
          `DELETE FROM push_outbox WHERE push_id IN (
             SELECT push_id FROM push_outbox
             WHERE status = 'pending' AND kind IN ('activity_update', 'alert') AND device_id = ?
             ORDER BY created_at ASC LIMIT ?
           )`,
          deviceId,
          limit,
        );
      } else {
        this.ctx.storage.sql.exec(
          `DELETE FROM push_outbox WHERE push_id IN (
             SELECT push_id FROM push_outbox
             WHERE status = 'pending' AND kind IN ('activity_update', 'alert')
             ORDER BY created_at ASC LIMIT ?
           )`,
          limit,
        );
      }
    };

    let total = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM push_outbox").toArray()[0].n;
    if (total >= PUSH_OUTBOX_SPACE_ROW_CAP) {
      evictOldest(total - PUSH_OUTBOX_SPACE_ROW_CAP + 1, false);
      total = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM push_outbox").toArray()[0].n;
      if (total >= PUSH_OUTBOX_SPACE_ROW_CAP) {
        // §6: the business transaction still succeeds; only the notification
        // is dropped — but never SILENTLY (fault-injection review).
        this.logAlways("outbox_insert_dropped", { device_id: deviceId, cap: "space", total });
        return false;
      }
    }
    let perDevice = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM push_outbox WHERE device_id = ?", deviceId).toArray()[0].n;
    if (perDevice >= PUSH_OUTBOX_DEVICE_ROW_CAP) {
      evictOldest(perDevice - PUSH_OUTBOX_DEVICE_ROW_CAP + 1, true);
      perDevice = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM push_outbox WHERE device_id = ?", deviceId).toArray()[0].n;
      if (perDevice >= PUSH_OUTBOX_DEVICE_ROW_CAP) {
        this.logAlways("outbox_insert_dropped", { device_id: deviceId, cap: "device", per_device: perDevice });
        return false;
      }
    }
    return true;
  }

  private insertOutboxRow(opts: {
    pushId: string;
    kind: PushKind;
    deviceId: string;
    subjectId: string;
    generation: number | null;
    revision: number;
    coalesceKey: string;
    aps: Record<string, unknown>;
    priority: 5 | 10;
    pushType: "liveactivity" | "alert";
    expiresInMs: number;
  }): void {
    const now = Date.now();
    const payload = JSON.stringify({ aps: opts.aps, priority: opts.priority, pushType: opts.pushType });
    this.ctx.storage.sql.exec(
      `INSERT OR IGNORE INTO push_outbox
         (push_id, kind, device_id, subject_id, generation, revision, coalesce_key, payload, status, attempts, next_attempt_at, dispatch_started_at, last_error, created_at, expires_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, ?, ?)`,
      opts.pushId,
      opts.kind,
      opts.deviceId,
      opts.subjectId,
      opts.generation,
      opts.revision,
      opts.coalesceKey,
      payload,
      now,
      now,
      now + opts.expiresInMs,
    );
  }

  /** §4.1: at most one push_to_start row per (device, task, generation),
   * fanned out to every device with a registered push_to_start_token. */
  private enqueuePushToStartRows(taskId: string, generation: number, revision: number, view: PushTaskView): void {
    const targets = this.ctx.storage.sql
      .exec<{ device_id: string }>("SELECT device_id FROM push_tokens WHERE push_to_start_token IS NOT NULL")
      .toArray();
    for (const { device_id } of targets) {
      if (!this.makeRoomForInsert(device_id)) continue;
      const pushId = crypto.randomUUID();
      this.insertOutboxRow({
        pushId,
        kind: "push_to_start",
        deviceId: device_id,
        subjectId: taskId,
        generation,
        revision,
        coalesceKey: pushId, // non-coalescing kind: self-unique (§3.2)
        aps: pushToStartAps(view),
        priority: 10,
        pushType: "liveactivity",
        expiresInMs: 15 * 60_000,
      });
    }
  }

  /** §4.2: coalesces into the existing pending row for (device, task) if
   * one exists and the new revision is strictly greater; otherwise inserts
   * a fresh row (subject to the row caps). */
  private enqueueActivityUpdate(deviceId: string, taskId: string, generation: number, revision: number, view: PushTaskView): void {
    const coalesceKey = `${deviceId}:${taskId}`;
    const existing = this.ctx.storage.sql
      .exec<{ push_id: string; revision: number }>("SELECT push_id, revision FROM push_outbox WHERE coalesce_key = ? AND kind = 'activity_update' AND status = 'pending'", coalesceKey)
      .toArray()[0];
    const payload = JSON.stringify({ aps: activityUpdateAps(view, revision), priority: 5, pushType: "liveactivity" });
    if (existing) {
      if (revision > existing.revision) {
        this.ctx.storage.sql.exec(`UPDATE push_outbox SET payload = ?, revision = ?, generation = ? WHERE push_id = ?`, payload, revision, generation, existing.push_id);
      }
      return; // not newer than what's already queued: dropped (§4.2)
    }
    if (!this.makeRoomForInsert(deviceId)) return;
    this.insertOutboxRow({
      pushId: crypto.randomUUID(),
      kind: "activity_update",
      deviceId,
      subjectId: taskId,
      generation,
      revision,
      coalesceKey,
      aps: activityUpdateAps(view, revision),
      priority: 5,
      pushType: "liveactivity",
      expiresInMs: 2 * 60_000,
    });
  }

  /** §4.3: non-coalescing — a fresh row per end event. */
  private enqueueActivityEnd(deviceId: string, taskId: string, generation: number, revision: number, view: PushTaskView): void {
    if (!this.makeRoomForInsert(deviceId)) return;
    const pushId = crypto.randomUUID();
    this.insertOutboxRow({
      pushId,
      kind: "activity_end",
      deviceId,
      subjectId: taskId,
      generation,
      revision,
      coalesceKey: pushId,
      aps: activityEndAps(view, revision),
      priority: 10,
      pushType: "liveactivity",
      expiresInMs: 15 * 60_000,
    });
  }

  /** §4.3: fans out to every device with a registered alert_token.
   * `generation` is NULL for alert rows (no owning task generation). */
  private enqueueAlertBroadcast(subjectId: string, title: string, body: string, level: MessageLevel, priority: 5 | 10, revision: number): void {
    const targets = this.ctx.storage.sql.exec<{ device_id: string }>("SELECT device_id FROM push_tokens WHERE alert_token IS NOT NULL").toArray();
    for (const { device_id } of targets) {
      if (!this.makeRoomForInsert(device_id)) continue;
      const pushId = crypto.randomUUID();
      this.insertOutboxRow({
        pushId,
        kind: "alert",
        deviceId: device_id,
        subjectId,
        generation: null,
        revision,
        coalesceKey: pushId,
        aps: alertAps(title, body, level),
        priority,
        pushType: "alert",
        expiresInMs: 3600_000,
      });
    }
  }

  // =========================================================================
  // PushOutbox — Alarm drain side (docs/design/v1-apns-outbox.md §2, §5)
  // =========================================================================

  /** Bounded inline retry count for a transient setAlarm() failure (pre-
   * launch fix), tried synchronously since ensureAlarm() is already awaited
   * within the caller's request — makes the separate ensureAlarmIfPending()
   * self-heal a backstop for the rare case ALL attempts fail, not the
   * primary recovery mechanism it was before this fix (where a single
   * failure silently logged-and-returned, relying entirely on self-heal). */
  private static readonly ENSURE_ALARM_MAX_ATTEMPTS = 3;

  /** The earliest ms-epoch this DO's alarm needs to fire for, across BOTH
   * things it drives: push_outbox's earliest pending row, and (P0-7) the
   * per-space metrics_current debounce flush due time. `null` when neither
   * has anything pending. */
  private earliestAlarmDueAt(): number | null {
    const outboxEarliest = this.ctx.storage.sql.exec<{ m: number | null }>("SELECT MIN(next_attempt_at) as m FROM push_outbox WHERE status = 'pending'").toArray()[0]?.m ?? null;
    const candidates = [outboxEarliest, this.metricsFlushDueAt].filter((v): v is number => v !== null);
    return candidates.length === 0 ? null : Math.min(...candidates);
  }

  /** Reads the current alarm and the earliest due time (push_outbox and/or
   * the metrics_current debounce flush, P0-7); moves the alarm earlier if
   * needed (never later). Fire-and-forget from callers (never awaited inside
   * a synchronous business transaction — v1-apns-outbox.md §2) but also
   * serves as the cheap self-heal check any business-request entry point
   * should run (§7): calling it when nothing changed is a harmless no-op.
   *
   * A transient `setAlarm()` failure is retried inline, synchronously, up to
   * ENSURE_ALARM_MAX_ATTEMPTS times before giving up and logging (pre-launch
   * fix) — the separate ensureAlarmIfPending() self-heal (invoked on every
   * authenticated request via resolveToken) remains as the final backstop,
   * covering the case where even this bounded retry doesn't land, not as the
   * primary recovery path. */
  private async ensureAlarm(): Promise<void> {
    const earliest = this.earliestAlarmDueAt();
    if (earliest === null) return;
    let lastErr: unknown;
    for (let attempt = 1; attempt <= SpaceHub.ENSURE_ALARM_MAX_ATTEMPTS; attempt++) {
      try {
        const current = await this.ctx.storage.getAlarm();
        if (current === null || current > earliest) {
          await this.ctx.storage.setAlarm(earliest);
        }
        return;
      } catch (e) {
        lastErr = e;
      }
    }
    // Must never surface as a failure of the business write that triggered
    // it (v1-apns-outbox.md §2) — the anomaly self-heals on the next
    // business-request entry point's own ensureAlarmIfPending() call.
    this.logAlways("ensure_alarm_failed", { message: String(lastErr), attempts: SpaceHub.ENSURE_ALARM_MAX_ATTEMPTS });
  }

  /** Public re-arm for the HTTP ingest boundary (5c): POST /v1/events awaits
   * this after applying its batch, so the outbox alarm scheduled by the just-
   * enqueued rows has an AWAITED lifecycle within the request — the last row
   * is never stranded on abandoned post-response async work. The WS path awaits
   * `ensureAlarm()` directly at the end of `webSocketMessage`. */
  async ensureOutboxAlarm(): Promise<void> {
    await this.ensureAlarm();
  }

  /** Universal anomaly self-heal (v1-apns-outbox.md §2/§7): if push_outbox
   * has a pending row but no alarm is scheduled — the signature of a
   * `setAlarm()` failure (caught+logged in ensureAlarm) or a DO eviction
   * between an enqueue commit and its fire-and-forget re-arm — re-arm now.
   * Without this, a stranded row waits for the next *push-worthy* event to
   * re-arm; if the space's next activity isn't one, the push is silently
   * lost forever. Called at EVERY business-request entry point (the WS
   * `webSocketMessage` handler, and — via `resolveToken`, which every
   * authenticated HTTP request already invokes — HTTP route entry), so it
   * costs no extra RPC round-trip. Cheap: a single `getAlarm()` guarded
   * existence check. */
  private async ensureAlarmIfPending(): Promise<void> {
    try {
      if ((await this.ctx.storage.getAlarm()) !== null) return; // already scheduled — nothing to heal
      const pending = this.ctx.storage.sql.exec("SELECT 1 FROM push_outbox WHERE status = 'pending' LIMIT 1").toArray()[0];
      if (pending) await this.ensureAlarm();
    } catch (e) {
      this.logAlways("ensure_alarm_if_pending_failed", { message: String(e) });
    }
  }

  private apnsConfig(): ApnsConfig | null {
    const e = this.env;
    if (!e.APNS_KEY_P8 || !e.APNS_KEY_ID || !e.APNS_TEAM_ID || !e.APNS_BUNDLE_ID) return null;
    return { keyP8: e.APNS_KEY_P8, keyId: e.APNS_KEY_ID, teamId: e.APNS_TEAM_ID, bundleId: e.APNS_BUNDLE_ID, host: e.APNS_HOST || "api.sandbox.push.apple.com" };
  }

  async alarm(): Promise<void> {
    try {
      await this.drainOutbox();
    } catch (e) {
      this.logAlways("outbox_alarm_error", { message: String(e) });
    }
    try {
      // P0-7: flush any buffered routine metrics_current updates — reuses
      // this same alarm rather than a second, independent alarm mechanism
      // (v1-architecture.md §1.2.0).
      this.flushMetricsCurrent();
    } catch (e) {
      this.logAlways("metrics_current_flush_error", { message: String(e) });
    }
    this.sweepOutboxRetention();
    await this.ensureAlarm();
  }

  private async drainOutbox(): Promise<void> {
    const now = Date.now();
    const BATCH = 100;

    // §8.2 (fault-injection re-review): when delivery is paused, skip only the
    // APNs network call — still run the NON-network terminal decisions on
    // each due row, so expired/superseded rows reach their terminal status
    // and leave the 2000/200 cap accounting instead of lingering `pending`
    // for the whole pause window (APNS_DELIVERY_ENABLED=false is the shipped
    // default). A row that is neither expired nor stale gets its
    // `next_attempt_at` advanced to now+60s — preserving the no-busy-loop
    // property (un-terminated rows still move forward; the next wake is a
    // LATER check, never a re-arm to a past instant).
    if (!parseTransportFlag(this.env.APNS_DELIVERY_ENABLED)) {
      const dueRows = this.ctx.storage.sql
        .exec<PushOutboxSqlRow>("SELECT * FROM push_outbox WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY next_attempt_at LIMIT ?", now, BATCH)
        .toArray();
      for (const row of dueRows) {
        if (this.terminateIfDead(row, now)) continue;
        this.ctx.storage.sql.exec(`UPDATE push_outbox SET next_attempt_at = ? WHERE push_id = ?`, now + DELIVERY_PAUSED_RECHECK_MS, row.push_id);
      }
      return;
    }

    const rows = this.ctx.storage.sql
      .exec<PushOutboxSqlRow>("SELECT * FROM push_outbox WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY next_attempt_at LIMIT ?", now, BATCH)
      .toArray();
    if (rows.length === 0) return;

    // Bounded-concurrency dispatch (v1-apns-outbox.md §2.1) — never
    // Promise.all(rows.map(...)) unbounded.
    const CONCURRENCY = 8;
    let i = 0;
    const workers = Array.from({ length: Math.min(CONCURRENCY, rows.length) }, async () => {
      while (i < rows.length) {
        const row = rows[i++];
        await this.dispatchOutboxRow(row, now);
      }
    });
    await Promise.all(workers);
  }

  private markOutboxTerminal(pushId: string, status: "sent" | "permanent_failure" | "expired", lastError: string | null): void {
    // Every transition into a terminal status stamps the dedicated
    // terminal_at column (v1-apns-outbox.md §3) — the decision time the §6
    // retention sweep keys on. next_attempt_at is left untouched (its
    // dispatch-eligibility meaning is simply moot for a terminal row).
    const now = Date.now();
    this.ctx.storage.sql.exec(`UPDATE push_outbox SET status = ?, last_error = ?, dispatch_started_at = NULL, terminal_at = ? WHERE push_id = ?`, status, lastError, now, pushId);
  }

  private resolveOutboxToken(row: PushOutboxSqlRow): string | null {
    if (row.kind === "push_to_start") {
      return this.ctx.storage.sql.exec<{ t: string | null }>("SELECT push_to_start_token as t FROM push_tokens WHERE device_id = ?", row.device_id).toArray()[0]?.t ?? null;
    }
    if (row.kind === "activity_update" || row.kind === "activity_end") {
      return (
        this.ctx.storage.sql
          .exec<{ t: string | null }>("SELECT token as t FROM activity_tokens WHERE task_id = ? AND device_id = ?", row.subject_id, row.device_id)
          .toArray()[0]?.t ?? null
      );
    }
    return this.ctx.storage.sql.exec<{ t: string | null }>("SELECT alert_token as t FROM push_tokens WHERE device_id = ?", row.device_id).toArray()[0]?.t ?? null;
  }

  /** Cleans up a token that APNs just classified as permanently invalid
   * (v1-apns-outbox.md §4.3) — the exact table/column is already known from
   * `row.kind`, so no cross-table search is needed. */
  private cleanupInvalidToken(row: PushOutboxSqlRow): void {
    if (row.kind === "push_to_start") {
      this.ctx.storage.sql.exec(`UPDATE push_tokens SET push_to_start_token = NULL WHERE device_id = ?`, row.device_id);
    } else if (row.kind === "activity_update" || row.kind === "activity_end") {
      this.ctx.storage.sql.exec(`DELETE FROM activity_tokens WHERE task_id = ? AND device_id = ?`, row.subject_id, row.device_id);
    } else {
      this.ctx.storage.sql.exec(`UPDATE push_tokens SET alert_token = NULL WHERE device_id = ?`, row.device_id);
    }
  }

  private currentTaskGeneration(taskId: string): number | null {
    return this.ctx.storage.sql.exec<{ generation: number }>("SELECT generation FROM tasks WHERE task_id = ?", taskId).toArray()[0]?.generation ?? null;
  }

  /** The NON-network terminal decisions for one due row: expiry, generation
   * staleness (activity_update/activity_end), and the push_to_start
   * ambiguous-dispatch grace. Returns true if the row was moved to a
   * terminal status here (caller must stop), false if it is still a live
   * candidate for an APNs call. Shared by `dispatchOutboxRow` and the
   * delivery-paused path in `drainOutbox`, so dead rows are cleaned out of
   * the cap accounting even while APNs delivery is paused (the shipped
   * default) — v1-apns-outbox.md §8.2 re-review. None of these decisions
   * touches the network, so running them while paused is safe. */
  private terminateIfDead(row: PushOutboxSqlRow, now: number): boolean {
    if (row.expires_at <= now) {
      this.markOutboxTerminal(row.push_id, "expired", null);
      return true;
    }

    // Generation-staleness guard (activity_update/activity_end only, §3.1/§4.2/§4.3).
    if ((row.kind === "activity_update" || row.kind === "activity_end") && row.generation !== null) {
      const currentGen = this.currentTaskGeneration(row.subject_id);
      if (currentGen !== null && row.generation < currentGen) {
        this.markOutboxTerminal(row.push_id, "permanent_failure", "superseded_by_newer_generation");
        return true;
      }
    }

    // Ambiguous-dispatch grace (push_to_start only, §4.1): a prior attempt
    // may have actually reached APNs before the DO was evicted/crashed.
    if (row.kind === "push_to_start" && row.dispatch_started_at !== null && now - row.dispatch_started_at > AMBIGUOUS_DISPATCH_GRACE_MS) {
      this.markOutboxTerminal(row.push_id, "permanent_failure", "ambiguous_dispatch_outcome_not_retried");
      return true;
    }

    return false;
  }

  private async dispatchOutboxRow(row: PushOutboxSqlRow, now: number): Promise<void> {
    // Non-network terminal decisions first (shared with the paused path).
    if (this.terminateIfDead(row, now)) return;

    // Delivery-paused is handled up-front in drainOutbox (§8.2), which
    // short-circuits before any row reaches here — so a row that gets this
    // far is genuinely eligible to dispatch.

    const cfg = this.apnsConfig();
    if (!cfg) {
      this.markOutboxTerminal(row.push_id, "permanent_failure", "apns_not_configured");
      return;
    }

    const token = this.resolveOutboxToken(row);
    if (!token) {
      this.markOutboxTerminal(row.push_id, "permanent_failure", "no_token");
      return;
    }

    // Stamped BEFORE the network call (§4.1) so a crash mid-flight leaves
    // durable evidence of a possibly-in-flight push.
    this.ctx.storage.sql.exec(`UPDATE push_outbox SET dispatch_started_at = ? WHERE push_id = ?`, now, row.push_id);

    const { aps, priority, pushType } = JSON.parse(row.payload) as { aps: Record<string, unknown>; priority: 5 | 10; pushType: "liveactivity" | "alert" };
    let outcome: ReturnType<typeof classifyApnsResponse> | { kind: "transient"; reason: string; retryAfterMs?: number };
    try {
      const req = await buildApnsRequest(cfg, token, { pushType, priority, aps });
      const res = await this.apnsFetch(req);
      const bodyText = await res.text().catch(() => "");
      let reason: string | undefined;
      try {
        reason = (JSON.parse(bodyText) as { reason?: string })?.reason;
      } catch {
        // non-JSON body; reason stays undefined
      }
      outcome = classifyApnsResponse(res.status, reason, res.headers.get("retry-after"));
    } catch (e) {
      outcome = { kind: "transient", reason: `network_error:${String(e)}` };
    }

    if (outcome.kind === "sent") {
      this.markOutboxTerminal(row.push_id, "sent", null);
      return;
    }
    if (outcome.kind === "permanent") {
      this.markOutboxTerminal(row.push_id, "permanent_failure", outcome.reason);
      if (outcome.badToken) this.cleanupInvalidToken(row);
      return;
    }

    const attempts = row.attempts + 1;
    if (attempts >= MAX_TRANSIENT_ATTEMPTS) {
      this.markOutboxTerminal(row.push_id, "permanent_failure", "retry_budget_exhausted");
      return;
    }
    const backoff = backoffMs(attempts);
    const nextAt = outcome.retryAfterMs !== undefined ? Math.max(now + outcome.retryAfterMs, now + backoff) : now + backoff;
    this.ctx.storage.sql.exec(
      `UPDATE push_outbox SET status = 'pending', attempts = ?, next_attempt_at = ?, dispatch_started_at = NULL, last_error = ? WHERE push_id = ?`,
      attempts,
      nextAt,
      outcome.reason,
      row.push_id,
    );
  }

  private sweepOutboxRetention(now = Date.now()): void {
    // Keyed on the explicit terminal_at column (v1-apns-outbox.md §6): sent
    // rows drop 1h after they reached terminal, permanent_failure/expired
    // rows 24h after. A pending row (terminal_at IS NULL) is never swept.
    this.ctx.storage.sql.exec(`DELETE FROM push_outbox WHERE status = 'sent' AND terminal_at < ?`, now - 3600_000);
    this.ctx.storage.sql.exec(`DELETE FROM push_outbox WHERE status IN ('permanent_failure', 'expired') AND terminal_at < ?`, now - 24 * 3600_000);
  }

  // =========================================================================
  // Control-plane state reads/writes used by app.ts's HTTP handlers
  // =========================================================================

  /** The space's current revision counter, with no other side effect —
   * used by POST /v1/events to report `space_revision` when a batch
   * contained only metric.frame items (no reliable event advanced it). */
  currentRevision(): number {
    return this.getRevision();
  }

  getSnapshot(capabilities: Capabilities): Snapshot {
    const arrays = this.buildSnapshotArrays();
    return {
      space_revision: this.getRevision(),
      generated_at: new Date().toISOString(),
      capabilities,
      presence: this.buildPresence(),
      ...arrays,
    };
  }

  private buildPresence(): Presence {
    const ingest = this.spaceMetaNumber("ingest_last_seen");
    const agent = this.spaceMetaNumber("agent_last_seen");
    const sourceDeviceIds = new Set<string>();
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (isConnAttachment(att) && att.role === "source") sourceDeviceIds.add(att.deviceId);
    }
    return {
      ...(ingest !== null ? { ingest_last_seen: ingest } : {}),
      ...(agent !== null ? { agent_last_seen: agent } : {}),
      sources_online: sourceDeviceIds.size,
    };
  }

  getMetric(id: string): MetricSample | null {
    // Reads the persistent metrics_current (P0-2): a rebuilt DO still serves
    // the last accepted value, and 404 (null) is authoritative — the metric
    // was genuinely never reported.
    return this.readMetricCurrent(id)?.fields ?? null;
  }

  /** Reads the PERSISTENT metric_series table (v1-architecture.md §1.2.1)
   * via the pure selectSeries, mapping each stored SeriesPoint{t,v} onto the
   * frozen MetricSeriesPoint{ts,value} wire shape (openapi). Durable across
   * DO eviction. */
  getMetricSeries(id: string, range: SeriesRange): MetricSeriesPoint[] {
    const series = this.readMetricSeries(id);
    return selectSeries(series, range).map((p) => ({ ts: p.t, value: p.v }));
  }

  /** Reads the PERSISTENT per-task log ring buffer (v1-architecture.md
   * §1.2.2), oldest first — NOT reconstructed from event_log. */
  getTaskLog(taskId: string): string[] {
    const row = this.ctx.storage.sql.exec<{ lines: string }>("SELECT lines FROM task_logs WHERE task_id = ?", taskId).toArray()[0];
    return row ? (JSON.parse(row.lines) as string[]) : [];
  }

  /** Source uplink for POST /v1/tasks/:id/log (v1-architecture.md §1.2.2):
   * best-effort append into the per-task ring buffer via the pure
   * appendTaskLog. CRITICAL: non-folded — issues only a task_logs write,
   * never a setRevision/broadcastDelta/dedup insert. Not on the device_seq
   * at-least-once path (a dropped/duplicated log line has no
   * revision-continuity consequence). */
  appendTaskLogLines(taskId: string, lines: string[]): void {
    const prev = this.getTaskLog(taskId);
    const next = appendTaskLog(prev, lines);
    this.ctx.storage.sql.exec(
      `INSERT INTO task_logs (task_id, lines, updated_at) VALUES (?, ?, ?)
       ON CONFLICT(task_id) DO UPDATE SET lines = excluded.lines, updated_at = excluded.updated_at`,
      taskId,
      JSON.stringify(next),
      Date.now(),
    );
  }

  deleteMessage(id: string): void {
    this.ctx.storage.sql.exec(`DELETE FROM messages WHERE message_id = ?`, id);
  }

  deleteAllMessages(): void {
    this.ctx.storage.sql.exec(`DELETE FROM messages`);
  }

  private spaceMetaNumber(key: string): number | null {
    const row = this.ctx.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key = ?", key).toArray()[0];
    return row ? Number(row.value) : null;
  }

  /** Writes a non-folded presence marker (v1-architecture.md §1.2/§7.1) —
   * deliberately does NOT touch `revision`. */
  private stampSpaceMetaNow(key: string): void {
    this.ctx.storage.sql.exec(`INSERT INTO space_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, String(Date.now()));
  }

  private getRevision(): number {
    const row = this.ctx.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key = 'revision'").toArray()[0];
    return row ? Number(row.value) : 0;
  }

  private setRevision(n: number): void {
    this.ctx.storage.sql.exec(`INSERT INTO space_meta (key, value) VALUES ('revision', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, String(n));
  }

  private pruneEventLog(currentRevision: number): void {
    const floor = currentRevision - EVENT_LOG_RETENTION_REVISIONS;
    if (floor > 0) this.ctx.storage.sql.exec(`DELETE FROM event_log WHERE revision <= ?`, floor);
  }

  // ---- automations (config.event mint — v1-architecture.md §5) ----

  mintConfigEvent(
    idempotencyKey: string | null,
    payload: { kind: ConfigEventKind; automation_id: string; automation?: AutomationState },
  ): { revision: number; automation: AutomationState | null; conflict?: boolean } {
    const fingerprint = canonicalJson({ kind: payload.kind, automation_id: payload.automation_id, automation: payload.automation ?? null });
    if (idempotencyKey) {
      const existing = this.ctx.storage.sql
        .exec<{ revision: number; fingerprint: string }>("SELECT revision, fingerprint FROM http_idempotency WHERE idempotency_key = ?", idempotencyKey)
        .toArray()[0];
      if (existing) {
        if (existing.fingerprint !== fingerprint) return { revision: existing.revision, automation: null, conflict: true };
        return { revision: existing.revision, automation: this.readAutomation(payload.automation_id) };
      }
    }

    const occurredAt = Date.now();
    const revision = this.getRevision() + 1;
    const body: ConfigEventBody = { kind: payload.kind, automation_id: payload.automation_id, automation: payload.automation, occurred_at: occurredAt };
    const eventPayload = JSON.stringify(body);

    this.ctx.storage.sql.exec(
      `INSERT INTO event_log (revision, event_type, device_id, device_seq, occurred_at, payload) VALUES (?, 'config.event', NULL, NULL, ?, ?)`,
      revision,
      occurredAt,
      eventPayload,
    );
    this.foldConfigEvent(body);
    this.setRevision(revision);
    if (idempotencyKey) {
      this.ctx.storage.sql.exec(`INSERT INTO http_idempotency (idempotency_key, fingerprint, revision, created_at) VALUES (?, ?, ?, ?)`, idempotencyKey, fingerprint, revision, occurredAt);
      this.ctx.storage.sql.exec(`DELETE FROM http_idempotency WHERE created_at < ?`, occurredAt - 24 * 3600 * 1000);
      this.ctx.storage.sql.exec(`DELETE FROM http_idempotency WHERE idempotency_key NOT IN (SELECT idempotency_key FROM http_idempotency ORDER BY created_at DESC LIMIT 500)`);
    }
    this.pruneEventLog(revision);

    this.broadcastDelta({ from_revision: revision - 1, to_revision: revision, events: [{ event_type: "config.event", event: body }] });

    return { revision, automation: this.readAutomation(payload.automation_id) };
  }

  automationsSnapshot(): AutomationState[] {
    return this.ctx.storage.sql.exec<AutomationRow>("SELECT * FROM automations ORDER BY automation_id").toArray().map(rowToAutomationState);
  }

  private foldConfigEvent(body: ConfigEventBody): void {
    if (body.kind === "automation.upserted" && body.automation) {
      const a = body.automation;
      this.ctx.storage.sql.exec(
        `INSERT INTO automations (automation_id, name, executor_kind, every_seconds, state, last_run_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(automation_id) DO UPDATE SET
           name = excluded.name, executor_kind = excluded.executor_kind, every_seconds = excluded.every_seconds,
           state = excluded.state, last_run_at = excluded.last_run_at`,
        a.automation_id,
        a.name,
        a.executor_kind,
        a.schedule.every_seconds,
        a.state,
        a.last_run_at ?? null,
      );
    } else if (body.kind === "automation.removed") {
      this.ctx.storage.sql.exec(`DELETE FROM automations WHERE automation_id = ?`, body.automation_id);
    }
  }

  private readAutomation(id: string): AutomationState | null {
    const row = this.ctx.storage.sql.exec<AutomationRow>("SELECT * FROM automations WHERE automation_id = ?", id).toArray()[0];
    return row ? rowToAutomationState(row) : null;
  }

  // =========================================================================
  // CommandStore (v1-architecture.md §1.4) — HTTP-issued reverse control
  // =========================================================================

  /** Shared by POST /v1/tasks/:id/commands and POST /v1/automations/:id/run
   * (v1-architecture.md §4.4/§5.1/§6). Idempotent on commandId: a retried
   * POST with the same id neither re-relays nor re-queues. `pending_commands`
   * doubles as the idempotency ledger for BOTH the live-relayed and the
   * queued case (its PK is command_id, not "queued-only"), swept lazily by
   * TTL on every call. */
  /** The device_id running `taskId` (P0-3), or null if no `started` has been
   * seen for it. Reverse-control commands are directed to this device. */
  taskOwningDevice(taskId: string): string | null {
    return this.ctx.storage.sql.exec<{ owning_device_id: string | null }>("SELECT owning_device_id FROM tasks WHERE task_id = ?", taskId).toArray()[0]?.owning_device_id ?? null;
  }

  /** Enqueues a reverse-control command DIRECTED to the task's owning device
   * (P0-3), never broadcast. Returns `{ ok: false, error }` when the command
   * can never deliver: `"task not running"` (no `started` seen for the task)
   * or `"owning device unavailable"` (the owning device was revoked, MINOR
   * fix) — the caller surfaces a clear error rather than silently queuing a
   * dead command.
   *
   * The command is pushed to the owning device's live source WS as a
   * BEST-EFFORT low-latency hint, but this NEVER sets `delivered` (MAJOR fix):
   * the WS is not necessarily the task executor. `delivered` is flipped ONLY
   * by the executing process's HTTP `for_task_id` drain, so a command pushed
   * to an agent WS that ignores it is not lost — the row stays `delivered=0`
   * and pending for the real executor. */
  relayOrQueueCommand(body: CommandBody): { ok: true } | { ok: false; error: string } {
    const now = Date.now();
    this.ctx.storage.sql.exec(`DELETE FROM pending_commands WHERE origin_ts + ttl_ms < ?`, now);

    const owningDeviceId = body.task_id ? this.taskOwningDevice(body.task_id) : null;
    if (!owningDeviceId) return { ok: false, error: "task not running" };
    // MINOR: the owning device must still resolve — if it was revoked, the
    // command could never deliver, so reject at enqueue with viewer feedback.
    const deviceExists = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM devices WHERE device_id = ?", owningDeviceId).toArray()[0].n > 0;
    if (!deviceExists) return { ok: false, error: "owning device unavailable" };

    const already = this.ctx.storage.sql.exec<{ command_id: string }>("SELECT command_id FROM pending_commands WHERE command_id = ?", body.command_id).toArray()[0];
    if (already) return { ok: true }; // idempotent replay

    // Best-effort push to the owning device's live source WS (a delivery hint
    // only) — MUST NOT set delivered.
    for (const target of this.ctx.getWebSockets(owningDeviceId)) {
      const targetAtt = target.deserializeAttachment();
      if (!isConnAttachment(targetAtt) || targetAtt.role !== "source" || !targetAtt.helloDone) continue;
      this.sendRaw(target, { type: "command", id: newEnvelopeId(), ts: now, body });
    }
    // Always persist delivered=0: only the executor's HTTP for_task_id drain
    // consumes it.
    this.ctx.storage.sql.exec(
      `INSERT INTO pending_commands (command_id, target_device_id, origin_ts, ttl_ms, payload, delivered) VALUES (?, ?, ?, ?, ?, 0)
       ON CONFLICT(command_id) DO NOTHING`,
      body.command_id,
      owningDeviceId,
      now,
      body.ttl_ms,
      JSON.stringify(body),
    );
    return { ok: true };
  }

  hasAutomation(id: string): boolean {
    return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM automations WHERE automation_id = ?", id).toArray()[0].n > 0;
  }

  /** POST /v1/automations/:id/run (v1-architecture.md §5.1, P0-4): INCREMENTS
   * the automation's monotonic `run_request_id` and stamps `run_requested_at`
   * (display only). Returns the resulting run_request_id, or null if no such
   * automation exists (caller 404s). Mints NO config.event, advances NO
   * space_revision, enqueues NO command. The agent runs the automation once
   * when it observes run_request_id advance beyond its last-consumed value —
   * no clock comparison.
   *
   * An optional `idempotencyKey` dedups a network retry of the SAME tap to one
   * increment (reusing the http_idempotency ledger, §1.5): replaying the key
   * returns the same run_request_id without a second increment. A retry with
   * no key increments again, which is safe anyway (the agent still runs at
   * most once per distinct id). */
  setRunRequest(id: string, idempotencyKey: string | null): number | null {
    if (!this.hasAutomation(id)) return null;

    if (idempotencyKey) {
      const existing = this.ctx.storage.sql
        .exec<{ fingerprint: string; revision: number }>("SELECT fingerprint, revision FROM http_idempotency WHERE idempotency_key = ?", idempotencyKey)
        .toArray()[0];
      // fingerprint pins the key to this automation; revision stores the
      // resulting run_request_id. A replay of the same key+automation returns
      // the same id without a second increment.
      if (existing && existing.fingerprint === `run:${id}`) return existing.revision;
    }

    const now = Date.now();
    this.ctx.storage.sql.exec(`UPDATE automations SET run_request_id = run_request_id + 1, run_requested_at = ? WHERE automation_id = ?`, now, id);
    const runRequestId = this.ctx.storage.sql.exec<{ run_request_id: number }>("SELECT run_request_id FROM automations WHERE automation_id = ?", id).toArray()[0].run_request_id;

    if (idempotencyKey) {
      this.ctx.storage.sql.exec(
        `INSERT INTO http_idempotency (idempotency_key, fingerprint, revision, created_at) VALUES (?, ?, ?, ?)
         ON CONFLICT(idempotency_key) DO NOTHING`,
        idempotencyKey,
        `run:${id}`,
        runRequestId,
        now,
      );
      this.ctx.storage.sql.exec(`DELETE FROM http_idempotency WHERE created_at < ?`, now - 24 * 3600 * 1000);
      this.ctx.storage.sql.exec(`DELETE FROM http_idempotency WHERE idempotency_key NOT IN (SELECT idempotency_key FROM http_idempotency ORDER BY created_at DESC LIMIT 500)`);
    }
    return runRequestId;
  }

  // ---- WS-only interest lease + command relay (proto/realtime/SPEC.md) ----

  private handleSubscribe(ws: WebSocket, att: ConnAttachment, body: SubscribeBody, envelopeId: string): void {
    const expiresAt = this.upsertLease(att.deviceId, body.topics ?? []);
    att.subscribedThisConn = true;
    ws.serializeAttachment(att);
    this.send(ws, "ack", { in_reply_to: envelopeId, lease: { expires_at: expiresAt } });
    this.reconcileLeaseEdge();
  }

  private handleUnsubscribe(ws: WebSocket, att: ConnAttachment, envelopeId: string): void {
    this.ctx.storage.sql.exec(`DELETE FROM leases WHERE device_id = ?`, att.deviceId);
    this.send(ws, "ack", { in_reply_to: envelopeId });
    this.reconcileLeaseEdge();
  }

  private handleInterestRenew(ws: WebSocket, att: ConnAttachment, body: InterestRenewBody, envelopeId: string): void {
    const expiresAt = this.upsertLease(att.deviceId, body.topics ?? []);
    this.send(ws, "ack", { in_reply_to: envelopeId, lease: { expires_at: expiresAt } });
    this.reconcileLeaseEdge();
  }

  private upsertLease(deviceId: string, topics: string[]): number {
    const expiresAt = Date.now() + LEASE_DEFAULT_MS;
    this.ctx.storage.sql.exec(
      `INSERT INTO leases (device_id, expires_at, topics) VALUES (?, ?, ?)
       ON CONFLICT(device_id) DO UPDATE SET expires_at = excluded.expires_at, topics = excluded.topics`,
      deviceId,
      expiresAt,
      JSON.stringify(topics),
    );
    return expiresAt;
  }

  private deviceHasActiveLease(deviceId: string): boolean {
    const row = this.ctx.storage.sql.exec<{ expires_at: number }>("SELECT expires_at FROM leases WHERE device_id = ?", deviceId).toArray()[0];
    return !!row && row.expires_at > Date.now();
  }

  private deviceHasMetricInterest(deviceId: string, now: number): boolean {
    const row = this.ctx.storage.sql.exec<{ expires_at: number; topics: string }>("SELECT expires_at, topics FROM leases WHERE device_id = ?", deviceId).toArray()[0];
    if (!row || row.expires_at <= now) return false;
    const topics: string[] = JSON.parse(row.topics);
    return topics.length === 0 || topics.includes("metric");
  }

  private reconcileLeaseEdge(): void {
    const now = Date.now();
    const activeNow = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM leases WHERE expires_at > ?", now).toArray()[0].n;
    const prevRow = this.ctx.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key = 'lease_active_count'").toArray()[0];
    const prev = prevRow ? Number(prevRow.value) : 0;
    if (activeNow === prev) return;
    this.ctx.storage.sql.exec(`INSERT INTO space_meta (key, value) VALUES ('lease_active_count', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, String(activeNow));
    if (prev > 0 && activeNow === 0) this.broadcastServerCommand("throttle");
    else if (prev === 0 && activeNow > 0) this.broadcastServerCommand("resume_rate");
  }

  private broadcastServerCommand(action: "throttle" | "resume_rate"): void {
    const body: CommandBody = { command_id: crypto.randomUUID(), origin: "server", action, ttl_ms: 60_000 };
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (!isConnAttachment(att) || att.role !== "source" || !att.helloDone) continue;
      this.send(ws, "command", body);
    }
  }

  private sendCurrentRateState(ws: WebSocket): void {
    const active = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM leases WHERE expires_at > ?", Date.now()).toArray()[0].n;
    const body: CommandBody = { command_id: crypto.randomUUID(), origin: "server", action: active > 0 ? "resume_rate" : "throttle", ttl_ms: 60_000 };
    this.send(ws, "command", body);
  }

  private handleCommand(ws: WebSocket, att: ConnAttachment, body: CommandBody, envelopeTs: number, envelopeId: string): void {
    if (body.issued_by_device_id !== att.deviceId) {
      this.reply(ws, "unauthorized", "issued_by_device_id must match the authenticated device", envelopeId);
      return;
    }
    if (Date.now() > envelopeTs + body.ttl_ms) {
      this.reply(ws, "command_expired", "command TTL elapsed before relay", envelopeId);
      return;
    }

    // Directed to the task's owning device (P0-3), server-trusted from
    // owning_device_id — never broadcast to every source. A command for a
    // task with no owning device (no `started` seen) is dropped rather than
    // fanned out or persisted with no valid recipient.
    const owningDeviceId = body.task_id ? this.taskOwningDevice(body.task_id) : (body.target_device_id ?? null);
    if (!owningDeviceId) return;

    // Best-effort push to the owning device's live source WS (delivery hint) —
    // MUST NOT set delivered (MAJOR fix). ALWAYS persist delivered=0 even when
    // pushed live: only the executor's HTTP for_task_id drain consumes it, so
    // an agent WS that receives-and-ignores the command does not lose it.
    for (const target of this.ctx.getWebSockets(owningDeviceId)) {
      const targetAtt = target.deserializeAttachment();
      if (!isConnAttachment(targetAtt) || targetAtt.role !== "source" || !targetAtt.helloDone) continue;
      this.sendRaw(target, { type: "command", id: newEnvelopeId(), ts: envelopeTs, body });
    }
    this.ctx.storage.sql.exec(
      `INSERT INTO pending_commands (command_id, target_device_id, origin_ts, ttl_ms, payload, delivered) VALUES (?, ?, ?, ?, ?, 0) ON CONFLICT(command_id) DO NOTHING`,
      body.command_id,
      owningDeviceId,
      envelopeTs,
      body.ttl_ms,
      JSON.stringify(body),
    );
  }

  private drainPendingCommands(ws: WebSocket, deviceId: string): void {
    // BEST-EFFORT WS delivery hint (P0-5, fetch-then-ack): re-push pending
    // commands to a (re)connecting source WS. This NEVER sets delivered —
    // under fetch-then-ack, NOTHING sets delivered except a matching
    // ack_command_ids entry on POST /v1/events (applyCommandAcks below). The
    // agent WS is not necessarily the task executor, and inclusion here is
    // just a low-latency hint, not delivery.
    for (const { body, origin_ts } of this.drainCommandsForDevice(deviceId, {})) {
      // Relay rules (SPEC.md §8): preserve the viewer's original ts
      // (origin_ts), only the envelope id is fresh.
      this.sendRaw(ws, { type: "command", id: newEnvelopeId(), ts: origin_ts, body });
    }
  }

  /** The ONE CommandStore drain (v1-architecture.md §1.4, §4.1, P0-5). Returns
   * this device's currently-pending, non-expired directed commands. Fetch-
   * then-ack: inclusion here NEVER sets `delivered`, on EITHER transport — a
   * device that polls repeatedly without acking sees the identical command
   * every time (intentional at-least-once redelivery). The ONLY way
   * `delivered` is ever set is `applyCommandAcks` below, called from
   * `POST /v1/events` with an explicit `ack_command_ids` entry from the
   * owning device. This replaces the prior R1 "HTTP-drain-authoritative"
   * model (delivered flipped on inclusion), which an external review
   * live-reproduced as a data-loss bug against real workerd: a lost HTTP
   * response meant the command was gone forever, even though the device
   * never actually acted on it.
   *
   * Lazy expiry (fix 7): a global sweep of ALL expired rows runs first.
   *
   * `opts.actionFilter` restricts to representable actions (the HTTP
   * `PendingCommand` shape covers only pause/resume/stop). `opts.forTaskId`
   * (fix 6): a command naming a DIFFERENT task than this uplink declares is
   * left for that task's process — only the matching task's commands are
   * returned. */
  private drainCommandsForDevice(deviceId: string, opts: { actionFilter?: ReadonlySet<string>; forTaskId?: string }): Array<{ body: CommandBody; origin_ts: number }> {
    const now = Date.now();
    // Fix 7: unconditional lazy sweep of every expired row, regardless of
    // delivered flag or task scope — transport/task-agnostic GC.
    this.ctx.storage.sql.exec(`DELETE FROM pending_commands WHERE origin_ts + ttl_ms < ?`, now);

    // Directed delivery (P0-3): only rows addressed to THIS device drain. A
    // source WS whose device_id is not the row's target_device_id is not a
    // valid recipient — the broadcast `OR target_device_id IS NULL` fallback
    // is removed so a command is never consumed by the wrong device.
    const rows = this.ctx.storage.sql
      .exec<{ command_id: string; origin_ts: number; ttl_ms: number; payload: string }>(
        `SELECT command_id, origin_ts, ttl_ms, payload FROM pending_commands WHERE delivered = 0 AND target_device_id = ?`,
        deviceId,
      )
      .toArray();
    const drained: Array<{ body: CommandBody; origin_ts: number }> = [];
    for (const row of rows) {
      const body = JSON.parse(row.payload) as CommandBody;
      if (opts.actionFilter && !opts.actionFilter.has(body.action)) continue; // leave for the other transport
      // Task-scoped routing (§4.1): on a multi-task device, a command naming a
      // DIFFERENT task than this uplink declares is left for that task's
      // process to poll.
      if (opts.forTaskId !== undefined && body.task_id !== undefined && body.task_id !== opts.forTaskId) continue;
      drained.push({ body, origin_ts: row.origin_ts });
    }
    return drained;
  }

  /** P0-5 fetch-then-ack (v1-architecture.md §1.4, §4.1): the ONLY place
   * `delivered` is ever set. Applies every `command_id` in `ackCommandIds`
   * that names a pending, still-undelivered row OWNED by this device — an
   * id that is unknown, already acked, expired, or belongs to a different
   * device is silently ignored (a no-op, not an error): acking is
   * idempotent and safe to retry or send redundantly. Called from
   * `drainPendingCommandsForHttp` BEFORE it computes the drain, so a single
   * request that both acks an old command and polls for new ones never gets
   * back the very command it just acked. */
  private applyCommandAcks(deviceId: string, ackCommandIds: readonly string[] | undefined): void {
    if (!ackCommandIds || ackCommandIds.length === 0) return;
    for (const commandId of ackCommandIds) {
      this.ctx.storage.sql.exec(`UPDATE pending_commands SET delivered = 1 WHERE command_id = ? AND target_device_id = ? AND delivered = 0`, commandId, deviceId);
    }
  }

  /** POST /v1/events reverse-control piggyback (v1-architecture.md §4.1,
   * P0-5). Applies this request's `ackCommandIds` FIRST (the only thing that
   * ever sets `delivered`), THEN returns the executing process's currently-
   * pending, non-expired pause/resume/stop commands into the frozen
   * `PendingCommand[]` shape — included on EVERY poll regardless of prior
   * inclusion, until acked or expired. Called on every uplink, including an
   * empty `events:[]` heartbeat. */
  drainPendingCommandsForHttp(deviceId: string, forTaskId?: string, ackCommandIds?: readonly string[]): PendingCommand[] {
    this.applyCommandAcks(deviceId, ackCommandIds);
    return this.drainCommandsForDevice(deviceId, { actionFilter: TASK_COMMAND_ACTIONS, forTaskId }).map(({ body, origin_ts }) => ({
      command_id: body.command_id,
      origin: "viewer" as const,
      action: body.action as TaskCommandAction,
      task_id: body.task_id!,
      ttl_ms: body.ttl_ms,
      origin_ts,
    }));
  }

  // ---- send helpers ----

  private send<B>(ws: WebSocket, type: MessageType, body: B): void {
    ws.send(JSON.stringify(makeEnvelope(type, body)));
  }

  private sendRaw(ws: WebSocket, envelope: AnyEnvelope): void {
    ws.send(JSON.stringify(envelope));
  }

  private reply(ws: WebSocket, code: ErrorCode, message: string, inReplyTo: string | undefined): void {
    this.sendRaw(ws, makeStandardError(code, message, inReplyTo));
    this.logAlways("protocol_error", { code, message });
  }

  private logHotPath(event: string, data: Record<string, unknown>): void {
    this.hotPathCounter++;
    if (this.logSampler(this.hotPathCounter)) {
      console.log(JSON.stringify({ level: "info", event, ...data, ts: Date.now() }));
    }
  }

  private logAlways(event: string, data: Record<string, unknown>): void {
    console.log(JSON.stringify({ level: "error", event, ...data, ts: Date.now() }));
    this.securityEventLog.push({ event, data });
    if (this.securityEventLog.length > 200) this.securityEventLog.shift();
  }
}

export const SPACE_HUB_PROTOCOL_VERSION = PROTOCOL_VERSION;
