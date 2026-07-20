// Frozen v1 HTTP contract — pure types and constants, no logic.
//
// This file exists to remove ambiguity for the server implementation line
// (and to give the daemon/apple lines a single TypeScript reference to diff
// their own models against), NOT as a place to implement `/v1` behavior.
// The narrative source of truth is `docs/design/v1-architecture.md` and
// `docs/design/v1-apns-outbox.md`; the machine-checkable source of truth for
// wire shapes is `docs/api/v1/openapi.yaml`. Every shape below must match
// both, field for field. If you find a mismatch, the doc/schema is wrong (or
// this file is) — fix whichever is out of sync, do not let a third,
// silently-different shape enter the codebase.
//
// The event-body types below (TaskEventBody, MessageEventBody,
// MetricFrameBody, DisplayHints, MetricSample, AutomationState, ...) are
// themselves derived from `proto/realtime/*.schema.json` — see
// `server/src/realtime/types.ts` (once the realtime implementation lands on
// this branch) for the canonical WS-side mirror. They are repeated here,
// not imported, because this contract module must remain buildable and
// reviewable independently of whichever realtime server code is merged in
// underneath it.

// ---- shared primitives ----

export type DeviceRole = "owner" | "viewer" | "source";
export type MessageLevel = "info" | "warn" | "error";
export type TaskRunState = "running" | "done" | "failed";
export type AutomationExecutorKind = "script" | "agent" | "hybrid";
export type AutomationRunState = "active" | "paused";

export interface DisplayHints {
  icon?: string;
  tint?: string;
  template?: string;
}

export interface TaskState {
  task_id: string;
  device_id: string;
  title?: string;
  state: TaskRunState;
  percent?: number;
  step?: string;
  message?: string;
  updated_at: number; // unix ms
  display?: DisplayHints;
}

export interface MetricSample {
  metric_id: string;
  value: string;
  label?: string;
  ts: number; // unix ms
  display?: DisplayHints;
  target?: string;
  min?: string;
  max?: string;
  alert_above?: string;
  alert_below?: string;
}

export interface MessageRecord {
  message_id: string;
  device_id: string;
  level: MessageLevel;
  text: string;
  occurred_at: number; // unix ms
}

export interface AutomationSchedule {
  kind: "interval";
  every_seconds: number;
}

export interface AutomationState {
  automation_id: string;
  name: string;
  executor_kind: AutomationExecutorKind;
  schedule: AutomationSchedule;
  state: AutomationRunState;
  last_run_at?: number; // unix ms
  /** Monotonic per-automation counter, incremented by POST
   * /v1/automations/:id/run. The resident agent runs the automation exactly
   * once when this advances beyond its last-consumed in-memory value — no
   * wall-clock comparison (v1-architecture.md §5.1, P0-4). 0 = never triggered. */
  run_request_id: number;
  /** Optional, DISPLAY ONLY — unix ms of the most recent /run. The agent keys
   * off run_request_id, never this timestamp. */
  run_requested_at?: number | null;
}

// ---- sr1 bearer token (v1-architecture.md §10) ----

/** `sr1_<space_id>_<secret>`. The `1` is a credential-format version,
 * independent of the HTTP API version — do not conflate the two. */
export const TOKEN_RE = /^sr1_([a-z0-9]{1,16})_[a-f0-9]{48}$/;

export function newToken(spaceId: string): string {
  const secret = [...crypto.getRandomValues(new Uint8Array(24))]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return `sr1_${spaceId}_${secret}`;
}

// ---- self-routing connect code (v1-architecture.md §10.5, P0-6) ----

/** 31-symbol confusable-free alphabet shared by space_id minting
 * (server/src/app.ts's newSpaceId()) and the connect-code display/secret
 * payload: digits 2-9, then a-z excluding i/l/o. Canonical form is
 * lowercase, matching SpaceId's charset; the connect code's display form is
 * this same set uppercased. */
export const CONNECT_CODE_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz";

/** Layout (v1-architecture.md §10.5): 'X' + space_id(10, uppercased 1:1) +
 * secret(9) + 'Z' = 21 chars total. Anchors are positional, not exclusive —
 * X and Z are themselves valid alphabet symbols and may recur inside the
 * payload. */
export const CONNECT_CODE_LENGTH = 21;
export const CONNECT_CODE_SPACE_ID_LEN = 10;
export const CONNECT_CODE_SECRET_LEN = 9;

/** Shape-only check (uppercase display form). Capture group 1 = space_id
 * (uppercased), group 2 = secret (uppercased); lowercase both to get the
 * canonical server-side values. */
export const CONNECT_CODE_RE = /^X([2-9A-HJ-KM-NP-Z]{10})([2-9A-HJ-KM-NP-Z]{9})Z$/;

export interface DecodedConnectCode {
  space_id: string; // lowercased
  secret: string; // lowercased
}

/** Normalizes (uppercase, strip non-alphabet chars is the CALLER's job for
 * OCR/paste input — this function only validates+decodes an already-
 * normalized candidate) and decodes a connect code; null if malformed. */
export function decodeConnectCode(code: string): DecodedConnectCode | null {
  const m = CONNECT_CODE_RE.exec(code.trim().toUpperCase());
  if (!m) return null;
  return { space_id: m[1].toLowerCase(), secret: m[2].toLowerCase() };
}

// ---- control-plane responses ----

/** POST /v1/spaces response. Returns device_id (P0-1): device_seq is scoped
 * to (device_id, space), so the creating owner device must know its own id to
 * uplink events (v1-architecture.md §2.2). */
export interface CreateSpaceResponse {
  space_id: string;
  device_id: string;
  owner_token: string;
}

/** POST /v1/join request body. `space` is REQUIRED (P0-6, v1-architecture.md
 * §10.5): the Worker routes on it directly (env.SPACE_HUB.getByName(space)),
 * zero KV lookup. Populated by decoding `code` locally for the connect-code
 * path, or carried explicitly by the self-host deep-link path — both send
 * this same shape. */
export interface JoinRequest {
  code: string; // 21-char self-routing connect code, see CONNECT_CODE_RE
  space: string;
  name?: string;
  platform?: string;
}

export interface JoinResponse {
  token: string;
  device_id: string;
  role: "viewer" | "source";
  space_id: string;
}

export interface InviteCreateRequest {
  role?: "viewer" | "source"; // default: viewer
}

/** `code` is the full self-routing layout: this SpaceHub's own space_id
 * prefixed to a freshly generated secret (v1-architecture.md §10.5). */
export interface InviteCreateResponse {
  code: string;
  expires_in: number; // seconds
  space_id: string;
}

// ---- role/endpoint authorization matrix (v1-architecture.md §3) ----

export type V1Route =
  | "GET /v1/realtime"
  | "POST /v1/events"
  | "GET /v1/snapshot"
  | "GET /v1/metrics/:id"
  | "GET /v1/metrics/:id/series"
  | "POST /v1/tasks/:id/commands"
  | "GET /v1/tasks/:id/log"
  | "POST /v1/tasks/:id/log"
  | "DELETE /v1/messages/:id"
  | "DELETE /v1/messages"
  | "GET /v1/automations"
  | "POST /v1/automations"
  | "PATCH /v1/automations/:id"
  | "DELETE /v1/automations/:id"
  | "POST /v1/automations/:id/run"
  | "POST /v1/invites"
  | "GET /v1/devices"
  | "DELETE /v1/devices/:id"
  | "PUT /v1/devices/self/push-tokens"
  | "PUT /v1/tasks/:id/live-activity-token";

/** `POST /v1/spaces` and `POST /v1/join` are intentionally absent — they are
 * unauthenticated and outside this matrix. v1 has exactly three roles
 * (`owner`/`viewer`/`source`); there is no `admin` role and no bare-secret
 * path (v1-architecture.md §3, §10.4).
 *
 * `owner` is a strict capability superset of BOTH `source` and `viewer` (P0-1):
 * it is `true` in every cell either of them is, including the source-only
 * uplinks. Verified by the assertion just below this table. */
export const ROUTE_ROLES: Record<V1Route, { source: boolean; viewer: boolean; owner: boolean }> = {
  "GET /v1/realtime": { source: true, viewer: true, owner: true },
  "POST /v1/events": { source: true, viewer: false, owner: true },
  "GET /v1/snapshot": { source: false, viewer: true, owner: true },
  "GET /v1/metrics/:id": { source: false, viewer: true, owner: true },
  "GET /v1/metrics/:id/series": { source: false, viewer: true, owner: true },
  "POST /v1/tasks/:id/commands": { source: false, viewer: true, owner: true },
  "GET /v1/tasks/:id/log": { source: false, viewer: true, owner: true },
  "POST /v1/tasks/:id/log": { source: true, viewer: false, owner: true },
  "DELETE /v1/messages/:id": { source: false, viewer: true, owner: true },
  "DELETE /v1/messages": { source: false, viewer: true, owner: true },
  "GET /v1/automations": { source: true, viewer: false, owner: true },
  "POST /v1/automations": { source: false, viewer: false, owner: true },
  "PATCH /v1/automations/:id": { source: false, viewer: true, owner: true },
  "DELETE /v1/automations/:id": { source: false, viewer: true, owner: true },
  "POST /v1/automations/:id/run": { source: false, viewer: true, owner: true },
  "POST /v1/invites": { source: false, viewer: true, owner: true },
  "GET /v1/devices": { source: false, viewer: true, owner: true },
  "DELETE /v1/devices/:id": { source: false, viewer: true, owner: true },
  "PUT /v1/devices/self/push-tokens": { source: false, viewer: true, owner: true },
  "PUT /v1/tasks/:id/live-activity-token": { source: false, viewer: true, owner: true },
};

export function isRouteAllowed(route: V1Route, role: DeviceRole): boolean {
  const allowed = ROUTE_ROLES[route];
  if (role === "source") return allowed.source;
  if (role === "viewer") return allowed.viewer;
  return allowed.owner;
}

/** Owner-superset invariant (P0-1): owner is allowed wherever source OR viewer
 * is. Runtime-checkable so a table typo can't silently narrow owner again. */
export function assertOwnerIsSuperset(): void {
  for (const [route, r] of Object.entries(ROUTE_ROLES)) {
    if ((r.source || r.viewer) && !r.owner) {
      throw new Error(`owner-superset invariant violated: ${route} allows source/viewer but not owner`);
    }
  }
}

// ---- transport capability switches (v1-architecture.md §8) ----

/** Strict allow-list parser shared by WS_TRANSPORT_ENABLED and
 * APNS_DELIVERY_ENABLED. A Cloudflare dashboard variable override always
 * arrives as a string at runtime regardless of the declared wrangler.jsonc
 * type — `Boolean("false")` is `true`, which this function must never do. */
export function parseTransportFlag(value: string | boolean | undefined): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return false;
  const normalized = value.trim().toLowerCase();
  return normalized === "true" || normalized === "1";
}

export interface Capabilities {
  ws_transport_enabled: boolean;
  apns_delivery_enabled: boolean;
  protocol_versions: number[];
}

/** Metric-frame rate limit is per-DEVICE in v1 (shared across the WS path and
 * POST /v1/events), a refinement of proto/realtime/SPEC.md §11's "per
 * connection" wording — the only definition that works across transports.
 * v1-architecture.md §4.3. */
export const METRIC_FRAME_RATE_PER_SEC_PER_DEVICE = 10;

// ---- GET /v1/snapshot ----

/** Non-folded presence markers, outside space_revision accounting
 * (v1-architecture.md §7.1). Drives the product status pill. */
export interface Presence {
  ingest_last_seen?: number; // unix ms; absent if never
  agent_last_seen?: number; // unix ms; absent if never
  sources_online: number; // 0 when WS_TRANSPORT_ENABLED is off
}

export interface Snapshot {
  space_revision: number;
  generated_at: string; // ISO 8601
  capabilities: Capabilities;
  presence: Presence;
  tasks: TaskState[];
  metrics: MetricSample[];
  messages: MessageRecord[];
  automations: AutomationState[];
}

// ---- metrics_current: persistent last-value + alert edge-state (v1-architecture.md §1.2.0, P0-2/P0-7) ----

export type ThresholdEdgeState = "armed" | "fired";

/** One persistent row per metric_id: last folded value/fields + per-threshold
 * alert edge-state. Survives DO eviction, so GET /v1/snapshot and
 * GET /v1/metrics/:id never lose an accepted value and a fired threshold stays
 * fired (no duplicate alert on rebuild). Not revisioned. */
export interface MetricsCurrentRow {
  metric_id: string;
  value: string;
  fields: MetricSample; // last folded label/display/target/min/max/alert lines/ts
  alert_state: Partial<Record<"above" | "below", ThresholdEdgeState>>;
  updated_at: number; // unix ms
}

/** Shared cap for BOTH the in-memory metricsCache hot cache AND the
 * persistent metrics_current table — one number, not two independently-
 * drifting limits (P0-7, v1-architecture.md §1.2.0). LRU eviction
 * (least-recently-updated) on overflow in both places. */
export const METRIC_CACHE_MAX_METRICS = 256;

/** Per-space debounce window for persisting ROUTINE (non-alert-edge)
 * metrics_current writes; last-value-wins within the window (P0-7,
 * v1-architecture.md §1.2.0). An alert edge transition (armed<->fired)
 * always bypasses this and persists immediately in the same transaction as
 * its triggering sample — this constant governs write-timing only, never
 * alert correctness. Within the 5-15s band this freeze specifies. */
export const METRICS_CURRENT_DOWNSAMPLE_MS = 10_000;

// ---- metric_series: persistent tiered history, non-folded (v1-architecture.md §1.2.1) ----

export type SeriesTier = "raw" | "hour" | "day";
export type SeriesRange = "1h" | "6h" | "1d" | "1w" | "1m" | "1y";
export const SERIES_RANGES: SeriesRange[] = ["1h", "6h", "1d", "1w", "1m", "1y"];

/** GET /v1/metrics/:id/series response element. ts = bucket timestamp,
 * value = bucket's last value. Never participates in space_revision. */
export interface MetricSeriesPoint {
  ts: number; // unix ms
  value: number;
}

export const SERIES_RAW_CAP = 720;
export const SERIES_HOUR_CAP = 768;
export const SERIES_DAY_CAP = 400;

// ---- task_logs: persistent per-task log tail, non-folded (v1-architecture.md §1.2.2) ----

/** Body of POST /v1/tasks/:id/log (source-only, best-effort). Appended to a
 * TASK_LOG_WINDOW-line ring buffer; never bumps space_revision. */
export interface TaskLogRequest {
  lines: string[];
}

export const TASK_LOG_WINDOW = 100;

// ---- POST /v1/events (v1-architecture.md §4) ----

export interface TaskEventBody {
  device_id: string;
  device_seq: number;
  task_id: string;
  kind: "started" | "progress" | "step" | "done" | "failed";
  occurred_at: number; // unix ms
  title?: string;
  percent?: number;
  step?: string;
  message?: string;
  display?: DisplayHints;
}

export interface MessageEventBody {
  device_id: string;
  device_seq: number;
  message_id: string;
  level: MessageLevel;
  text: string;
  occurred_at: number; // unix ms
  automation_id?: string;
}

export interface MetricFrameBody {
  device_id: string;
  metrics: MetricSample[];
}

export type EventEnvelopeType = "task.event" | "message.event" | "metric.frame";

export interface EventEnvelope {
  type: EventEnvelopeType;
  id: string;
  ts: number; // unix ms
  body: TaskEventBody | MessageEventBody | MetricFrameBody;
}

export interface EventsRequest {
  events: EventEnvelope[]; // may be empty ([]) for a pure command-poll heartbeat
  /** Optional. The task_id this uplink's process owns; scopes the response
   * `commands[]` to that task (plus broadcast commands). Omit for a general
   * source with no task partitioning (v1-architecture.md §1.4, §4.1). */
  for_task_id?: string;
  /** Optional (P0-5, fetch-then-ack). command_ids this device has durably
   * handed off to its local process controller since it last acked —
   * receiving a command is not sufficient grounds to ack it. Processed
   * BEFORE this same request's `commands[]` response is computed. An id
   * that is unknown / not owned by this device / already acked / expired is
   * silently ignored (idempotent no-op) (v1-architecture.md §1.4, §4.1). */
  ack_command_ids?: string[];
}

export interface AckedPair {
  device_id: string;
  device_seq: number;
}

export type EventResultStatus = "applied" | "duplicate" | "stale" | "rejected";

export interface EventResult {
  index: number;
  type: EventEnvelopeType;
  status: EventResultStatus;
  device_seq?: number;
  revision?: number;
  error?: { code: string; message: string };
}

/** A reverse-control command read from CommandStore for an HTTP-only
 * source, piggybacked on the POST /v1/events response (v1-architecture.md
 * §1.4, §4.1). Shaped like the WS `command` envelope body; the HTTP-transport
 * equivalent of a WS `command` frame — same CommandStore rows. P0-5
 * fetch-then-ack: included on EVERY poll from the owning device until acked
 * (EventsRequest.ack_command_ids) or expired — inclusion here never by
 * itself marks a row delivered. command_id is the local-execution
 * idempotency key regardless of ack state. */
export interface PendingCommand {
  command_id: string;
  origin: "viewer";
  action: TaskCommandAction;
  task_id: string;
  ttl_ms: number;
  origin_ts: number; // unix ms; viewer's original send time, ttl_ms measured from it
}

export interface EventsResponse {
  space_revision: number;
  acked: AckedPair[];
  results: EventResult[];
  /** Omitted or [] when the source has no pending commands. Computed AFTER
   * this same request's ack_command_ids is applied (P0-5, v1-architecture.md
   * §1.4/§4.1) — a request that acks and polls in one call never gets back
   * what it just acked. */
  commands?: PendingCommand[];
}

/** Max envelopes accepted per POST /v1/events request (v1-architecture.md §4.1). */
export const EVENTS_BATCH_MAX = 500;

// ---- automations control plane (v1-architecture.md §5, §6) ----

export interface AutomationCreateRequest {
  automation_id?: string;
  name: string;
  executor_kind: AutomationExecutorKind;
  schedule: { every_seconds: number };
  state?: AutomationRunState;
}

export interface AutomationMintResult {
  revision: number;
  automation: AutomationState | null;
}

// ---- reverse control (v1-architecture.md §4.4, §5.1, §6) ----

export type TaskCommandAction = "pause" | "resume" | "stop";

/** Body of POST /v1/tasks/:id/commands. task_id and issued_by_device_id are
 * NOT here — the server takes them from the path param and the authenticated
 * identity respectively (§4.4). `command_id` (or the Idempotency-Key header)
 * maps to the WS command_id so a retried POST is deduped (§6). */
export interface TaskCommandRequest {
  action: TaskCommandAction;
  ttl_ms?: number; // optional; default DEFAULT_COMMAND_TTL_MS
  command_id?: string; // optional; alternative to the Idempotency-Key header
}

// POST /v1/automations/:id/run takes no request body: it increments the
// automation's monotonic run_request_id counter (v1-architecture.md §5.1, P0-4)
// rather than enqueuing a command, so there is no AutomationRunRequest type. An
// optional Idempotency-Key header dedups a network retry to one increment.

export const DEFAULT_COMMAND_TTL_MS = 60_000;
export const COMMAND_TTL_MIN_MS = 1;
export const COMMAND_TTL_MAX_MS = 86_400_000;

// ---- push_outbox (v1-apns-outbox.md §3) ----

export type PushKind = "push_to_start" | "activity_update" | "activity_end" | "alert";
export type PushStatus = "pending" | "sent" | "permanent_failure" | "expired";

export interface PushOutboxRow {
  push_id: string;
  kind: PushKind;
  device_id: string;
  subject_id: string;
  generation: number | null;
  revision: number;
  coalesce_key: string;
  payload: string; // JSON-encoded APNs aps payload
  status: PushStatus;
  attempts: number;
  next_attempt_at: number; // unix ms
  dispatch_started_at: number | null; // unix ms
  last_error: string | null;
  created_at: number; // unix ms
  expires_at: number; // unix ms; pre-dispatch drop deadline
  terminal_at: number | null; // unix ms; null while pending; set on move to a terminal status; retention sweep keys on it (v1-apns-outbox.md §3, §6)
}

// MAX_TRANSIENT_ATTEMPTS is NOT re-exported here: the single live copy is in
// `apns.ts` (the retry-budget owner), which both space-hub.ts and the tests
// import. A second copy here was dead and risked drift.
export const AMBIGUOUS_DISPATCH_GRACE_MS = 60_000;
export const PUSH_OUTBOX_SPACE_ROW_CAP = 2000;
export const PUSH_OUTBOX_DEVICE_ROW_CAP = 200;
