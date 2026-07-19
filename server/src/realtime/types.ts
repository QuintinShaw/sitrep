// Hand-written TypeScript mirror of proto/realtime/*.schema.json (protocol
// v1, frozen). This file is a DERIVED artifact: proto/realtime/ is the sole
// source of truth and every shape here must match its schemas field for
// field. Any semantic difference from proto/realtime/ is a bug in this file,
// never an intentional deviation — do not "improve" the protocol here.
//
// proto/realtime/ itself MUST NOT be edited as part of server work.

export type DeviceRole = "source" | "viewer";
export type MessageLevel = "info" | "warn" | "error";
export type TaskKind = "started" | "progress" | "step" | "done" | "failed";
export type TaskRunState = "running" | "done" | "failed";
export type AutomationExecutorKind = "script" | "agent" | "hybrid";
export type AutomationRunState = "active" | "paused";
export type SubscribeTopic = "task" | "metric" | "message";
export type CommandOrigin = "viewer" | "server";
export type CommandAction = "pause" | "resume" | "stop" | "run_now" | "throttle" | "resume_rate";
export type ConfigEventKind = "automation.upserted" | "automation.removed";

export type ErrorCode =
  | "version_unsupported"
  | "hello_required"
  | "unauthenticated"
  | "unauthorized"
  | "malformed"
  | "rate_limited"
  | "frame_too_large"
  | "batch_too_large"
  | "revision_unavailable"
  | "command_expired"
  | "sequence_gap"
  | "superseded"
  | "internal_error";

export const MESSAGE_TYPES = [
  "hello",
  "resume",
  "snapshot",
  "delta",
  "ack",
  "task.event",
  "message.event",
  "metric.frame",
  "config.event",
  "subscribe",
  "unsubscribe",
  "interest.renew",
  "command",
  "error",
] as const;

export type MessageType = (typeof MESSAGE_TYPES)[number];

// ---- common.schema.json $defs ----

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
  updated_at: number;
  display?: DisplayHints;
}

export interface MetricSample {
  metric_id: string;
  value: string;
  label?: string;
  ts: number;
  display?: DisplayHints;
  target?: string;
  min?: string;
  max?: string;
  alert_above?: string;
  alert_below?: string;
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
  last_run_at?: number;
}

export interface MessageRecord {
  message_id: string;
  device_id: string;
  level: MessageLevel;
  text: string;
  occurred_at: number;
}

// ---- message bodies ----

export interface HelloOfferBody {
  stage: "offer";
  device_id: string;
  role: DeviceRole;
  protocol_versions: number[];
  capabilities?: string[];
}

export interface HelloAcceptBody {
  stage: "accept";
  protocol_version: number;
  session_id: string;
  heartbeat_interval_ms: number;
  capabilities?: string[];
}

export type HelloBody = HelloOfferBody | HelloAcceptBody;

export interface ResumeBody {
  last_revision: number;
}

export interface SnapshotBody {
  revision: number;
  part: number;
  final: boolean;
  tasks: TaskState[];
  metrics: MetricSample[];
  messages: MessageRecord[];
  automations: AutomationState[];
}

export interface TaskEventBody {
  device_id: string;
  device_seq: number;
  task_id: string;
  kind: TaskKind;
  occurred_at: number;
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
  occurred_at: number;
  automation_id?: string;
}

export interface ConfigEventBody {
  kind: ConfigEventKind;
  automation_id: string;
  automation?: AutomationState;
  occurred_at: number;
}

export type DeltaEventItem =
  | { event_type: "task.event"; event: TaskEventBody }
  | { event_type: "message.event"; event: MessageEventBody }
  | { event_type: "config.event"; event: ConfigEventBody };

export interface DeltaBody {
  from_revision: number;
  to_revision: number;
  events: DeltaEventItem[];
}

export interface AckedPair {
  device_id: string;
  device_seq: number;
}

export interface LeaseInfo {
  expires_at: number;
}

export interface AckBody {
  acked?: AckedPair[];
  in_reply_to?: string;
  lease?: LeaseInfo;
}

export interface MetricFrameBody {
  device_id: string;
  metrics: MetricSample[];
}

export interface SubscribeBody {
  topics?: SubscribeTopic[];
}

export type UnsubscribeBody = Record<string, never>;

export interface InterestRenewBody {
  topics?: SubscribeTopic[];
}

export interface CommandBody {
  command_id: string;
  origin: CommandOrigin;
  issued_by_device_id?: string;
  action: CommandAction;
  task_id?: string;
  automation_id?: string;
  target_device_id?: string;
  ttl_ms: number;
  params?: Record<string, unknown>;
}

export interface ErrorBody {
  code: ErrorCode;
  message: string;
  in_reply_to?: string;
  retryable: boolean;
  fatal: boolean;
}

// ---- envelope ----

export interface Envelope<T extends MessageType = MessageType, B = unknown> {
  type: T;
  id: string;
  ts: number;
  body: B;
}

export type HelloEnvelope = Envelope<"hello", HelloBody>;
export type ResumeEnvelope = Envelope<"resume", ResumeBody>;
export type SnapshotEnvelope = Envelope<"snapshot", SnapshotBody>;
export type DeltaEnvelope = Envelope<"delta", DeltaBody>;
export type AckEnvelope = Envelope<"ack", AckBody>;
export type TaskEventEnvelope = Envelope<"task.event", TaskEventBody>;
export type MessageEventEnvelope = Envelope<"message.event", MessageEventBody>;
export type MetricFrameEnvelope = Envelope<"metric.frame", MetricFrameBody>;
export type ConfigEventEnvelope = Envelope<"config.event", ConfigEventBody>;
export type SubscribeEnvelope = Envelope<"subscribe", SubscribeBody>;
export type UnsubscribeEnvelope = Envelope<"unsubscribe", UnsubscribeBody>;
export type InterestRenewEnvelope = Envelope<"interest.renew", InterestRenewBody>;
export type CommandEnvelope = Envelope<"command", CommandBody>;
export type ErrorEnvelope = Envelope<"error", ErrorBody>;

export type AnyEnvelope =
  | HelloEnvelope
  | ResumeEnvelope
  | SnapshotEnvelope
  | DeltaEnvelope
  | AckEnvelope
  | TaskEventEnvelope
  | MessageEventEnvelope
  | MetricFrameEnvelope
  | ConfigEventEnvelope
  | SubscribeEnvelope
  | UnsubscribeEnvelope
  | InterestRenewEnvelope
  | CommandEnvelope
  | ErrorEnvelope;
