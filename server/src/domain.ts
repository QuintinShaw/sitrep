import type {
  EventLogEntry,
  MetricState,
  PresenceInfo,
  TaskState,
  AutomationDef,
} from "./store.ts";

export type Severity = "info" | "warning" | "critical";
export type AutomationExecutorKind = "script" | "agent" | "hybrid";

export interface MetricThreshold {
  id: string;
  direction: "above" | "below";
  value: string;
  severity: Severity;
}

export interface MetricView {
  id: string;
  value: string;
  label?: string;
  updated_at: string;
  automation_id?: string;
  display: {
    icon?: string;
    tint?: string;
    style?: string;
    target?: string;
    min?: string;
    max?: string;
  };
  thresholds: MetricThreshold[];
  history?: number[];
}

export interface AutomationView {
  id: string;
  name: string;
  executor: { kind: AutomationExecutorKind };
  schedule: { kind: "interval"; every_seconds: number };
  state: "active" | "paused";
  last_run_at?: string;
  run_requested_at?: string;
  capabilities: {
    run_now: boolean;
    edit_schedule: boolean;
    pause: boolean;
    delete: boolean;
  };
}

export interface MessageView {
  id: string;
  body: string;
  severity: Severity;
  created_at: string;
  automation_id?: string;
}

export interface SpaceSnapshot {
  version: 2;
  generated_at: string;
  presence: PresenceInfo;
  tasks: TaskState[];
  metrics: MetricView[];
  messages: MessageView[];
  automations: AutomationView[];
}

const automationID = (source?: string): string | undefined => {
  if ((!source?.startsWith("a") && !source?.startsWith("w")) || source.length === 1) return undefined;
  return source.slice(1);
};

const severity = (level: EventLogEntry["level"]): Severity => {
  if (level === "error") return "critical";
  if (level === "warn") return "warning";
  return "info";
};

/**
 * Migration boundary from the v1 persistence layout to the product domain.
 * New clients consume only this model; no client is allowed to infer entity
 * ownership from labels or names.
 */
export function makeSnapshot(input: {
  now: string;
  presence: PresenceInfo;
  tasks: TaskState[];
  metrics: MetricState[];
  events: EventLogEntry[];
  automations: AutomationDef[];
}): SpaceSnapshot {
  return {
    version: 2,
    generated_at: input.now,
    presence: input.presence,
    tasks: input.tasks,
    metrics: input.metrics.map((metric) => {
      const thresholds: MetricThreshold[] = [];
      if (metric.alert_above !== undefined) {
        thresholds.push({ id: "above", direction: "above", value: metric.alert_above, severity: "warning" });
      }
      if (metric.alert_below !== undefined) {
        thresholds.push({ id: "below", direction: "below", value: metric.alert_below, severity: "warning" });
      }
      return {
        id: metric.key,
        value: metric.value,
        label: metric.label,
        updated_at: metric.updated_at,
        automation_id: automationID(metric.source),
        display: {
          icon: metric.icon,
          tint: metric.tint,
          style: metric.template,
          target: metric.target,
          min: metric.min,
          max: metric.max,
        },
        thresholds,
        history: metric.history,
      };
    }),
    messages: input.events.map((event) => ({
      id: event.id ?? `${event.ts}:${event.text}`,
      body: event.text,
      severity: severity(event.level),
      created_at: event.ts,
      automation_id: automationID(event.source),
    })),
    automations: input.automations.map((automation) => ({
      id: automation.id,
      name: automation.name,
      executor: { kind: automation.executor_kind ?? "script" },
      schedule: { kind: "interval", every_seconds: automation.every_s },
      state: automation.enabled ? "active" : "paused",
      last_run_at: automation.last_run,
      run_requested_at: automation.run_requested_at,
      capabilities: { run_now: true, edit_schedule: true, pause: true, delete: true },
    })),
  };
}
