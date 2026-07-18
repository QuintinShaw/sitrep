// Storage abstraction + pure state reducers. Cloudflare deployments back the
// Store with a Durable Object; the Docker/Node deployment uses MemoryStore
// (SQLite later). Nothing outside the adapters may touch a concrete backend,
// and all state transitions go through the reducers below so every backend
// behaves identically.

export type EventKind =
  | "task.start"
  | "task.progress"
  | "task.step"
  | "task.done"
  | "task.fail"
  | "metric.update"
  | "message.send"
  | "task.log"; // daemon-generated: passthrough output tail, joined by \n

export interface SitrepEvent {
  kind: EventKind;
  source_id: string;
  ts: string;
  title?: string;
  percent?: number;
  step?: string;
  key?: string;
  value?: string;
  label?: string;
  text?: string;
  level?: "info" | "warn" | "error";
  // presentation hints (docs/design/presentation.md)
  icon?: string;
  tint?: string;
  template?: string;
  // metric display metadata: renderers derive gauges from value+target
  target?: string;
  min?: string;
  max?: string;
  // metric alert lines: the server edge-detects crossings (see metricViolation)
  alert_above?: string;
  alert_below?: string;
}

export interface TaskState {
  source_id: string;
  title: string;
  status: "running" | "done" | "failed";
  percent?: number;
  step?: string;
  updated_at: string;
  started_at?: string;
  icon?: string;
  tint?: string;
  template?: string;
}

export interface MetricState {
  key: string;
  value: string;
  label?: string;
  updated_at: string;
  icon?: string;
  tint?: string;
  template?: string; // "number" (default) | "gauge" | "bar" | "spark"
  target?: string;
  min?: string;
  max?: string;
  /** Metric-owned threshold rules. Crossing one is
   * edge-detected server-side: notify once, re-arm when the value returns. */
  alert_above?: string;
  alert_below?: string;
  /** Source that last updated this metric — links it to its automation ("a<id>")
   * so viewers reach schedule controls from the metric's detail screen. */
  source?: string;
  /** Ring buffer of recent numeric values (oldest first), for sparklines. */
  history?: number[];
}

/** Which alert line (if any) the metric's current value violates. Pure, so
 * every backend edge-detects identically: fire on false→true, re-arm on
 * true→false. */
export function metricViolation(m: MetricState): { line: string; dir: "above" | "below" } | undefined {
  const v = Number(m.value);
  if (!Number.isFinite(v)) return undefined;
  const above = m.alert_above !== undefined ? Number(m.alert_above) : undefined;
  const below = m.alert_below !== undefined ? Number(m.alert_below) : undefined;
  if (above !== undefined && Number.isFinite(above) && v > above) return { line: m.alert_above!, dir: "above" };
  if (below !== undefined && Number.isFinite(below) && v < below) return { line: m.alert_below!, dir: "below" };
  return undefined;
}

/** Notification text for a threshold crossing. */
export function violationText(m: MetricState, viol: { line: string; dir: "above" | "below" }): string {
  const name = m.label ?? m.key;
  return viol.dir === "above"
    ? `${name} ${m.value}，高于提醒线 ${viol.line}`
    : `${name} ${m.value}，低于提醒线 ${viol.line}`;
}

export const METRIC_HISTORY_CAP = 50;

// ---- timestamped series (the real time axis; `history` stays as the
// lightweight sparkline ring) ----

export interface SeriesPoint {
  t: string; // ISO timestamp
  v: number;
}

/** Tiered retention: raw points recent, hourly for weeks, daily for a year.
 * Buckets keep the LAST value of their window (metrics are levels, not
 * counters). */
export const SERIES_RAW_CAP = 720;
export const SERIES_HOUR_CAP = 768; // 32 days of hours
export const SERIES_DAY_CAP = 400;

export interface MetricSeries {
  raw: SeriesPoint[];
  hour: SeriesPoint[];
  day: SeriesPoint[];
}

const bucketStart = (iso: string, ms: number): string => new Date(Math.floor(Date.parse(iso) / ms) * ms).toISOString();

/** Pure: fold one update into all three tiers. */
export function appendSeries(prev: MetricSeries | undefined, ts: string, value: string): MetricSeries | undefined {
  const v = Number(value);
  if (!Number.isFinite(v) || !Number.isFinite(Date.parse(ts))) return prev;
  const s: MetricSeries = { raw: [...(prev?.raw ?? [])], hour: [...(prev?.hour ?? [])], day: [...(prev?.day ?? [])] };
  s.raw.push({ t: ts, v });
  if (s.raw.length > SERIES_RAW_CAP) s.raw.splice(0, s.raw.length - SERIES_RAW_CAP);
  for (const [tier, ms, cap] of [
    ["hour", 3_600_000, SERIES_HOUR_CAP],
    ["day", 86_400_000, SERIES_DAY_CAP],
  ] as const) {
    const arr = s[tier];
    const t = bucketStart(ts, ms);
    if (arr.length > 0 && arr[arr.length - 1].t === t) arr[arr.length - 1] = { t, v };
    else {
      arr.push({ t, v });
      if (arr.length > cap) arr.splice(0, arr.length - cap);
    }
  }
  return s;
}

export type SeriesRange = "1h" | "6h" | "1d" | "1w" | "1m" | "1y";

export const SERIES_RANGES: SeriesRange[] = ["1h", "6h", "1d", "1w", "1m", "1y"];

/** Pure: pick the right tier + window for a range. */
export function selectSeries(s: MetricSeries | undefined, range: SeriesRange, now = Date.now()): SeriesPoint[] {
  if (!s) return [];
  const windows: Record<SeriesRange, [number, SeriesPoint[]]> = {
    "1h": [3_600_000, s.raw],
    "6h": [6 * 3_600_000, s.raw],
    "1d": [86_400_000, s.raw],
    "1w": [7 * 86_400_000, s.hour],
    "1m": [30 * 86_400_000, s.hour],
    "1y": [365 * 86_400_000, s.day],
  };
  const [ms, tier] = windows[range];
  const cutoff = now - ms;
  let points = tier.filter((p) => Date.parse(p.t) >= cutoff);
  // Raw may not span a whole day yet after fresh installs — fall back to
  // hourly so the chart still covers the asked-for window.
  if (range === "1d" && s.raw.length > 0 && Date.parse(s.raw[0].t) > cutoff) {
    const hourly = s.hour.filter((p) => Date.parse(p.t) >= cutoff);
    if (hourly.length > points.length) points = hourly;
  }
  return points;
}

/** Presence: when did we last hear from the computer side. */
export interface PresenceInfo {
  /** Last event ingest (any `sitrep run`/watch upload). */
  ingest_last_seen?: string;
  /** Last agent registry poll (the scheduler's heartbeat). */
  agent_last_seen?: string;
}

/** Append the new value to the metric's numeric history (no-op for
 * non-numeric values). */
export function pushHistory(prev: MetricState | undefined, next: MetricState): MetricState {
  const n = Number(next.value);
  if (!Number.isFinite(n)) return next;
  const history = [...(prev?.history ?? []), n];
  if (history.length > METRIC_HISTORY_CAP) history.splice(0, history.length - METRIC_HISTORY_CAP);
  return { ...next, history };
}

/** Fired events, newest first, capped. */
export interface EventLogEntry {
  /** Stable id so viewers can delete entries. */
  id?: string;
  text: string;
  level: "info" | "warn" | "error";
  ts: string;
  /** Emitting source ("a<id>" for automation runs) — lets clients group the
   * log by which automation/task produced each entry. */
  source?: string;
}

export function newEventId(): string {
  return [...crypto.getRandomValues(new Uint8Array(8))].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export const EVENT_LOG_CAP = 300;
export const TASK_LOG_CAP = 100;

export function appendTaskLog(prev: string[] | undefined, text: string): string[] {
  const lines = text.split("\n").map((l) => l.slice(0, 300)).filter((l) => l.length > 0);
  const log = [...(prev ?? []), ...lines];
  if (log.length > TASK_LOG_CAP) log.splice(0, log.length - TASK_LOG_CAP);
  return log;
}

/** A scheduled automation: the local agent runs these (the scheduler lives in
 * `sitrep agent` on the computer); the server is the registry so every
 * device can see and edit them. Interval/enabled are viewer-editable —
 * the command line is not (code never flows from phones to computers). */
export interface AutomationDef {
  id: string;
  name: string;
  command: string[]; // argv, executed as-is by the agent
  executor_kind?: "script" | "agent" | "hybrid";
  every_s: number;
  enabled: boolean;
  created_at: string;
  updated_at: string;
  last_run?: string;
  /** Viewer-requested immediate run: the agent runs the automation when this
   * is newer than last_run. Phones trigger execution without ever being
   * able to say WHAT runs. */
  run_requested_at?: string;
}

/** Metric-owned viewer preference, keyed by the metric's stable id. */
export interface MetricPreference {
  icon?: string;
  tint?: string;
  template?: string;
  level?: "info" | "warn" | "error" | "off";
  alert_above?: string;
  alert_below?: string;
}

export function mergeMetric(m: MetricState, prefs: Record<string, MetricPreference>): MetricState {
  const p = prefs[m.key];
  if (!p) return m;
  return {
    ...m,
    icon: p.icon ?? m.icon,
    tint: p.tint ?? m.tint,
    template: p.template ?? m.template,
    alert_above: p.alert_above ?? m.alert_above,
    alert_below: p.alert_below ?? m.alert_below,
  };
}

export interface Store {
  apply(events: SitrepEvent[]): Promise<void>;
  tasks(): Promise<TaskState[]>;
  metrics(): Promise<MetricState[]>;
  eventLog(): Promise<EventLogEntry[]>;
  /** Recent output lines of a task (ring buffer, newest last). */
  taskLog(sourceId: string): Promise<string[]>;
  /** Timestamped history for real charts (tiered retention). */
  metricSeries(key: string, range: SeriesRange): Promise<SeriesPoint[]>;
  /** Presence heartbeats from the computer side. */
  stampPresence(kind: "ingest" | "agent", ts: string): Promise<void>;
  presence(): Promise<PresenceInfo>;
  /** Remove event-log entries ("all" clears the log). */
  deleteEvents(ids: string[] | "all"): Promise<void>;
  automations(): Promise<AutomationDef[]>;
  putAutomation(automation: AutomationDef): Promise<void>;
  patchAutomation(
    id: string,
    patch: Partial<Pick<AutomationDef, "name" | "every_s" | "enabled" | "last_run" | "run_requested_at">>,
  ): Promise<AutomationDef | undefined>;
  deleteAutomation(id: string): Promise<void>;
  metricPrefs(): Promise<Record<string, MetricPreference>>;
  setMetricPref(metricId: string, pref: MetricPreference | null): Promise<void>;
  deleteTask(sourceId: string): Promise<void>;
  deleteMetric(key: string): Promise<void>;
}

/** Hide finished tasks after this long; clients pass ?all=1 to see history.
 * A day, not an hour: the tab going empty reads as a dead app. */
export const FINISHED_TASK_TTL_MS = 24 * 60 * 60 * 1000;

export function visibleTasks(all: TaskState[], includeAll: boolean, now = Date.now()): TaskState[] {
  if (includeAll) return all;
  return all.filter(
    (t) => t.status === "running" || now - Date.parse(t.updated_at) < FINISHED_TASK_TTL_MS,
  );
}

/** Pure reducer: previous task state + event → next task state (or unchanged). */
export function reduceTask(prev: TaskState | undefined, ev: SitrepEvent): TaskState | undefined {
  switch (ev.kind) {
    case "task.start":
      return {
        source_id: ev.source_id,
        title: ev.title || ev.source_id,
        status: "running",
        updated_at: ev.ts,
        started_at: prev?.started_at ?? ev.ts,
        icon: ev.icon ?? prev?.icon,
        tint: ev.tint ?? prev?.tint,
        template: ev.template ?? prev?.template,
      };
    case "task.progress":
    case "task.step": {
      const t: TaskState = prev ?? {
        source_id: ev.source_id,
        title: ev.source_id,
        status: "running",
        updated_at: ev.ts,
      };
      return {
        ...t,
        percent: ev.percent ?? t.percent,
        step: ev.step || t.step,
        updated_at: ev.ts,
      };
    }
    case "task.done":
    case "task.fail":
      if (!prev) return undefined;
      return {
        ...prev,
        status: ev.kind === "task.done" ? "done" : "failed",
        step: ev.text || prev.step,
        updated_at: ev.ts,
      };
    default:
      return prev;
  }
}

/** Pure reducer: metric.update event → metric state (undefined for non-metric events). */
export function reduceMetric(ev: SitrepEvent): MetricState | undefined {
  if (ev.kind !== "metric.update" || !ev.key || ev.value === undefined) return undefined;
  return {
    key: ev.key,
    value: ev.value,
    label: ev.label,
    updated_at: ev.ts,
    icon: ev.icon,
    tint: ev.tint,
    template: ev.template,
    target: ev.target,
    min: ev.min,
    max: ev.max,
    alert_above: ev.alert_above,
    alert_below: ev.alert_below,
    source: ev.source_id,
  };
}

/** In-memory store: Node adapter + tests. Single-tenant, no persistence. */
export class MemoryStore implements Store {
  private taskMap = new Map<string, TaskState>();
  private metricMap = new Map<string, MetricState>();
  private log: EventLogEntry[] = [];
  private prefMap = new Map<string, MetricPreference>();
  private seriesMap = new Map<string, MetricSeries>();
  private presenceInfo: PresenceInfo = {};

  async apply(events: SitrepEvent[]): Promise<void> {
    for (const ev of events) {
      const task = reduceTask(this.taskMap.get(ev.source_id), ev);
      if (task) this.taskMap.set(ev.source_id, task);
      const metric = reduceMetric(ev);
      if (metric) {
        const effective = mergeMetric(metric, Object.fromEntries(this.prefMap));
        const prev = this.metricMap.get(effective.key);
        // Threshold edge: notify exactly once per excursion.
        const viol = metricViolation(effective);
        if (viol && (!prev || !metricViolation(prev))) {
          this.log.unshift({ id: newEventId(), text: violationText(effective, viol), level: "warn", ts: ev.ts, source: ev.source_id });
        }
        this.metricMap.set(effective.key, pushHistory(prev, effective));
        const series = appendSeries(this.seriesMap.get(effective.key), ev.ts, effective.value);
        if (series) this.seriesMap.set(effective.key, series);
      }
      switch (ev.kind) {
        case "message.send":
          if (ev.text) {
            this.log.unshift({ id: newEventId(), text: ev.text, level: ev.level ?? "info", ts: ev.ts, source: ev.source_id });
          }
          break;
        case "task.log":
          if (ev.text) {
            this.taskLogs.set(ev.source_id, appendTaskLog(this.taskLogs.get(ev.source_id), ev.text));
          }
          break;
      }
      if (this.log.length > EVENT_LOG_CAP) this.log.length = EVENT_LOG_CAP;
    }
  }

  async eventLog(): Promise<EventLogEntry[]> {
    return this.log;
  }

  async metricPrefs(): Promise<Record<string, MetricPreference>> {
    return Object.fromEntries(this.prefMap);
  }

  async setMetricPref(metricId: string, pref: MetricPreference | null): Promise<void> {
    if (pref === null) this.prefMap.delete(metricId);
    else this.prefMap.set(metricId, pref);
  }

  private taskLogs = new Map<string, string[]>();

  async taskLog(sourceId: string): Promise<string[]> {
    return this.taskLogs.get(sourceId) ?? [];
  }

  private automationMap = new Map<string, AutomationDef>();

  async automations(): Promise<AutomationDef[]> {
    return [...this.automationMap.values()];
  }

  async putAutomation(automation: AutomationDef): Promise<void> {
    this.automationMap.set(automation.id, automation);
  }

  async patchAutomation(
    id: string,
    patch: Partial<Pick<AutomationDef, "name" | "every_s" | "enabled" | "last_run" | "run_requested_at">>,
  ): Promise<AutomationDef | undefined> {
    const automation = this.automationMap.get(id);
    if (!automation) return undefined;
    const next = { ...automation, ...patch, updated_at: new Date().toISOString() };
    this.automationMap.set(id, next);
    return next;
  }

  async deleteAutomation(id: string): Promise<void> {
    this.automationMap.delete(id);
  }

  async tasks(): Promise<TaskState[]> {
    return [...this.taskMap.values()];
  }

  async metrics(): Promise<MetricState[]> {
    return [...this.metricMap.values()];
  }

  async deleteTask(sourceId: string): Promise<void> {
    this.taskMap.delete(sourceId);
  }

  async deleteMetric(key: string): Promise<void> {
    this.metricMap.delete(key);
    this.seriesMap.delete(key);
  }

  async metricSeries(key: string, range: SeriesRange): Promise<SeriesPoint[]> {
    return selectSeries(this.seriesMap.get(key), range);
  }

  async stampPresence(kind: "ingest" | "agent", ts: string): Promise<void> {
    if (kind === "ingest") this.presenceInfo.ingest_last_seen = ts;
    else this.presenceInfo.agent_last_seen = ts;
  }

  async presence(): Promise<PresenceInfo> {
    return this.presenceInfo;
  }

  async deleteEvents(ids: string[] | "all"): Promise<void> {
    if (ids === "all") this.log.length = 0;
    else this.log = this.log.filter((e) => !e.id || !ids.includes(e.id));
  }
}
