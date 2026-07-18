// SQLite-backed Store for the Node/Docker deployment (node:sqlite, built in
// since Node 22 — zero native deps). One table per concern, JSON columns for
// the flexible parts; identical semantics to the DO/Memory stores.
import { DatabaseSync } from "node:sqlite";
import {
  appendSeries,
  appendTaskLog,
  EVENT_LOG_CAP,
  pushHistory,
  metricViolation,
  newEventId,
  mergeMetric,
  reduceMetric,
  selectSeries,
  violationText,
  reduceTask,
  type MetricSeries,
  type PresenceInfo,
  type SeriesPoint,
  type SeriesRange,
  type EventLogEntry,
  type MetricState,
  type MetricPreference,
  type SitrepEvent,
  type Store,
  type TaskState,
  type AutomationDef,
} from "./store.ts";

export class SqliteStore implements Store {
  private db: DatabaseSync;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS kv (
        ns TEXT NOT NULL,
        k TEXT NOT NULL,
        v TEXT NOT NULL,
        PRIMARY KEY (ns, k)
      );
    `);
  }

  private get(ns: string, k: string): unknown {
    const row = this.db.prepare("SELECT v FROM kv WHERE ns = ? AND k = ?").get(ns, k) as
      | { v: string }
      | undefined;
    return row ? JSON.parse(row.v) : undefined;
  }

  private put(ns: string, k: string, v: unknown): void {
    this.db
      .prepare("INSERT OR REPLACE INTO kv (ns, k, v) VALUES (?, ?, ?)")
      .run(ns, k, JSON.stringify(v));
  }

  private del(ns: string, k: string): void {
    this.db.prepare("DELETE FROM kv WHERE ns = ? AND k = ?").run(ns, k);
  }

  private list<T>(ns: string): Map<string, T> {
    const rows = this.db.prepare("SELECT k, v FROM kv WHERE ns = ?").all(ns) as {
      k: string;
      v: string;
    }[];
    return new Map(rows.map((r) => [r.k, JSON.parse(r.v) as T]));
  }

  async apply(events: SitrepEvent[]): Promise<void> {
    for (const ev of events) {
      const task = reduceTask(this.get("task", ev.source_id) as TaskState | undefined, ev);
      if (task) this.put("task", ev.source_id, task);
      const metric = reduceMetric(ev);
      if (metric) {
        const pref = this.get("metric-pref", metric.key) as MetricPreference | undefined;
        const effective = pref ? mergeMetric(metric, { [metric.key]: pref }) : metric;
        const prev = this.get("metric", effective.key) as MetricState | undefined;
        // Threshold edge: notify exactly once per excursion (see store.ts).
        const viol = metricViolation(effective);
        if (viol && (!prev || !metricViolation(prev))) {
          this.appendLog({ id: newEventId(), text: violationText(effective, viol), level: "warn", ts: ev.ts, source: ev.source_id });
        }
        this.put("metric", effective.key, pushHistory(prev, effective));
        const series = appendSeries(this.get("series", effective.key) as MetricSeries | undefined, ev.ts, effective.value);
        if (series) this.put("series", effective.key, series);
      }
      switch (ev.kind) {
        case "message.send":
          if (ev.text) this.appendLog({ id: newEventId(), text: ev.text, level: ev.level ?? "info", ts: ev.ts, source: ev.source_id });
          break;
        case "task.log":
          if (ev.text) {
            this.put("tasklog", ev.source_id,
              appendTaskLog(this.get("tasklog", ev.source_id) as string[] | undefined, ev.text));
          }
          break;
      }
    }
  }

  private appendLog(entry: EventLogEntry): void {
    const log = ((this.get("sys", "evlog") as EventLogEntry[] | undefined) ?? []);
    log.unshift(entry);
    if (log.length > EVENT_LOG_CAP) log.length = EVENT_LOG_CAP;
    this.put("sys", "evlog", log);
  }

  async tasks(): Promise<TaskState[]> {
    return [...this.list<TaskState>("task").values()];
  }

  async metrics(): Promise<MetricState[]> {
    return [...this.list<MetricState>("metric").values()];
  }

  async eventLog(): Promise<EventLogEntry[]> {
    return (this.get("sys", "evlog") as EventLogEntry[] | undefined) ?? [];
  }

  async metricPrefs(): Promise<Record<string, MetricPreference>> {
    return Object.fromEntries(this.list<MetricPreference>("metric-pref"));
  }

  async setMetricPref(metricId: string, pref: MetricPreference | null): Promise<void> {
    if (pref === null) this.del("metric-pref", metricId);
    else this.put("metric-pref", metricId, pref);
  }

  async automations(): Promise<AutomationDef[]> {
    return [...this.list<AutomationDef>("automation").values()];
  }

  async putAutomation(automation: AutomationDef): Promise<void> {
    this.put("automation", automation.id, automation);
  }

  async patchAutomation(
    id: string,
    patch: Partial<Pick<AutomationDef, "name" | "every_s" | "enabled" | "last_run" | "run_requested_at">>,
  ): Promise<AutomationDef | undefined> {
    const automation = this.get("automation", id) as AutomationDef | undefined;
    if (!automation) return undefined;
    const next = { ...automation, ...patch, updated_at: new Date().toISOString() };
    this.put("automation", id, next);
    return next;
  }

  async deleteAutomation(id: string): Promise<void> {
    this.del("automation", id);
  }

  async deleteTask(sourceId: string): Promise<void> {
    this.del("task", sourceId);
    this.del("tasklog", sourceId);
  }

  async taskLog(sourceId: string): Promise<string[]> {
    return (this.get("tasklog", sourceId) as string[] | undefined) ?? [];
  }

  async deleteMetric(key: string): Promise<void> {
    this.del("metric", key);
    this.del("series", key);
  }

  async metricSeries(key: string, range: SeriesRange): Promise<SeriesPoint[]> {
    return selectSeries(this.get("series", key) as MetricSeries | undefined, range);
  }

  async stampPresence(kind: "ingest" | "agent", ts: string): Promise<void> {
    this.put("presence", kind, ts);
  }

  async presence(): Promise<PresenceInfo> {
    return {
      ingest_last_seen: this.get("presence", "ingest") as string | undefined,
      agent_last_seen: this.get("presence", "agent") as string | undefined,
    };
  }

  async deleteEvents(ids: string[] | "all"): Promise<void> {
    if (ids === "all") {
      this.del("sys", "evlog");
      return;
    }
    const log = (this.get("sys", "evlog") as EventLogEntry[] | undefined) ?? [];
    this.put("sys", "evlog", log.filter((e) => !e.id || !ids.includes(e.id)));
  }
}
