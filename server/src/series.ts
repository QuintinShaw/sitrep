// Pure tiered-series + task-log-tail helpers for StateStore's persistent,
// non-folded history tables (v1-architecture.md §1.2.1 metric_series,
// §1.2.2 task_logs). No I/O — SpaceHub loads/writes the SQLite rows around
// these, so they are cheap to unit test directly. Semantics are carried over
// verbatim from the pre-v1 product implementation (the old store.ts
// appendSeries/selectSeries/appendTaskLog), with one representational
// change the frozen table schema requires: series point timestamps are
// unix-ms numbers (`t`), not ISO strings.

import { SERIES_DAY_CAP, SERIES_HOUR_CAP, SERIES_RAW_CAP, TASK_LOG_WINDOW, type SeriesRange } from "./v1/contract/types.ts";

/** One stored series point: `t` bucket timestamp (unix ms), `v` bucket
 * value. Oldest first within each tier's array. */
export interface SeriesPoint {
  t: number;
  v: number;
}

/** The three retention tiers for one metric, as held in the three
 * `metric_series` rows (one row per tier) for that metric_id. */
export interface MetricSeries {
  raw: SeriesPoint[];
  hour: SeriesPoint[];
  day: SeriesPoint[];
}

export function emptySeries(): MetricSeries {
  return { raw: [], hour: [], day: [] };
}

const bucketStart = (ts: number, ms: number): number => Math.floor(ts / ms) * ms;

/** Folds one accepted metric sample into all three tiers (pure). `raw` is
 * per-sample (capped 720); `hour`/`day` keep the LAST value seen in each
 * bucket window (capped 768 / 400). A non-finite value or timestamp is a
 * no-op (returns the input unchanged). Mirrors the pre-v1 appendSeries. */
export function appendSeries(prev: MetricSeries | undefined, ts: number, value: number): MetricSeries {
  const base = prev ?? emptySeries();
  if (!Number.isFinite(value) || !Number.isFinite(ts)) return base;
  const s: MetricSeries = { raw: [...base.raw], hour: [...base.hour], day: [...base.day] };
  s.raw.push({ t: ts, v: value });
  if (s.raw.length > SERIES_RAW_CAP) s.raw.splice(0, s.raw.length - SERIES_RAW_CAP);
  for (const [tier, ms, cap] of [
    ["hour", 3_600_000, SERIES_HOUR_CAP],
    ["day", 86_400_000, SERIES_DAY_CAP],
  ] as const) {
    const arr = s[tier];
    const t = bucketStart(ts, ms);
    if (arr.length > 0 && arr[arr.length - 1].t === t) arr[arr.length - 1] = { t, v: value };
    else {
      arr.push({ t, v: value });
      if (arr.length > cap) arr.splice(0, arr.length - cap);
    }
  }
  return s;
}

/** Picks the right tier + window for a range (pure). Short ranges read the
 * per-sample `raw` tier; `1w`/`1m` read hourly; `1y` reads daily. `1d` falls
 * back to hourly when `raw` doesn't yet span the full day (fresh installs).
 * Mirrors the pre-v1 selectSeries. */
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
  let points = tier.filter((p) => p.t >= cutoff);
  if (range === "1d" && s.raw.length > 0 && s.raw[0].t > cutoff) {
    const hourly = s.hour.filter((p) => p.t >= cutoff);
    if (hourly.length > points.length) points = hourly;
  }
  return points;
}

/** Appends lines to a per-task ring buffer, dropping the oldest past the
 * TASK_LOG_WINDOW cap (pure). Mirrors the pre-v1 appendTaskLog, minus the
 * old \n-splitting: the v1 POST /v1/tasks/:id/log route carries an already
 * line-split `{lines: [...]}` array. Empty/whitespace-only lines are kept
 * as-is (the caller decides trimming); each line is truncated to 300 chars,
 * matching the old tail's per-line cap. */
export function appendTaskLog(prev: string[] | undefined, lines: string[]): string[] {
  const trimmed = lines.map((l) => l.slice(0, 300));
  const log = [...(prev ?? []), ...trimmed];
  if (log.length > TASK_LOG_WINDOW) log.splice(0, log.length - TASK_LOG_WINDOW);
  return log;
}
