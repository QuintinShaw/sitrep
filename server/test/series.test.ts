import assert from "node:assert/strict";
import test from "node:test";
import { appendSeries, appendTaskLog, selectSeries } from "../src/series.ts";
import { SERIES_RAW_CAP, TASK_LOG_WINDOW } from "../src/v1/contract/types.ts";

const HOUR = 3_600_000;
const DAY = 86_400_000;

test("appendSeries: raw is per-sample and capped at SERIES_RAW_CAP (oldest dropped)", () => {
  let s = undefined as ReturnType<typeof appendSeries> | undefined;
  for (let i = 0; i < SERIES_RAW_CAP + 5; i++) s = appendSeries(s, i * 1000, i);
  assert.equal(s!.raw.length, SERIES_RAW_CAP);
  assert.equal(s!.raw[0].v, 5); // first 5 dropped
  assert.equal(s!.raw[s!.raw.length - 1].v, SERIES_RAW_CAP + 4);
});

test("appendSeries: hour/day tiers keep the LAST value per bucket", () => {
  const base = 1_000_000 * HOUR; // aligned to an hour boundary
  let s = appendSeries(undefined, base + 60_000, 1); // minute 1 of hour H
  s = appendSeries(s, base + 120_000, 2); // minute 2 of the SAME hour H
  s = appendSeries(s, base + HOUR + 60_000, 9); // next hour H+1
  assert.equal(s.hour.length, 2);
  assert.equal(s.hour[0].v, 2); // last value in hour H, not 1
  assert.equal(s.hour[1].v, 9);
  assert.equal(s.day.length, 1); // both hours fall in the same day
  assert.equal(s.day[0].v, 9);
});

test("appendSeries: a non-finite value is a no-op", () => {
  const s = appendSeries(undefined, 1000, 1);
  const same = appendSeries(s, 2000, Number.NaN);
  assert.deepEqual(same, s);
});

test("selectSeries: short ranges read raw, long ranges read hour/day, filtered by window", () => {
  const now = 100 * DAY;
  let s = undefined as ReturnType<typeof appendSeries> | undefined;
  s = appendSeries(s, now - 40 * DAY, 1);
  s = appendSeries(s, now - 2 * HOUR, 2);
  s = appendSeries(s, now - 90 * 60_000, 3); // 90 min ago — outside a 1h window
  s = appendSeries(s, now - 5 * 60_000, 4);

  const oneH = selectSeries(s, "1h", now);
  assert.deepEqual(oneH.map((p) => p.v), [4]); // only the 5-min-ago sample is within 1h

  const oneD = selectSeries(s, "1d", now);
  assert.deepEqual(oneD.map((p) => p.v), [2, 3, 4]); // the 40-day-old one is out of the 1d window

  const oneY = selectSeries(s, "1y", now).map((p) => p.v);
  assert.ok(oneY.includes(1)); // the 40-day-old sample IS visible in the 1y (daily) tier
});

test("appendTaskLog: appends and caps at TASK_LOG_WINDOW (oldest dropped), truncates long lines", () => {
  let log: string[] = [];
  for (let i = 0; i < TASK_LOG_WINDOW + 3; i++) log = appendTaskLog(log, [`line ${i}`]);
  assert.equal(log.length, TASK_LOG_WINDOW);
  assert.equal(log[0], "line 3");
  assert.equal(log[log.length - 1], `line ${TASK_LOG_WINDOW + 2}`);

  const withLong = appendTaskLog([], ["x".repeat(500)]);
  assert.equal(withLong[0].length, 300);
});

test("appendTaskLog: a multi-line append preserves order", () => {
  const log = appendTaskLog(["a"], ["b", "c"]);
  assert.deepEqual(log, ["a", "b", "c"]);
});
