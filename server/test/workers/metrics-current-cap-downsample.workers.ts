// metrics_current: P0-7 cap + downsample (v1-architecture.md §1.2.0).
//   - routine (non-edge-transition) samples are coalesced — last-value-wins
//     — and flushed by the shared alarm at most once per
//     METRICS_CURRENT_DOWNSAMPLE_MS, instead of one SQL write per sample.
//   - an alert edge transition (armed<->fired) ALWAYS persists immediately,
//     bypassing the debounce entirely.
//   - the table is capped at the shared METRIC_CACHE_MAX_METRICS (256)
//     distinct metric_ids per space, LRU eviction (least-recently-updated)
//     on overflow.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { armAlarmNow, bootstrapSpace, fireAlarm, spaceHubStub } from "./helpers.ts";

const ORIGIN = "https://example.com";

let envelopeIdCounter = 0;

async function postMetric(token: string, deviceId: string, metricId: string, value: string, ts: number, alertAbove?: string): Promise<Response> {
  envelopeIdCounter += 1;
  return SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({
      events: [
        {
          // Envelope id must match the proto id grammar — NOT the metric_id
          // (which may contain '.'), a plain counter-derived id.
          type: "metric.frame",
          id: `mid${envelopeIdCounter}`,
          ts,
          body: { device_id: deviceId, metrics: [{ metric_id: metricId, value, ts, ...(alertAbove ? { alert_above: alertAbove } : {}) }] },
        },
      ],
    }),
  });
}

async function readMetricsCurrentRow(stub: ReturnType<typeof spaceHubStub>, metricId: string): Promise<{ value: string; updated_at: number } | null> {
  const { runInDurableObject } = await import("cloudflare:test");
  return runInDurableObject(
    stub,
    (_i, state) => (state.storage.sql.exec("SELECT value, updated_at FROM metrics_current WHERE metric_id = ?", metricId).toArray()[0] as { value: string; updated_at: number } | undefined) ?? null,
  );
}

describe("metrics_current: P0-7 downsample", () => {
  it("rapid routine samples within the debounce window collapse to one write; a threshold crossing persists immediately mid-window", async () => {
    const { source, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    const base = Date.now();

    // r1 is this metric_id's FIRST-EVER sample: it persists SYNCHRONOUSLY
    // (existence is never deferred behind the debounce). r2/r3 are routine
    // updates to an already-persisted metric_id, so THEY coalesce in the
    // in-memory debounce buffer instead of being written per sample.
    const r1 = await postMetric(source.token, source.device_id, "cpu", "10", base + 0);
    const r2 = await postMetric(source.token, source.device_id, "cpu", "20", base + 1000);
    const r3 = await postMetric(source.token, source.device_id, "cpu", "30", base + 2000);
    expect([r1.status, r2.status, r3.status]).toEqual([200, 200, 200]);
    expect((await readMetricsCurrentRow(stub, "cpu"))?.value).toBe("10"); // r1's synchronous first-persist; r2/r3 still buffered

    // But the hot-cache-backed GET /v1/metrics/:id is already fresh — the
    // downsample is purely a write-timing detail of the persisted row, not
    // of what reads see. (GET /v1/metrics/:id is viewer/owner only — source
    // has no read access, v1-architecture.md §3.)
    const liveRes = await SELF.fetch(`${ORIGIN}/v1/metrics/cpu`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(((await liveRes.json()) as { value: string }).value).toBe("30");

    // A threshold-crossing sample (armed -> fired) persists IMMEDIATELY,
    // bypassing the debounce entirely, regardless of the timer.
    await postMetric(source.token, source.device_id, "cpu", "95", base + 3000, "90");
    const rowAfterCross = await readMetricsCurrentRow(stub, "cpu");
    expect(rowAfterCross?.value).toBe("95");

    // Force the debounce flush (if anything routine is still buffered for
    // this metric) — the persisted row must still reflect the latest
    // overall value.
    await armAlarmNow(stub);
    expect(await fireAlarm(stub)).toBe(true);
    const finalRow = await readMetricsCurrentRow(stub, "cpu");
    expect(finalRow?.value).toBe("95");
  });

  it("a metric_id with a single BUFFERED routine update (i.e. its second-ever sample) and no follow-up is still flushed within the window (never left unpersisted indefinitely)", async () => {
    const { source, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    const now = Date.now();
    // First-ever sample: synchronous persist (existence path), not what
    // this test targets.
    await postMetric(source.token, source.device_id, "solo.metric", "1", now);
    expect((await readMetricsCurrentRow(stub, "solo.metric"))?.value).toBe("1");
    // Second sample: a routine update to an already-persisted metric_id —
    // THIS is what buffers under the debounce.
    await postMetric(source.token, source.device_id, "solo.metric", "2", now + 50);
    expect((await readMetricsCurrentRow(stub, "solo.metric"))?.value).toBe("1"); // buffered, not yet flushed

    await armAlarmNow(stub);
    expect(await fireAlarm(stub)).toBe(true);
    const row = await readMetricsCurrentRow(stub, "solo.metric");
    expect(row?.value).toBe("2");
  });
});

describe("metrics_current: P0-7 cap (256 distinct metric_ids per space, LRU eviction)", () => {
  it("an insert past the cap evicts the least-recently-updated row, keeping the table at exactly 256", async () => {
    const { source, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    const { runInDurableObject } = await import("cloudflare:test");
    const now = Date.now();

    // Seed exactly 256 rows directly, oldest (seed-0) to newest (seed-255).
    await runInDurableObject(stub, (_i, state) => {
      for (let i = 0; i < 256; i++) {
        state.storage.sql.exec(
          `INSERT INTO metrics_current (metric_id, value, fields, alert_state, updated_at) VALUES (?, '0', '{"metric_id":"seed","value":"0","ts":0}', '{}', ?)`,
          `seed-${i}`,
          now - (256 - i) * 1000,
        );
      }
    });

    // One more, brand-new metric_id — threshold-crossing so it persists
    // immediately (bypassing the debounce), exercising the cap-eviction path
    // deterministically without waiting for a flush.
    const res = await postMetric(source.token, source.device_id, "new.metric", "99", now + 10_000, "90");
    expect(res.status).toBe(200);

    const rows = await runInDurableObject(stub, (_i, state) => state.storage.sql.exec("SELECT metric_id FROM metrics_current").toArray() as unknown as Array<{ metric_id: string }>);
    expect(rows.length).toBe(256); // capped, not 257
    expect(rows.some((r) => r.metric_id === "seed-0")).toBe(false); // least-recently-updated evicted
    expect(rows.some((r) => r.metric_id === "seed-255")).toBe(true); // newest seed kept
    expect(rows.some((r) => r.metric_id === "new.metric")).toBe(true);

    // GET /v1/metrics/:id on the evicted metric_id is a genuine 404 (it was
    // reported, then LRU-evicted to make room — the documented cap tradeoff,
    // not a bug).
    const evicted = await SELF.fetch(`${ORIGIN}/v1/metrics/seed-0`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(evicted.status).toBe(404);
  });
});
