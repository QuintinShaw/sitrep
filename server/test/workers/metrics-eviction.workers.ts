// Promoted from an adversarial review pass's `adversarial-metrics-eviction
// .workers.ts` — verifies the metrics_current debounce buffer (P0-7)
// interacts correctly with a DO eviction landing INSIDE the 10s
// (METRICS_CURRENT_DOWNSAMPLE_MS) window:
//   (b) a metric_id's FIRST-EVER sample is persisted SYNCHRONOUSLY
//       (bypassing pendingMetricsFlush entirely, exactly like an alert edge
//       transition) so existence is never deferred behind the debounce —
//       eviction right after can lose at most a SUBSEQUENT routine update's
//       tail value, never the fact that the metric was reported at all.
//   (c) an alert armed->fired edge on one metric survives eviction even
//       while an unrelated metric's routine update is still buffered.
//   (d) the debounce flush is driven by the shared alarm regardless of
//       APNs delivery state / outbox activity.
import { env, SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, evictSpaceHub, hasScheduledAlarm, spaceHubStub } from "./helpers.ts";

const ORIGIN = "https://example.com";
let envelopeIdCounter = 90000;

async function postMetric(token: string, deviceId: string, metricId: string, value: string, ts: number, alertAbove?: string): Promise<Response> {
  envelopeIdCounter += 1;
  return SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({
      events: [
        {
          type: "metric.frame",
          id: `mid${envelopeIdCounter}`,
          ts,
          body: { device_id: deviceId, metrics: [{ metric_id: metricId, value, ts, ...(alertAbove ? { alert_above: alertAbove } : {}) }] },
        },
      ],
    }),
  });
}

async function readRow(stub: ReturnType<typeof spaceHubStub>, metricId: string) {
  const { runInDurableObject } = await import("cloudflare:test");
  return runInDurableObject(
    stub,
    (_i, state) => (state.storage.sql.exec("SELECT value, updated_at FROM metrics_current WHERE metric_id = ?", metricId).toArray()[0] as { value: string; updated_at: number } | undefined) ?? null,
  );
}

describe("metrics_current debounce buffer vs DO eviction", () => {
  it("(b) a metric_id's first-ever sample survives eviction (no false 404); a SUBSEQUENT routine update buffered in the same window may be lost, i.e. the persisted value may still be the older one", async () => {
    const { source, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    const now = Date.now();

    // First-ever sample of this metric_id: must persist SYNCHRONOUSLY —
    // existence is never deferred behind the debounce.
    const first = await postMetric(source.token, source.device_id, "evict.cpu", "42", now);
    expect(first.status).toBe(200);
    expect((await readRow(stub, "evict.cpu"))?.value).toBe("42");

    // A second, routine (non-edge) update for the SAME already-persisted
    // metric_id: this one buffers under the 10s debounce, as before.
    const second = await postMetric(source.token, source.device_id, "evict.cpu", "43", now + 50);
    expect(second.status).toBe(200);
    // Still the first-persisted value pre-flush — the second update hasn't
    // hit metrics_current yet, only the (about-to-be-evicted) hot cache.
    expect((await readRow(stub, "evict.cpu"))?.value).toBe("42");

    // Evict well within the 10s debounce window (no time has passed at
    // all) — simulates the DO being kicked from memory before its own
    // durably-scheduled alarm gets a chance to run flushMetricsCurrent().
    await evictSpaceHub(stub);

    // Re-fetch a stub (fresh in-memory instance) and inspect persisted
    // state directly — this is what a real "space rebuilt after eviction,
    // then read via snapshot/metrics endpoint" scenario would see.
    const freshStub = spaceHubStub(spaceId);
    const rowAfterEviction = await readRow(freshStub, "evict.cpu");

    // The metric_id itself is NOT lost — the row exists with the
    // first-persisted value. Only the second sample's value update (a
    // documented, accepted bound: at most the debounce-window tail) is
    // gone; the metric's genuinely-reported existence is not.
    console.log("(b) row after eviction inside debounce window:", JSON.stringify(rowAfterEviction));
    expect(rowAfterEviction).not.toBeNull();
    expect(rowAfterEviction?.value).toBe("42");

    // GET /v1/metrics/:id must NOT 404 for a metric that was genuinely
    // reported — the "404 is authoritative iff never reported" invariant.
    const liveRes = await SELF.fetch(`${ORIGIN}/v1/metrics/evict.cpu`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(liveRes.status).toBe(200);
    const liveBody = (await liveRes.json()) as { value: string };
    expect(liveBody.value).toBe("42");

    // Even waiting out the alarm doesn't recover the lost tail update,
    // because the alarm fires against the fresh (empty-buffer) instance.
    const { runDurableObjectAlarm } = await import("cloudflare:test");
    await runDurableObjectAlarm(freshStub);
    const rowAfterAlarm = await readRow(freshStub, "evict.cpu");
    console.log("(b) row after post-eviction alarm fire:", JSON.stringify(rowAfterAlarm));
    expect(rowAfterAlarm?.value).toBe("42");
  });

  it("(c) an alert armed->fired edge that lands DURING an in-flight routine debounce window for a DIFFERENT metric survives eviction — edge persistence is correctly decoupled from the (bounded-lossy) routine buffer", async () => {
    const { source, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    const now = Date.now();

    // Routine metric A: first sample persists synchronously (existence),
    // then a SECOND routine update buffers under the debounce.
    await postMetric(source.token, source.device_id, "routine.metric", "1", now);
    expect((await readRow(stub, "routine.metric"))?.value).toBe("1");
    await postMetric(source.token, source.device_id, "routine.metric", "2", now + 50);
    expect((await readRow(stub, "routine.metric"))?.value).toBe("1"); // buffered update not yet flushed

    // Alert edge (armed->fired) on metric B, arriving inside the SAME
    // window while A's second update is still buffered.
    const alertRes = await postMetric(source.token, source.device_id, "alert.metric", "95", now + 100, "90");
    expect(alertRes.status).toBe(200);
    expect((await readRow(stub, "alert.metric"))?.value).toBe("95"); // persisted immediately, per code path

    // Evict now, before the 10s window elapses.
    await evictSpaceHub(stub);
    const freshStub = spaceHubStub(spaceId);

    const routineAfter = await readRow(freshStub, "routine.metric");
    const alertAfter = await readRow(freshStub, "alert.metric");
    console.log("(c) routine.metric after eviction:", JSON.stringify(routineAfter));
    console.log("(c) alert.metric after eviction:", JSON.stringify(alertAfter));

    // routine.metric's existence and its first value both survive; only the
    // buffered SECOND update ("2") is lost, bounded to the debounce tail.
    expect(routineAfter?.value).toBe("1");
    expect(alertAfter?.value).toBe("95"); // survived — edge persistence genuinely bypasses the buffer

    // GET /v1/metrics/alert.metric and /v1/metrics/routine.metric both
    // correctly work post-eviction — neither false-404s.
    const alertLive = await SELF.fetch(`${ORIGIN}/v1/metrics/alert.metric`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(alertLive.status).toBe(200);
    const routineLive = await SELF.fetch(`${ORIGIN}/v1/metrics/routine.metric`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(routineLive.status).toBe(200);
  });

  it("(d) metrics debounce-flush does NOT depend on APNS_DELIVERY_ENABLED or any push_outbox activity — the shared alarm still flushes pending metrics when APNs delivery is disabled and there is nothing in the outbox", async () => {
    env.APNS_DELIVERY_ENABLED = false;
    const { source, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);
    const now = Date.now();

    // First sample persists synchronously regardless (existence path).
    const res = await postMetric(source.token, source.device_id, "quiet.metric", "7", now);
    expect(res.status).toBe(200);
    expect((await readRow(stub, "quiet.metric"))?.value).toBe("7");

    // A second, routine update is what actually exercises the debounce
    // buffer + shared alarm flush path this test targets.
    const res2 = await postMetric(source.token, source.device_id, "quiet.metric", "8", now + 50);
    expect(res2.status).toBe(200);
    expect((await readRow(stub, "quiet.metric"))?.value).toBe("7"); // still buffered, not yet flushed

    const scheduled = await hasScheduledAlarm(stub);
    console.log("(d) alarm scheduled with APNS disabled + empty outbox:", scheduled);
    expect(scheduled).toBe(true); // ensureAlarm() is called regardless of APNs state

    const { runDurableObjectAlarm } = await import("cloudflare:test");
    const ran = await runDurableObjectAlarm(stub);
    expect(ran).toBe(true);
    const row = await readRow(stub, "quiet.metric");
    console.log("(d) row after alarm fire with APNS disabled:", JSON.stringify(row));
    expect(row?.value).toBe("8"); // flush happened despite APNS_DELIVERY_ENABLED=false

    env.APNS_DELIVERY_ENABLED = true;
  });
});
