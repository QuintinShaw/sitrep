// Promoted from an adversarial review pass's `adversarial-metrics-cap-live
// .workers.ts` — rotates DISTINCT metric_ids through real HTTP /v1/events
// calls (not seeded SQL rows) and confirms the DO's persistent
// metrics_current table stays bounded with correct LRU eviction order. A
// live-HTTP complement to metrics-current-cap-downsample.workers.ts's
// seeded-row cap test.
//
// Scale note: this issues ~330 real SELF.fetch round-trips (invite+join per
// device, plus one metric.frame per sample), which reliably takes several
// seconds of real wall-clock time under vitest-pool-workers — well past
// vitest's default 5s per-test timeout. Trimmed from an original 300 to 270
// distinct metric_ids (still comfortably crosses the 256 cap) and given an
// explicit generous timeout below so it passes reliably rather than racing
// the default.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, spaceHubStub } from "./helpers.ts";

const ORIGIN = "https://example.com";
let envelopeIdCounter = 500000;

async function postAlertMetric(token: string, deviceId: string, metricId: string, ts: number): Promise<Response> {
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
          // alert_above always set so every sample is a threshold crossing
          // (armed->fired), persisting IMMEDIATELY — lets us rotate many
          // distinct metric_ids through real storage without waiting on the
          // 10s debounce alarm once per metric_id.
          body: { device_id: deviceId, metrics: [{ metric_id: metricId, value: "999", ts, alert_above: "1" }] },
        },
      ],
    }),
  });
}

describe("metrics_current cap under real rotation via HTTP", () => {
  it(
    "270 distinct metric_ids via real /v1/events calls -> storage stays at exactly 256, oldest 14 evicted, newest 256 kept",
    async () => {
      const { spaceId, inviteAndJoin } = await bootstrapSpace();
      const stub = spaceHubStub(spaceId);
      const base = Date.now();

      // The per-DEVICE metric.frame rate limit is 10/sec (real, live —
      // METRIC_FRAME_RATE_PER_SEC_PER_DEVICE, types.ts) but the
      // metrics_current CAP is per-SPACE. To rotate 270 distinct metric_ids
      // through real HTTP calls without tripping the (unrelated,
      // correctly-enforced) per-device rate limiter, spread the load across
      // many real joined source devices in the SAME space — still a fully
      // live DO exercise of the cap/eviction path, just not conflating it
      // with the separate rate-limit feature.
      const TOTAL = 270;
      const PER_DEVICE = 9; // stay under the 10/s cap per device
      let i = 0;
      while (i < TOTAL) {
        const dev = await inviteAndJoin("source");
        for (let k = 0; k < PER_DEVICE && i < TOTAL; k++, i++) {
          const res = await postAlertMetric(dev.token, dev.device_id, `rot-${i}`, base + i);
          if (res.status !== 200) {
            const body = await res.text();
            throw new Error(`metric ${i} rejected: ${res.status} ${body}`);
          }
        }
      }

      const { runInDurableObject } = await import("cloudflare:test");
      const rows = await runInDurableObject(
        stub,
        (_i, state) => state.storage.sql.exec("SELECT metric_id FROM metrics_current ORDER BY updated_at ASC").toArray() as unknown as Array<{ metric_id: string }>,
      );
      console.log("live-rotation stored count:", rows.length);
      console.log("oldest 3 kept:", rows.slice(0, 3).map((r) => r.metric_id));
      console.log("newest 3 kept:", rows.slice(-3).map((r) => r.metric_id));

      expect(rows.length).toBe(256);
      // Oldest surviving row should be rot-14 (270-256=14 evicted: rot-0..rot-13)
      expect(rows[0]?.metric_id).toBe("rot-14");
      expect(rows[rows.length - 1]?.metric_id).toBe("rot-269");
      expect(rows.some((r) => r.metric_id === "rot-0")).toBe(false);
      expect(rows.some((r) => r.metric_id === "rot-13")).toBe(false);
    },
    30_000,
  );
});
