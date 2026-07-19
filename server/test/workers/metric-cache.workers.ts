// Required coverage: metricsCache (SpaceHub#metricsCache) must not grow
// without bound. A compromised or misbehaving source rotating metric_ids
// past any real cardinality would otherwise grow DO memory until it
// crashes — see METRIC_CACHE_MAX_METRICS in src/realtime/protocol.ts and
// SpaceHub#touchMetricCache.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { METRIC_CACHE_MAX_METRICS } from "../../src/realtime/protocol.ts";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";
import { bootstrapSpace, connect, helloOffer, helloSource, nextId, subscribe } from "./helpers";

describe("metricsCache cardinality cap", () => {
  it("evicts least-recently-updated metric_ids beyond the cap, keeping the most recent", async () => {
    const { spaceId, source, viewer } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);

    const viewerClient = await connect(viewer.token);
    await helloOffer(viewerClient, viewer.device_id, "viewer");
    await subscribe(viewerClient, ["metric"]);

    // Rotate far more distinct metric_ids than the cap, staying within the
    // 64-samples-per-frame guard (guards.ts) and the 10 frames/s limiter
    // (METRIC_FRAME_RATE_PER_SEC) by packing many ids per frame across a
    // handful of frames rather of one-id-per-frame.
    const perFrame = 64;
    const frameCount = 6; // 384 distinct ids, comfortably over the 256 cap
    const totalIds = perFrame * frameCount;
    for (let f = 0; f < frameCount; f++) {
      const metrics = Array.from({ length: perFrame }, (_, i) => {
        const n = f * perFrame + i;
        return { metric_id: `m${n}`, value: String(n), ts: Date.now() + n };
      });
      sourceClient.send({
        type: "metric.frame",
        id: nextId(),
        ts: Date.now(),
        body: { device_id: source.device_id, metrics },
      });
      // Drain this frame's broadcast to the subscribed viewer before
      // sending the next one, so the viewer's recv queue doesn't leak
      // across iterations (not load-bearing for the assertion below, just
      // hygiene for the WsClient helper).
      await viewerClient.recv();
    }

    const stub = env.SPACE_HUB.getByName(spaceId);
    const { size, ids } = await runInDurableObject(stub, async (instance: SpaceHub) => {
      const cache = (instance as any).metricsCache as Map<string, unknown>;
      return { size: cache.size, ids: [...cache.keys()] };
    });

    expect(size).toBe(METRIC_CACHE_MAX_METRICS);
    expect(size).toBeLessThan(totalIds);

    // The most-recently-sent ids (the last frame's) must all have
    // survived eviction...
    for (let n = totalIds - perFrame; n < totalIds; n++) {
      expect(ids).toContain(`m${n}`);
    }
    // ...while the earliest-sent ids (evicted as least-recently-updated)
    // must be gone.
    for (let n = 0; n < perFrame; n++) {
      expect(ids).not.toContain(`m${n}`);
    }

    sourceClient.close();
    viewerClient.close();
  });
});
