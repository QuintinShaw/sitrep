// metric_series (v1-architecture.md §1.2.1) and task_logs (§1.2.2): both
// are PERSISTENT, non-folded StateStore tables that must survive DO
// eviction — the whole point of the R0→frozen change from the lossy
// in-memory ring buffers to durable SQLite.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { armAlarmNow, bootstrapSpace, evictSpaceHub, fireAlarm, spaceHubStub } from "./helpers.ts";

const ORIGIN = "https://example.com";

async function postEvents(token: string, events: unknown[]) {
  const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ events }),
  });
  return { status: res.status, body: (await res.json()) as any };
}

describe("metric_series: persistent, non-folded, survives eviction", () => {
  it("metric.frame appends to a durable series that GET .../series reads back after a DO eviction", async () => {
    const { source, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);

    const base = Date.now();
    // A handful of samples for one metric over a few minutes.
    for (let i = 0; i < 4; i++) {
      await postEvents(source.token, [
        { type: "metric.frame", id: `m${i}`, ts: base + i * 1000, body: { device_id: source.device_id, metrics: [{ metric_id: "cpu.load1", value: String(1 + i * 0.1), ts: base + i * 60_000 }] } },
      ]);
    }

    // metric.frame is non-folded: space_revision must NOT have advanced.
    const snap = (await (await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as { space_revision: number };
    expect(snap.space_revision).toBe(0);

    // P0-7: none of the samples above crossed an alert threshold. The FIRST
    // (i=0) persists synchronously — a metric_id's first-ever sample always
    // bypasses the debounce so existence is never deferred — but samples
    // i=1..3 are routine updates to an already-persisted metric_id, so THEY
    // coalesce in the in-memory debounce buffer (flushed by the shared alarm
    // within METRICS_CURRENT_DOWNSAMPLE_MS) rather than being written
    // synchronously. Force that flush before evicting — the in-memory
    // debounce buffer itself does not survive an eviction (only a first-ever
    // sample and alert edge-state transitions bypass the debounce and are
    // always eviction-safe immediately); metric_series (below) is unaffected
    // by any of this, since it is appended on every accepted sample
    // regardless.
    await armAlarmNow(stub);
    expect(await fireAlarm(stub)).toBe(true);

    // Evict the DO (wipes in-memory metricsCache) — the persistent series
    // must remain.
    await evictSpaceHub(stub);

    const seriesRes = await SELF.fetch(`${ORIGIN}/v1/metrics/cpu.load1/series?range=1h`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(seriesRes.status).toBe(200);
    const points = (await seriesRes.json()) as Array<{ ts: number; value: number }>;
    expect(points.length).toBe(4);
    expect(points.map((p) => p.value)).toEqual([1, 1.1, 1.2, 1.3]);
    expect(points[0].ts).toBeLessThan(points[3].ts); // oldest first

    // GET /v1/metrics/:id now reads the persistent metrics_current (P0-2), so
    // it ALSO survives eviction with the last accepted value (was 404 in the
    // in-memory-cache era). A rebuilt DO never loses an accepted value.
    const cacheRes = await SELF.fetch(`${ORIGIN}/v1/metrics/cpu.load1`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(cacheRes.status).toBe(200);
    expect(((await cacheRes.json()) as { value: string }).value).toBe("1.3");

    // 404 is now authoritative: a never-reported metric.
    const missing = await SELF.fetch(`${ORIGIN}/v1/metrics/never.reported`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(missing.status).toBe(404);
  });

  it("range validation matches the frozen SERIES_RANGES (1h/6h/1d/1w/1m/1y)", async () => {
    const { ownerToken } = await bootstrapSpace();
    const bad = await SELF.fetch(`${ORIGIN}/v1/metrics/cpu.load1/series?range=99y`, { headers: { authorization: `Bearer ${ownerToken}` } });
    expect(bad.status).toBe(400);
    expect(await bad.json()).toEqual({ error: "invalid range" });

    for (const range of ["1h", "6h", "1d", "1w", "1m", "1y"]) {
      const ok = await SELF.fetch(`${ORIGIN}/v1/metrics/cpu.load1/series?range=${range}`, { headers: { authorization: `Bearer ${ownerToken}` } });
      expect(ok.status).toBe(200);
    }
  });
});

describe("task_logs: source-uplinked ring buffer, non-folded, survives eviction", () => {
  it("POST /v1/tasks/:id/log (source-only) appends; GET reads the ring buffer back after eviction", async () => {
    const { source, viewer, ownerToken, spaceId } = await bootstrapSpace();
    const stub = spaceHubStub(spaceId);

    // Viewer cannot POST log lines (source-only uplink).
    const viewerPost = await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/log`, {
      method: "POST",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ lines: ["nope"] }),
    });
    expect(viewerPost.status).toBe(403);

    // Empty lines -> 400.
    const empty = await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/log`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({ lines: [] }),
    });
    expect(empty.status).toBe(400);

    const post1 = await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/log`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({ lines: ["line a", "line b"] }),
    });
    expect(post1.status).toBe(200);
    expect(await post1.json()).toEqual({ ok: true });

    // A source log post is non-folded: space_revision stays 0.
    const snap = (await (await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as { space_revision: number };
    expect(snap.space_revision).toBe(0);

    await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/log`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({ lines: ["line c"] }),
    });

    // Evict, then GET (viewer/owner) must still see the durable buffer.
    await evictSpaceHub(stub);
    const getRes = await SELF.fetch(`${ORIGIN}/v1/tasks/build-1/log`, { headers: { authorization: `Bearer ${viewer.token}` } });
    expect(getRes.status).toBe(200);
    expect(await getRes.json()).toEqual(["line a", "line b", "line c"]);
  });

  it("the ring buffer keeps only the last TASK_LOG_WINDOW (100) lines", async () => {
    const { source, ownerToken } = await bootstrapSpace();
    // Post 130 lines across a couple of calls.
    await SELF.fetch(`${ORIGIN}/v1/tasks/big/log`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({ lines: Array.from({ length: 80 }, (_, i) => `L${i}`) }),
    });
    await SELF.fetch(`${ORIGIN}/v1/tasks/big/log`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({ lines: Array.from({ length: 50 }, (_, i) => `L${80 + i}`) }),
    });

    const lines = (await (await SELF.fetch(`${ORIGIN}/v1/tasks/big/log`, { headers: { authorization: `Bearer ${ownerToken}` } })).json()) as string[];
    expect(lines.length).toBe(100);
    expect(lines[0]).toBe("L30"); // first 30 dropped
    expect(lines[99]).toBe("L129");
  });
});
