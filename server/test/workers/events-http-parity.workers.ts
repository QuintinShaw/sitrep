// POST /v1/events shared-ingest parity with the WS path (v1-architecture.md
// §4): same applyTaskEvent/applyMessageEvent/ingestMetricFrame, same
// (device_id, device_seq) dedup counter regardless of which transport sent
// which frame, same per-device metric.frame rate limit (§4.3), and presence
// stamping (§7.1).
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, connect, helloSource, sendMetricFrame, sendTaskEvent } from "./helpers.ts";

const ORIGIN = "https://example.com";

async function postEvents(token: string, events: unknown[]): Promise<{ status: number; body: any }> {
  const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ events }),
  });
  return { status: res.status, body: await res.json() };
}

const taskEnvelope = (deviceId: string, deviceSeq: number, taskId = "build-1") => ({
  type: "task.event",
  id: `e${deviceSeq}`,
  ts: Date.now(),
  body: { device_id: deviceId, device_seq: deviceSeq, task_id: taskId, kind: "started", occurred_at: Date.now() },
});

describe("POST /v1/events — shared ingest parity with WS", () => {
  it("role gate: only source may POST /v1/events", async () => {
    const { viewer } = await bootstrapSpace();
    const { status, body } = await postEvents(viewer.token, [taskEnvelope(viewer.device_id, 1)]);
    expect(status).toBe(403);
    expect(body).toEqual({ error: "forbidden" });
  });

  it("malformed body (not {events:[...]})-> 400", async () => {
    const { source } = await bootstrapSpace();
    const res = await SELF.fetch(`${ORIGIN}/v1/events`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.token}`, "content-type": "application/json" },
      body: JSON.stringify({ notEvents: [] }),
    });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "malformed" });
  });

  it("a device_id in the body that doesn't match the authenticated identity is rejected per-item, not batch-fatal", async () => {
    const { source } = await bootstrapSpace();
    const { status, body } = await postEvents(source.token, [taskEnvelope("someone-else", 1)]);
    expect(status).toBe(200);
    expect(body.results[0].status).toBe("rejected");
    expect(body.results[0].error.code).toBe("unauthorized");
  });

  it("device_seq dedup is a SINGLE counter shared across HTTP and WS (v1-architecture.md §4.1)", async () => {
    const { source, ownerToken } = await bootstrapSpace();

    // First over HTTP.
    const first = await postEvents(source.token, [taskEnvelope(source.device_id, 1)]);
    expect(first.body.results[0].status).toBe("applied");
    const revisionAfterHttp = first.body.space_revision;

    // Same device_seq, now over WS: must be reported as a duplicate, not
    // re-applied, and space_revision must not advance.
    const client = await connect(source.token);
    await helloSource(client, source.device_id);
    // sendTaskEvent always sends kind:"started" by default with the same
    // device_seq value we already used over HTTP.
    await sendTaskEvent(client, source.device_id, 1);
    client.close();

    const snapshotRes = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    const snapshot = (await snapshotRes.json()) as { space_revision: number };
    expect(snapshot.space_revision).toBe(revisionAfterHttp);

    // And retrying the exact same HTTP batch again reports duplicate, with
    // the same space_revision — safe to retry unconditionally (§4.2).
    const retry = await postEvents(source.token, [taskEnvelope(source.device_id, 1)]);
    expect(retry.body.results[0].status).toBe("duplicate");
    expect(retry.body.space_revision).toBe(revisionAfterHttp);
    expect(retry.body.acked).toEqual([{ device_id: source.device_id, device_seq: 1 }]);
  });

  it("a mixed batch is processed per-item; one rejected item doesn't abort the rest", async () => {
    const { source } = await bootstrapSpace();
    const { status, body } = await postEvents(source.token, [
      taskEnvelope(source.device_id, 1, "task-a"),
      { type: "task.event", id: "bad", ts: Date.now(), body: { device_id: source.device_id, device_seq: -1, task_id: "x", kind: "started", occurred_at: Date.now() } },
      taskEnvelope(source.device_id, 2, "task-b"),
    ]);
    expect(status).toBe(200);
    expect(body.results.map((r: any) => r.status)).toEqual(["applied", "rejected", "applied"]);
  });

  it("per-device metric.frame rate limit (10/s) is shared across HTTP and WS (v1-architecture.md §4.3)", async () => {
    const { source } = await bootstrapSpace();
    const metricEnvelope = (i: number) => ({
      type: "metric.frame",
      id: `m${i}`,
      ts: Date.now(),
      body: { device_id: source.device_id, metrics: [{ metric_id: "cpu.load1", value: String(i), ts: Date.now() + i }] },
    });

    // 8 over HTTP...
    const eight = await postEvents(source.token, Array.from({ length: 8 }, (_, i) => metricEnvelope(i)));
    expect(eight.body.results.every((r: any) => r.status === "applied")).toBe(true);

    // ...then 2 more over WS should still be within budget...
    const client = await connect(source.token);
    await helloSource(client, source.device_id);
    sendMetricFrame(client, source.device_id, [{ metric_id: "cpu.load1", value: "8", ts: Date.now() + 100 }]);
    sendMetricFrame(client, source.device_id, [{ metric_id: "cpu.load1", value: "9", ts: Date.now() + 200 }]);

    // ...but the 11th frame for this device within the same second (now over
    // WS) must be rejected, because the limiter is per-device, not
    // per-connection/per-transport.
    sendMetricFrame(client, source.device_id, [{ metric_id: "cpu.load1", value: "10", ts: Date.now() + 300 }]);
    const err = await client.recv();
    expect(err.type).toBe("error");
    expect(err.body.code).toBe("rate_limited");
    client.close();
  });

  it("presence: ingest_last_seen is stamped by ingest (WS or HTTP), sources_online counts live source WS", async () => {
    const { source, ownerToken } = await bootstrapSpace();
    await postEvents(source.token, [taskEnvelope(source.device_id, 1)]);

    const before = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    const beforeBody = (await before.json()) as { presence: { ingest_last_seen?: number; sources_online: number } };
    expect(beforeBody.presence.ingest_last_seen).toBeTypeOf("number");
    expect(beforeBody.presence.sources_online).toBe(0);

    const client = await connect(source.token);
    await helloSource(client, source.device_id);

    const after = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${ownerToken}` } });
    const afterBody = (await after.json()) as { presence: { sources_online: number } };
    expect(afterBody.presence.sources_online).toBe(1);
    client.close();
  });
});
