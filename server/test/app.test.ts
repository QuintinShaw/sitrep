import assert from "node:assert/strict";
import test from "node:test";
import { createApp, parseRealtimeEnabledFlag } from "../src/app.ts";
import { SqliteStore } from "../src/sqlite-store.ts";

function testApp() {
  const store = new SqliteStore(":memory:");
  return createApp({ store: () => store });
}

test("v2 snapshot realtime_enabled defaults false when unwired", async () => {
  const app = testApp();
  const snapshot = await (await app.request("/v2/snapshot")).json() as { realtime_enabled: boolean };
  assert.equal(snapshot.realtime_enabled, false);
});

test("v2 snapshot realtime_enabled reflects the configured var", async () => {
  const store = new SqliteStore(":memory:");
  const app = createApp({ store: () => store, realtimeEnabled: () => true });
  const snapshot = await (await app.request("/v2/snapshot")).json() as { realtime_enabled: boolean };
  assert.equal(snapshot.realtime_enabled, true);
});

test("parseRealtimeEnabledFlag: only true/'true'/'1' enable, everything else disables", () => {
  // Dashboard variable overrides always arrive as strings at runtime even
  // though wrangler.jsonc declares REALTIME_ENABLED as boolean — a naive
  // Boolean(value) would treat the string "false" as truthy. This parser
  // must be a strict allow-list, not a truthiness check.
  const enabling: Array<string | boolean> = ["true", "TRUE ", " true", "1"];
  for (const value of enabling) {
    assert.equal(parseRealtimeEnabledFlag(value), true, `expected ${JSON.stringify(value)} to enable`);
  }
  assert.equal(parseRealtimeEnabledFlag(true), true);

  const disabling: Array<string | boolean | undefined> = [false, "false", "0", "", undefined, "yes", "TRUE_ISH", "truex"];
  for (const value of disabling) {
    assert.equal(parseRealtimeEnabledFlag(value), false, `expected ${JSON.stringify(value)} to disable`);
  }
});

test("Node ingest returns success and records presence", async () => {
  const app = testApp();
  const response = await app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      kind: "metric.update",
      source_id: "test-source",
      ts: "2026-07-18T00:00:00Z",
      key: "cpu",
      value: "42",
      label: "CPU",
    }),
  });

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { accepted: 1, commands: {} });

  const snapshotResponse = await app.request("/v2/snapshot");
  const snapshot = await snapshotResponse.json() as {
    metrics: Array<{ id: string; value: string }>;
    presence: { ingest_last_seen?: string };
  };
  assert.deepEqual(snapshot.metrics.map(({ id, value }) => ({ id, value })), [{ id: "cpu", value: "42" }]);
  assert.ok(snapshot.presence.ingest_last_seen);
});

test("v2 snapshot is the complete client read model", async () => {
  const app = testApp();
  await app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      kind: "metric.update",
      source_id: "aauto1",
      ts: "2026-07-18T00:00:00Z",
      key: "cpu",
      value: "42",
      label: "CPU",
      alert_above: "90",
    }),
  });

  const response = await app.request("/v2/snapshot");
  assert.equal(response.status, 200);
  const snapshot = await response.json() as {
    version: number;
    metrics: Array<{ id: string; automation_id?: string; thresholds: unknown[] }>;
    automations: unknown[];
  };
  assert.equal(snapshot.version, 2);
  assert.equal(snapshot.metrics[0].id, "cpu");
  assert.equal(snapshot.metrics[0].automation_id, "auto1");
  assert.equal(snapshot.metrics[0].thresholds.length, 1);
  assert.ok(Array.isArray(snapshot.automations));
});

test("v2 ingest accepts messages and rejects removed verbs", async () => {
  const app = testApp();
  const message = await app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      kind: "message.send",
      source_id: "a1",
      ts: "2026-07-18T00:00:00Z",
      text: "done",
      level: "info",
    }),
  });
  assert.equal(message.status, 200);

  const removed = await app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      kind: "param.declare",
      source_id: "a1",
      ts: "2026-07-18T00:00:00Z",
      key: "old",
      value: "1",
    }),
  });
  assert.equal(removed.status, 400);
});

test("metric-owned threshold overrides drive crossing messages", async () => {
  const app = testApp();
  const sample = (value: string) => app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      kind: "metric.update",
      source_id: "a1",
      ts: new Date().toISOString(),
      key: "cpu",
      value,
      label: "CPU",
      alert_above: "90",
    }),
  });
  await sample("40");
  const preference = await app.request("/v2/metrics/cpu", {
    method: "PATCH",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ alert_above: "50" }),
  });
  assert.equal(preference.status, 200);
  await sample("60");

  const snapshot = await (await app.request("/v2/snapshot")).json() as {
    metrics: Array<{ thresholds: Array<{ value: string }> }>;
    messages: Array<{ body: string }>;
  };
  assert.equal(snapshot.metrics[0].thresholds[0].value, "50");
  assert.match(snapshot.messages[0].body, /50/);
});

test("ingest rejects malformed batches", async () => {
  const app = testApp();
  const response = await app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ kind: "metric.update" }),
  });

  assert.equal(response.status, 400);
});

test("ingest rejects unknown event kinds and invalid timestamps", async () => {
  const app = testApp();
  const unknown = await app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ kind: "task.explode", source_id: "test", ts: "2026-07-18T00:00:00Z" }),
  });
  assert.equal(unknown.status, 400);

  const badTimestamp = await app.request("/v2/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ kind: "task.start", source_id: "test", ts: "tomorrow" }),
  });
  assert.equal(badTimestamp.status, 400);
});
