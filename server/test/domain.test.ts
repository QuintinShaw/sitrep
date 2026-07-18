import assert from "node:assert/strict";
import test from "node:test";
import { makeSnapshot } from "../src/domain.ts";

test("snapshot keeps metric rules owned by the metric", () => {
  const snapshot = makeSnapshot({
    now: "2026-07-18T00:00:00.000Z",
    presence: {},
    tasks: [],
    events: [],
    automations: [],
    metrics: [{
      key: "aapl",
      value: "169.1",
      updated_at: "2026-07-18T00:00:00.000Z",
      alert_below: "160",
      source: "aabc",
    }],
  });

  assert.equal(snapshot.metrics[0].automation_id, "abc");
  assert.deepEqual(snapshot.metrics[0].thresholds, [
    { id: "below", direction: "below", value: "160", severity: "warning" },
  ]);
});

test("snapshot never exposes an automation command", () => {
  const snapshot = makeSnapshot({
    now: "2026-07-18T00:00:00.000Z",
    presence: {},
    tasks: [],
    metrics: [],
    events: [],
    automations: [{
      id: "one",
      name: "Check news",
      command: ["agent", "secret prompt"],
      every_s: 300,
      enabled: true,
      created_at: "2026-07-18T00:00:00.000Z",
      updated_at: "2026-07-18T00:00:00.000Z",
    }],
  });

  assert.equal(snapshot.automations[0].executor.kind, "script");
  assert.equal("command" in snapshot.automations[0], false);
});
