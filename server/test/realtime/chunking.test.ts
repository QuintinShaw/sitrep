import assert from "node:assert/strict";
import test from "node:test";
import { chunkDeltaEvents, chunkSnapshot } from "../../src/realtime/chunking.ts";
import type { DeltaEventItem, TaskState } from "../../src/realtime/types.ts";

test("chunkSnapshot: empty space still yields exactly one final chunk", () => {
  const chunks = chunkSnapshot(0, { tasks: [], metrics: [], messages: [], automations: [] });
  assert.equal(chunks.length, 1);
  assert.deepEqual(chunks[0], { revision: 0, part: 1, final: true, tasks: [], metrics: [], messages: [], automations: [] });
});

test("chunkSnapshot: splits large task list across multiple parts, same revision, sequential parts, final on last", () => {
  const tasks: TaskState[] = Array.from({ length: 4000 }, (_, i) => ({
    task_id: `task-${i}`,
    device_id: "mac-01",
    state: "running",
    updated_at: 1_700_000_000_000 + i,
    title: "x".repeat(50),
  }));
  const chunks = chunkSnapshot(42, { tasks, metrics: [], messages: [], automations: [] });
  assert.ok(chunks.length > 1, "expected more than one chunk for a large task list");
  for (const [i, chunk] of chunks.entries()) {
    assert.equal(chunk.revision, 42);
    assert.equal(chunk.part, i + 1);
    assert.equal(chunk.final, i === chunks.length - 1);
  }
  const total = chunks.flatMap((c) => c.tasks);
  assert.deepEqual(total, tasks, "concatenating chunks must reproduce the full task list, in order");
});

test("chunkDeltaEvents: chains from/to boundaries across chunks", () => {
  const events: DeltaEventItem[] = Array.from({ length: 3000 }, (_, i) => ({
    event_type: "task.event",
    event: {
      device_id: "mac-01",
      device_seq: i + 1,
      task_id: `run-${i}`,
      kind: "progress",
      occurred_at: 1_700_000_000_000 + i,
      percent: i % 100,
    },
  }));
  const deltas = chunkDeltaEvents(100, events);
  assert.ok(deltas.length > 1);
  assert.equal(deltas[0].from_revision, 100);
  for (let i = 1; i < deltas.length; i++) {
    assert.equal(deltas[i].from_revision, deltas[i - 1].to_revision, "chained deltas must connect exactly");
  }
  assert.equal(deltas.at(-1)!.to_revision, 100 + events.length);
  for (const d of deltas) assert.equal(d.to_revision - d.from_revision, d.events.length);
});

test("chunkDeltaEvents: empty range still yields one delta with matching from/to", () => {
  const deltas = chunkDeltaEvents(5, []);
  assert.deepEqual(deltas, [{ from_revision: 5, to_revision: 5, events: [] }]);
});
