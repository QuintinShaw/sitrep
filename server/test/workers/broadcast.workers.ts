// Required coverage #4 (metric.frame never writes SQLite / never bumps
// revision), #5 (one metric.frame reaches multiple viewers), #10 (chunked
// snapshot: same revision, sequential parts, final closes it, nothing else
// interleaves).
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";
import { bootstrapSpace, connect, helloOffer, nextId, resume, sendTaskEvent, subscribe } from "./helpers";

function tableCounts(state: any) {
  const tables = ["event_log", "dedup", "tasks", "messages", "automations", "leases", "push_outbox", "space_meta"];
  const counts: Record<string, number> = {};
  for (const t of tables) {
    counts[t] = state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) as n FROM ${t}`).toArray()[0].n;
  }
  return counts;
}

describe("metric.frame", () => {
  it("never writes SQLite and never advances space_revision", async () => {
    const { spaceId, source, viewer } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloOffer(sourceClient, source.device_id, "source");

    const viewerClient = await connect(viewer.token);
    await helloOffer(viewerClient, viewer.device_id, "viewer");
    await subscribe(viewerClient, ["metric"]);
    await resume(viewerClient, 0);

    const stub = env.SPACE_HUB.getByName(spaceId);
    const before = await runInDurableObject(stub, async (_i: SpaceHub, state) => tableCounts(state));

    sourceClient.send({
      type: "metric.frame",
      id: nextId(),
      ts: Date.now(),
      body: { device_id: source.device_id, metrics: [{ metric_id: "cpu.load", value: "42", ts: Date.now() }] },
    });
    const frame = await viewerClient.recv();
    expect(frame.type).toBe("metric.frame");
    expect(frame.body.metrics[0].metric_id).toBe("cpu.load");

    const after = await runInDurableObject(stub, async (_i: SpaceHub, state) => tableCounts(state));
    expect(after).toEqual(before);

    sourceClient.close();
    viewerClient.close();
  });

  it("broadcasts one frame to every eligible viewer", async () => {
    const { source, viewer, inviteAndJoin } = await bootstrapSpace();
    const viewer2 = await inviteAndJoin("viewer");

    const sourceClient = await connect(source.token);
    await helloOffer(sourceClient, source.device_id, "source");

    const v1 = await connect(viewer.token);
    await helloOffer(v1, viewer.device_id, "viewer");
    await subscribe(v1, ["metric"]);
    await resume(v1, 0);

    const v2 = await connect(viewer2.token);
    await helloOffer(v2, viewer2.device_id, "viewer");
    await subscribe(v2, ["metric"]);
    await resume(v2, 0);

    sourceClient.send({
      type: "metric.frame",
      id: nextId(),
      ts: Date.now(),
      body: { device_id: source.device_id, metrics: [{ metric_id: "mem.used", value: "1024", ts: Date.now() }] },
    });

    const [f1, f2] = await Promise.all([v1.recv(), v2.recv()]);
    expect(f1.body.metrics[0].value).toBe("1024");
    expect(f2.body.metrics[0].value).toBe("1024");

    sourceClient.close();
    v1.close();
    v2.close();
  });
});

describe("chunked snapshot", () => {
  it("splits a large snapshot into parts sharing one revision, sequential, final last, with nothing else interleaved", async () => {
    const { spaceId, source, viewer } = await bootstrapSpace();

    // Seed enough state directly (fast + precise) to force multiple chunks
    // well under the 64 KiB frame limit's soft chunk threshold.
    const stub = env.SPACE_HUB.getByName(spaceId);
    await runInDurableObject(stub, async (_i: SpaceHub, state) => {
      const bigTitle = "x".repeat(2000);
      for (let i = 0; i < 60; i++) {
        state.storage.sql.exec(
          `INSERT INTO tasks (task_id, device_id, title, state, percent, step, message, updated_at, display)
           VALUES (?, ?, ?, 'running', 50, NULL, NULL, ?, NULL)`,
          `run-${i}`,
          source.device_id,
          bigTitle,
          Date.now(),
        );
      }
      state.storage.sql.exec(
        "INSERT INTO space_meta (key, value) VALUES ('revision', '1') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      );
      // A single reliable event so a live delta exists to race against the
      // chunk stream below.
      state.storage.sql.exec(
        `INSERT INTO event_log (revision, event_type, device_id, device_seq, occurred_at, payload) VALUES (1, 'task.event', ?, 1, ?, ?)`,
        source.device_id,
        Date.now(),
        JSON.stringify({ device_id: source.device_id, device_seq: 1, task_id: "run-0", kind: "started", occurred_at: Date.now() }),
      );
      state.storage.sql.exec(`INSERT INTO dedup (device_id, device_seq, revision) VALUES (?, 1, 1)`, source.device_id);
    });

    const sourceClient = await connect(source.token);
    await helloOffer(sourceClient, source.device_id, "source");

    const viewerClient = await connect(viewer.token);
    await helloOffer(viewerClient, viewer.device_id, "viewer");
    await subscribe(viewerClient);

    viewerClient.send({ type: "resume", id: nextId(), ts: Date.now(), body: { last_revision: 0 } });

    // Race a live event against the in-flight chunked snapshot: if the
    // server ever interleaved it between chunks, the assertions below
    // would see a non-"snapshot" envelope before `final`.
    sourceClient.send({
      type: "task.event",
      id: nextId(),
      ts: Date.now(),
      body: { device_id: source.device_id, device_seq: 2, task_id: "run-1", kind: "done", occurred_at: Date.now() },
    });

    let final = false;
    let partCount = 0;
    let sawRevision: number | null = null;
    while (!final) {
      const msg = await viewerClient.recv();
      expect(msg.type).toBe("snapshot");
      partCount += 1;
      expect(msg.body.part).toBe(partCount);
      if (sawRevision === null) sawRevision = msg.body.revision;
      expect(msg.body.revision).toBe(sawRevision);
      final = msg.body.final;
    }
    expect(partCount).toBeGreaterThan(1);

    sourceClient.close();
    viewerClient.close();
  });
});
