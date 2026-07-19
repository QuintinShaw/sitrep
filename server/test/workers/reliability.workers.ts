// Required coverage #2 (dedup), #3 (revision gap/current/unavailable),
// #12 (push_outbox idempotency), and #1 (attachment survives
// reconstruction — see the "recovery" test below and its comment).
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";
import { bootstrapSpace, connect, helloOffer, helloSource, nextId, resume, sendTaskEvent, subscribe } from "./helpers";

describe("reliable event dedup and revision accounting", () => {
  it("a duplicate device_seq re-acks without applying twice or bumping revision again", async () => {
    const { source, viewer } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);

    const viewerClient = await connect(viewer.token);
    await helloOffer(viewerClient, viewer.device_id, "viewer");
    await subscribe(viewerClient);
    await resume(viewerClient, 0); // now delta-eligible
    // subscribe's 0->1 lease edge notified the source with resume_rate;
    // drain it before relying on ordered recv() on sourceClient below.
    await sourceClient.recv();

    const envelope = {
      type: "task.event",
      id: nextId(),
      ts: Date.now(),
      body: { device_id: source.device_id, device_seq: 1, task_id: "run-1", kind: "started", occurred_at: Date.now() },
    };
    sourceClient.send(envelope);
    const ack1 = await sourceClient.recv();
    expect(ack1.body.acked).toEqual([{ device_id: source.device_id, device_seq: 1 }]);
    const delta1 = await viewerClient.recv();
    expect(delta1.type).toBe("delta");
    expect(delta1.body.to_revision).toBe(1);

    // Resend the identical device_seq in a fresh envelope (new id/ts), as a
    // real source would after a dropped ack (SPEC.md section 5.4).
    sourceClient.send({ ...envelope, id: nextId(), ts: Date.now() });
    const ack2 = await sourceClient.recv();
    expect(ack2.body.acked).toEqual([{ device_id: source.device_id, device_seq: 1 }]);

    // No second delta was broadcast for the retry.
    await viewerClient.expectSilence(200);

    // And the space's revision only advanced once.
    const current = await resume(viewerClient, 1);
    expect(current.type).toBe("delta");
    expect(current.body.from_revision).toBe(1);
    expect(current.body.to_revision).toBe(1);
    expect(current.body.events).toEqual([]);

    sourceClient.close();
    viewerClient.close();
  });

  it("event_id in push_outbox is idempotent across a duplicate device_seq retry", async () => {
    const { spaceId, source } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);
    await sendTaskEvent(sourceClient, source.device_id, 1);
    await sendTaskEvent(sourceClient, source.device_id, 1); // duplicate
    sourceClient.close();

    const stub = env.SPACE_HUB.getByName(spaceId);
    await runInDurableObject(stub, async (_instance: SpaceHub, state) => {
      const outbox = state.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM push_outbox").toArray()[0];
      expect(outbox.n).toBe(1);
      const events = state.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM event_log").toArray()[0];
      expect(events.n).toBe(1);
      const dedup = state.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM dedup").toArray()[0];
      expect(dedup.n).toBe(1);
    });
  });
});

describe("resume decision table", () => {
  it("last_revision 0 on a fresh space yields an (empty) snapshot", async () => {
    const { viewer } = await bootstrapSpace();
    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");
    await subscribe(client);
    const reply = await resume(client, 0);
    expect(reply.type).toBe("snapshot");
    expect(reply.body.revision).toBe(0);
    expect(reply.body.final).toBe(true);
    expect(reply.body.tasks).toEqual([]);
    client.close();
  });

  it("last_revision == current revision yields the explicit empty delta", async () => {
    const { source, viewer } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);
    await sendTaskEvent(sourceClient, source.device_id, 1);

    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");
    await subscribe(client);
    const reply = await resume(client, 1);
    expect(reply.type).toBe("delta");
    expect(reply.body.from_revision).toBe(1);
    expect(reply.body.to_revision).toBe(1);
    expect(reply.body.events).toEqual([]);
    sourceClient.close();
    client.close();
  });

  it("last_revision > current revision yields revision_unavailable and no eligibility", async () => {
    const { viewer } = await bootstrapSpace();
    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");
    await subscribe(client);
    const reply = await resume(client, 999);
    expect(reply.type).toBe("error");
    expect(reply.body.code).toBe("revision_unavailable");
    expect(reply.body.retryable).toBe(true);
    client.close();
  });

  it("0 < last_revision < current, fully retained, yields a catch-up delta (not a snapshot)", async () => {
    const { source, viewer } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);
    await sendTaskEvent(sourceClient, source.device_id, 1, { kind: "started" });
    await sendTaskEvent(sourceClient, source.device_id, 2, { kind: "progress", percent: 50 });
    await sendTaskEvent(sourceClient, source.device_id, 3, { kind: "done" });

    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");
    await subscribe(client);
    const reply = await resume(client, 1);
    expect(reply.type).toBe("delta");
    expect(reply.body.from_revision).toBe(1);
    expect(reply.body.to_revision).toBe(3);
    expect(reply.body.events.length).toBe(2);
    sourceClient.close();
    client.close();
  });

  it("a range whose retention was lost falls back to a snapshot instead of an error", async () => {
    const { spaceId, source, viewer } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);
    await sendTaskEvent(sourceClient, source.device_id, 1);
    await sendTaskEvent(sourceClient, source.device_id, 2);
    await sendTaskEvent(sourceClient, source.device_id, 3);
    sourceClient.close();

    // Simulate the retention window having moved past revision 1 (the real
    // window is 1000 revisions; forcing it directly keeps this test fast
    // and precise rather than sending 1000+ real events).
    const stub = env.SPACE_HUB.getByName(spaceId);
    await runInDurableObject(stub, async (_instance: SpaceHub, state) => {
      state.storage.sql.exec("DELETE FROM event_log WHERE revision <= 2");
    });

    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");
    await subscribe(client);
    const reply = await resume(client, 1); // wants (1,3], but revision 2 is gone
    expect(reply.type).toBe("snapshot");
    expect(reply.body.revision).toBe(3);
    client.close();
  });
});

describe("connection identity recovery", () => {
  // A true forced eviction/hibernation cycle isn't triggerable from the
  // outside in vitest-pool-workers. What we can and do verify: (a) the
  // DO's only source of connection identity is the per-connection
  // WebSocket attachment (never an in-memory map keyed by connection —
  // reviewed in space-hub.ts, every handler calls
  // ws.deserializeAttachment() fresh), and (b) all durable facts (revision,
  // dedup, folded task state) live in SQLite and are correctly rebuilt by a
  // brand new connection/stub reference with no shared JS state, which is
  // exactly what a reconstructed DO instance would see after eviction.
  it("a fresh connection for the same device, and a fresh stub reference, both observe identical durable state", async () => {
    const { spaceId, source } = await bootstrapSpace();
    const first = await connect(source.token);
    await helloSource(first, source.device_id);
    await sendTaskEvent(first, source.device_id, 1, { kind: "started", title: "build" });
    first.close();

    // A brand new WebSocket connection (fresh attachment, no memory of the
    // previous one) still authenticates correctly and can continue the
    // device's device_seq sequence — proving identity isn't cached
    // anywhere but the attachment + storage.
    const second = await connect(source.token);
    const { accept } = await helloSource(second, source.device_id);
    expect(accept.body.stage).toBe("accept");
    await sendTaskEvent(second, source.device_id, 2, { kind: "done" });
    second.close();

    // A fresh DurableObjectStub reference (not reusing any object from
    // above) reads the exact same durable state straight from storage.
    const stub = env.SPACE_HUB.getByName(spaceId);
    await runInDurableObject(stub, async (_instance: SpaceHub, state) => {
      const revision = state.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key='revision'").toArray()[0];
      expect(Number(revision.value)).toBe(2);
      const task = state.storage.sql.exec<{ state: string; title: string }>("SELECT state, title FROM tasks WHERE task_id='run-1'").toArray()[0];
      expect(task.state).toBe("done");
      expect(task.title).toBe("build");
    });
  });
});
