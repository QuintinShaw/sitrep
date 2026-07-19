// Required coverage #8 (connection gating), #9 (supersede).
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";
import { bootstrapSpace, connect, helloOffer, helloSource, nextId, resume, subscribe } from "./helpers";

describe("connection gating", () => {
  it("any frame before hello offer gets hello_required and the connection closes", async () => {
    const { viewer } = await bootstrapSpace();
    const client = await connect(viewer.token);
    // Send subscribe before ever sending hello{stage:offer}.
    client.send({ type: "subscribe", id: nextId(), ts: Date.now(), body: {} });
    const err = await client.recv();
    expect(err.type).toBe("error");
    expect(err.body.code).toBe("hello_required");
    expect(err.body.fatal).toBe(true);
    await client.waitForClose();
  });

  it("resume before subscribe on the same connection is rejected as malformed, not silently accepted", async () => {
    const { viewer } = await bootstrapSpace();
    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");
    client.send({ type: "resume", id: nextId(), ts: Date.now(), body: { last_revision: 0 } });
    const err = await client.recv();
    expect(err.type).toBe("error");
    expect(err.body.code).toBe("malformed");
    expect(err.body.retryable).toBe(true);
    expect(err.body.fatal).toBe(false);
    client.close();
  });

  it("no live delta reaches a connection before its own resume reply", async () => {
    const { source, viewer } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloSource(sourceClient, source.device_id);

    const viewerClient = await connect(viewer.token);
    await helloOffer(viewerClient, viewer.device_id, "viewer");
    await subscribe(viewerClient);
    // subscribe is acked, but the connection is not yet delta-eligible: a
    // reliable event applied right now must produce nothing on this socket
    // until resume's reply arrives.
    // subscribe just fired the space's 0->1 lease edge, which notifies every
    // connected source with command{resume_rate} — drain it before relying
    // on ordered recv() below.
    await sourceClient.recv();

    sourceClient.send({
      type: "task.event",
      id: nextId(),
      ts: Date.now(),
      body: { device_id: source.device_id, device_seq: 1, task_id: "run-1", kind: "started", occurred_at: Date.now() },
    });
    const ack = await sourceClient.recv();
    expect(ack.type).toBe("ack");

    await viewerClient.expectSilence(200);

    const reply = await resume(viewerClient, 0);
    expect(reply.type).toBe("snapshot");
    sourceClient.close();
    viewerClient.close();
  });

  it("supersede: an older connection for the same device is closed with `superseded`, not `throttle`", async () => {
    const { source, viewer, spaceId } = await bootstrapSpace();

    const first = await connect(source.token);
    await helloSource(first, source.device_id);

    // An unrelated viewer subscribes throughout — if supersession ever
    // spuriously toggled the lease count, this connection would see a
    // stray throttle/resume_rate command it never asked for.
    const bystander = await connect(viewer.token);
    await helloOffer(bystander, viewer.device_id, "viewer");
    await subscribe(bystander);
    // That subscribe just fired the space's 0->1 lease edge, notifying
    // every connected source (including `first`) with resume_rate — drain
    // it before asserting on `first`'s next message.
    const resumeRate = await first.recv();
    expect(resumeRate.body.action).toBe("resume_rate");

    const second = await connect(source.token);
    await helloSource(second, source.device_id);

    const supersededErr = await first.recv();
    expect(supersededErr.type).toBe("error");
    expect(supersededErr.body.code).toBe("superseded");
    expect(supersededErr.body.fatal).toBe(true);
    await first.waitForClose();

    // Product invariant 5: supersession must never be silent — it's a
    // security-relevant event (possible credential/session reuse) and must
    // always produce a server-side log, independent of the wire error sent
    // above. Console output can't be captured across the workerd/vitest
    // process boundary, so inspect the DO's in-memory security event log
    // directly via runInDurableObject (same pattern used in errors.workers.ts).
    const stub = env.SPACE_HUB.getByName(spaceId);
    const securityEvents = await runInDurableObject(
      stub,
      async (instance: SpaceHub) => (instance as any).securityEventLog as Array<{ event: string; data: Record<string, unknown> }>,
    );
    const supersededLog = securityEvents.find((e) => e.event === "superseded");
    expect(supersededLog).toBeDefined();
    expect(supersededLog?.data.device_id).toBe(source.device_id);
    expect(supersededLog?.data.role).toBe("source");

    // The new connection is fully functional.
    second.send({
      type: "task.event",
      id: nextId(),
      ts: Date.now(),
      body: { device_id: source.device_id, device_seq: 1, task_id: "run-1", kind: "started", occurred_at: Date.now() },
    });
    const ack = await second.recv();
    expect(ack.type).toBe("ack");
    expect(ack.body.acked).toEqual([{ device_id: source.device_id, device_seq: 1 }]);

    await bystander.expectSilence(200);

    second.close();
    bystander.close();
  });

  it("version_unsupported when the offer shares no protocol version with the server", async () => {
    const { viewer } = await bootstrapSpace();
    const client = await connect(viewer.token);
    client.send({
      type: "hello",
      id: nextId(),
      ts: Date.now(),
      body: { stage: "offer", device_id: viewer.device_id, role: "viewer", protocol_versions: [99] },
    });
    const err = await client.recv();
    expect(err.body.code).toBe("version_unsupported");
    expect(err.body.fatal).toBe(true);
    await client.waitForClose();
  });
});
