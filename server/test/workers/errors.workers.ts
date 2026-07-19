// Top-level exception guard: an unexpected throw inside any message handler
// must degrade to error{internal_error} (retryable, non-fatal, no internal
// details leaked per SPEC.md section 13) with the connection left usable.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";
import { bootstrapSpace, connect, helloOffer, nextId, subscribe } from "./helpers";

describe("internal_error degradation", () => {
  it("an injected handler exception yields error{internal_error} without closing the connection or leaking details", async () => {
    const { spaceId, viewer } = await bootstrapSpace();
    const client = await connect(viewer.token);
    await helloOffer(client, viewer.device_id, "viewer");

    // Inject a fault into the live DO instance (same isolate as the open
    // connection in vitest-pool-workers): the next subscribe throws.
    const stub = env.SPACE_HUB.getByName(spaceId);
    await runInDurableObject(stub, async (instance: SpaceHub) => {
      (instance as any).handleSubscribe = () => {
        throw new Error("boom /internal/secret/path/space-hub.ts:123");
      };
    });

    client.send({ type: "subscribe", id: nextId(), ts: Date.now(), body: {} });
    const err = await client.recv();
    expect(err.type).toBe("error");
    expect(err.body.code).toBe("internal_error");
    expect(err.body.retryable).toBe(true);
    expect(err.body.fatal).toBe(false);
    expect(err.body.message).not.toContain("boom");
    expect(err.body.message).not.toContain("space-hub");

    // Remove the injected fault (restores the prototype method) and verify
    // the same connection still works — internal_error is non-fatal.
    await runInDurableObject(stub, async (instance: SpaceHub) => {
      delete (instance as any).handleSubscribe;
    });
    const ack = await subscribe(client);
    expect(ack.type).toBe("ack");
    expect(ack.body.lease.expires_at).toBeGreaterThan(Date.now());

    client.close();
  });
});
