// Required coverage #6: the space's active-lease count's 1<->0 edges
// notify every connected source with command{throttle}/command{resume_rate}
// (SPEC.md section 7). We drive the 1->0 edge via explicit `unsubscribe`
// rather than waiting out the real 30-60s lease TTL — SPEC.md section 7
// explicitly names unsubscribe as one of exactly two ways a lease ends,
// and both paths run through the same reconcileLeaseEdge() call.
import { describe, expect, it } from "vitest";
import { bootstrapSpace, connect, helloOffer, subscribe, unsubscribe } from "./helpers";

describe("interest lease edges", () => {
  it("0->1 sends resume_rate; 1->0 sends throttle; a later 0->1 sends resume_rate again", async () => {
    const { source, viewer, inviteAndJoin } = await bootstrapSpace();

    const sourceClient = await connect(source.token);
    await helloOffer(sourceClient, source.device_id, "source");

    const v1 = await connect(viewer.token);
    await helloOffer(v1, viewer.device_id, "viewer");
    await subscribe(v1); // 0 -> 1

    const resumeRate1 = await sourceClient.recv();
    expect(resumeRate1.type).toBe("command");
    expect(resumeRate1.body.origin).toBe("server");
    expect(resumeRate1.body.action).toBe("resume_rate");

    await unsubscribe(v1); // 1 -> 0 (only lease in the space)

    const throttle = await sourceClient.recv();
    expect(throttle.type).toBe("command");
    expect(throttle.body.origin).toBe("server");
    expect(throttle.body.action).toBe("throttle");

    const viewer2 = await inviteAndJoin("viewer");
    const v2 = await connect(viewer2.token);
    await helloOffer(v2, viewer2.device_id, "viewer");
    await subscribe(v2); // 0 -> 1 again

    const resumeRate2 = await sourceClient.recv();
    expect(resumeRate2.type).toBe("command");
    expect(resumeRate2.body.action).toBe("resume_rate");

    sourceClient.close();
    v1.close();
    v2.close();
  });

  it("a second viewer subscribing while one is already active does not re-fire resume_rate", async () => {
    const { source, viewer, inviteAndJoin } = await bootstrapSpace();
    const sourceClient = await connect(source.token);
    await helloOffer(sourceClient, source.device_id, "source");

    const v1 = await connect(viewer.token);
    await helloOffer(v1, viewer.device_id, "viewer");
    await subscribe(v1); // 0 -> 1
    await sourceClient.recv(); // resume_rate

    const viewer2 = await inviteAndJoin("viewer");
    const v2 = await connect(viewer2.token);
    await helloOffer(v2, viewer2.device_id, "viewer");
    await subscribe(v2); // 1 -> 2, not an edge

    await sourceClient.expectSilence(200);

    sourceClient.close();
    v1.close();
    v2.close();
  });
});
