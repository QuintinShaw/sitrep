// Auth resolution (sr1 tokens only, no admin/AUTH_TOKEN) + control-plane
// routes: spaces, join, invites, devices, revocation force-close.
// v1-architecture.md §3, §10.2, §10.4.
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, connect, helloOffer } from "./helpers.ts";

const ORIGIN = "https://example.com";

describe("auth: sr1 tokens only, no admin/AUTH_TOKEN path", () => {
  it("no bearer token at all -> 401", async () => {
    const res = await SELF.fetch(`${ORIGIN}/v1/snapshot`);
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ error: "unauthorized" });
  });

  it("a bare/arbitrary secret (the shape of the old AUTH_TOKEN) -> 401, never an admin role", async () => {
    // Confirms the deleted admin/AUTH_TOKEN branch really is gone: there is
    // no code path where an arbitrary shared secret resolves to anything.
    const res = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: "Bearer supersecretadmintoken" } });
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ error: "unauthorized" });
  });

  it("a malformed sr1-shaped token -> 401", async () => {
    const res = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: "Bearer sr1_notenoughhex_deadbeef" } });
    expect(res.status).toBe(401);
  });

  it("a well-formed but unresolvable sr1 token -> 401", async () => {
    const res = await SELF.fetch(`${ORIGIN}/v1/snapshot`, {
      headers: { authorization: "Bearer sr1_zzzzzzzzzz_0123456789abcdef0123456789abcdef0123456789abcdef" },
    });
    expect(res.status).toBe(401);
  });

  it("401 vs 403: revoked token is 401, wrong role is 403 (v1-architecture.md §3)", async () => {
    const { ownerToken, viewer } = await bootstrapSpace();

    // Wrong role: viewer may not POST /v1/automations (owner only).
    const forbidden = await SELF.fetch(`${ORIGIN}/v1/automations`, {
      method: "POST",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ name: "n", executor_kind: "script", schedule: { every_seconds: 60 } }),
    });
    expect(forbidden.status).toBe(403);
    expect(await forbidden.json()).toEqual({ error: "forbidden" });

    // Now revoke the viewer and confirm it's 401 (not 403) on a route it
    // WAS allowed to call.
    await SELF.fetch(`${ORIGIN}/v1/devices/${viewer.device_id}`, { method: "DELETE", headers: { authorization: `Bearer ${ownerToken}` } });
    const revoked = await SELF.fetch(`${ORIGIN}/v1/snapshot`, { headers: { authorization: `Bearer ${viewer.token}` } });
    expect(revoked.status).toBe(401);
    expect(await revoked.json()).toEqual({ error: "unauthorized" });
  });
});

describe("control plane: spaces, join, invites, devices", () => {
  it("POST /v1/spaces mints a space + owner token; POST /v1/join redeems an invite", async () => {
    const { spaceId, ownerToken, source, viewer } = await bootstrapSpace();
    expect(spaceId).toMatch(/^[a-z0-9]{1,16}$/);
    expect(ownerToken).toMatch(/^sr1_[a-z0-9]{1,16}_[a-f0-9]{48}$/);
    expect(source.role).toBe("source");
    expect(viewer.role).toBe("viewer");

    const devicesRes = await SELF.fetch(`${ORIGIN}/v1/devices`, { headers: { authorization: `Bearer ${ownerToken}` } });
    const devices = (await devicesRes.json()) as Array<{ id: string; role: string }>;
    expect(devices.map((d) => d.role).sort()).toEqual(["owner", "source", "viewer"]);
  });

  it("an invite code is single-use: a second join with the same code 404s", async () => {
    const { ownerToken, spaceId } = await bootstrapSpace();
    const inviteRes = await SELF.fetch(`${ORIGIN}/v1/invites`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ role: "viewer" }),
    });
    const { code } = (await inviteRes.json()) as { code: string };

    const first = await SELF.fetch(`${ORIGIN}/v1/join`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code, space: spaceId, name: "d1", platform: "test" }),
    });
    expect(first.status).toBe(200);

    const second = await SELF.fetch(`${ORIGIN}/v1/join`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code, space: spaceId, name: "d2", platform: "test" }),
    });
    expect(second.status).toBe(404);
  });

  it("DELETE /v1/devices/:id force-closes every live WS for that device (v1-architecture.md §10.2)", async () => {
    const { ownerToken, viewer } = await bootstrapSpace();
    const client = await connect(viewer.token);
    const helloAccept = await helloOffer(client, viewer.device_id, "viewer");
    expect(helloAccept.body.stage).toBe("accept");

    const revokeRes = await SELF.fetch(`${ORIGIN}/v1/devices/${viewer.device_id}`, { method: "DELETE", headers: { authorization: `Bearer ${ownerToken}` } });
    expect(revokeRes.status).toBe(200);

    const errorFrame = await client.recv();
    expect(errorFrame.type).toBe("error");
    expect(errorFrame.body.code).toBe("unauthenticated");
    expect(errorFrame.body.retryable).toBe(false);
    expect(errorFrame.body.fatal).toBe(true);

    const closed = await client.waitForClose();
    expect(closed.code).toBe(1008);
    client.close();
  });

  it("P0-6 self-routing connect code: minted code embeds space_id, join is zero-KV (v1-architecture.md §10.5)", async () => {
    const { ownerToken, spaceId } = await bootstrapSpace();
    const inviteRes = await SELF.fetch(`${ORIGIN}/v1/invites`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ role: "viewer" }),
    });
    const { code, space_id } = (await inviteRes.json()) as { code: string; space_id: string };
    expect(space_id).toBe(spaceId);
    // 21-char self-routing layout: 'X' + space_id(10, uppercased) + secret(9) + 'Z'.
    expect(code).toMatch(/^X[2-9A-HJ-KM-NP-Z]{19}Z$/);
    expect(code.length).toBe(21);
    expect(code.slice(1, 11).toLowerCase()).toBe(spaceId);

    // The client decodes space_id from the code locally and sends it
    // explicitly — this is the ONLY thing the Worker uses to route (no KV
    // anywhere in this flow, grep-verifiable in adapters/workers.ts).
    const joinRes = await SELF.fetch(`${ORIGIN}/v1/join`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code, space: space_id, name: "phone", platform: "ios" }),
    });
    expect(joinRes.status).toBe(200);
    const joined = (await joinRes.json()) as { role: string; space_id: string };
    expect(joined.role).toBe("viewer");
    expect(joined.space_id).toBe(spaceId);
  });

  it("POST /v1/join: missing space -> 400; malformed code -> 400; code/space mismatch -> 400; unknown secret -> 404", async () => {
    const { ownerToken, spaceId } = await bootstrapSpace();
    const inviteRes = await SELF.fetch(`${ORIGIN}/v1/invites`, {
      method: "POST",
      headers: { authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ role: "viewer" }),
    });
    const { code } = (await inviteRes.json()) as { code: string };

    const join = (body: unknown) =>
      SELF.fetch(`${ORIGIN}/v1/join`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

    // `space` required.
    const noSpace = await join({ code, name: "d", platform: "test" });
    expect(noSpace.status).toBe(400);

    // Malformed code shape (wrong length/anchors/alphabet).
    const malformed = await join({ code: "not-a-real-code", space: spaceId });
    expect(malformed.status).toBe(400);
    expect(await malformed.json()).toEqual({ error: "malformed code" });

    // A well-formed but structurally different space's code pasted against
    // this space (routed space != code's embedded space_id).
    const otherSpaceRes = await SELF.fetch(`${ORIGIN}/v1/spaces`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ platform: "test", name: "other" }) });
    const { space_id: otherSpaceId } = (await otherSpaceRes.json()) as { space_id: string };
    const mismatch = await join({ code, space: otherSpaceId });
    expect(mismatch.status).toBe(400);
    expect(await mismatch.json()).toEqual({ error: "code does not match space" });

    // Well-formed code, correct space, but an unknown/never-issued secret.
    const unknownSecretCode = "X" + spaceId.toUpperCase() + "23456789Z" + "Z";
    expect(unknownSecretCode.length).toBe(21);
    const unknown = await join({ code: unknownSecretCode, space: spaceId });
    expect(unknown.status).toBe(404);
    expect(await unknown.json()).toEqual({ error: "invite invalid or expired" });

    // The real code still works (nothing above consumed it).
    const ok = await join({ code, space: spaceId, name: "d", platform: "test" });
    expect(ok.status).toBe(200);
  });

  it("POST /v1/join with a `space` that names no real, ever-initialized SpaceHub is a clean 400, not a 500", async () => {
    // env.SPACE_HUB.getByName(space) happily resolves a DO stub for ANY
    // name — including one no POST /v1/spaces ever created. A code routed
    // there must fail cleanly (its embedded space_id can never match an
    // uninitialized space's — which has none), not throw.
    const res = await SELF.fetch(`${ORIGIN}/v1/join`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code: "X23456789AB23456789AZ", space: "neverexisted01" }),
    });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "code does not match space" });
  });

  it("PUT /v1/devices/self/push-tokens targets only the authenticated caller (no device_id in body)", async () => {
    const { viewer } = await bootstrapSpace();
    const missing = await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(missing.status).toBe(400);

    const ok = await SELF.fetch(`${ORIGIN}/v1/devices/self/push-tokens`, {
      method: "PUT",
      headers: { authorization: `Bearer ${viewer.token}`, "content-type": "application/json" },
      body: JSON.stringify({ push_to_start_token: "a".repeat(64) }),
    });
    expect(ok.status).toBe(200);
    expect(await ok.json()).toEqual({ ok: true });
  });
});
