// Required coverage #7: an invalid token gets 401 at the Worker layer
// without ever instantiating a SpaceHub Durable Object, so a reconnect
// storm of bad/garbage tokens cannot spin up empty spaces.
import { env, listDurableObjectIds } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { bootstrapSpace, upgrade } from "./helpers";

describe("token gate", () => {
  it("rejects a malformed token with 401 and creates no SpaceHub", async () => {
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
    const res = await upgrade("not-a-real-token");
    expect(res.status).toBe(401);
    expect(res.webSocket).toBeNull();
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
  });

  it("rejects a missing Authorization header with 401 and creates no SpaceHub", async () => {
    const res = await upgrade(null);
    expect(res.status).toBe(401);
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
  });

  it("rejects a well-formed but unresolvable st2 token with 401 and still creates no SpaceHub", async () => {
    // Right shape (matches TOKEN_RE), but no device was ever issued this
    // secret — resolveToken() legitimately touches UserStore (pre-existing
    // behavior), but must never reach SpaceHub.
    const res = await upgrade("st2_zzzzzzzzzz_" + "0".repeat(48));
    expect(res.status).toBe(401);
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
  });

  it("a reconnect storm of invalid tokens across many spaces never creates a single SpaceHub", async () => {
    const attempts = Array.from({ length: 25 }, (_, i) => upgrade(`st2_space${i}_` + "a".repeat(48)));
    const results = await Promise.all(attempts);
    for (const res of results) expect(res.status).toBe(401);
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(0);
  });

  it("a valid token succeeds and creates exactly one SpaceHub for that space", async () => {
    const { source } = await bootstrapSpace();
    const res = await upgrade(source.token);
    expect(res.status).toBe(101);
    expect(res.webSocket).not.toBeNull();
    res.webSocket?.accept();
    res.webSocket?.close();
    expect((await listDurableObjectIds(env.SPACE_HUB)).length).toBe(1);
  });
});
