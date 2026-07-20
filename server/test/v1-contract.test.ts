import assert from "node:assert/strict";
import test from "node:test";
import {
  assertOwnerIsSuperset,
  CONNECT_CODE_LENGTH,
  CONNECT_CODE_RE,
  decodeConnectCode,
  isRouteAllowed,
  parseTransportFlag,
  TOKEN_RE,
} from "../src/v1/contract/types.ts";

test("parseTransportFlag: only true/'true'/'1' enable, everything else disables (v1-architecture.md §8.3)", () => {
  assert.equal(parseTransportFlag(true), true);
  assert.equal(parseTransportFlag("true"), true);
  assert.equal(parseTransportFlag("True"), true);
  assert.equal(parseTransportFlag(" 1 "), true);
  assert.equal(parseTransportFlag(false), false);
  assert.equal(parseTransportFlag("false"), false);
  assert.equal(parseTransportFlag("0"), false);
  assert.equal(parseTransportFlag(""), false);
  assert.equal(parseTransportFlag(undefined), false);
  assert.equal(parseTransportFlag("yes"), false);
});

test("TOKEN_RE: sr1 grammar only, never st2 (v1-architecture.md §10.1/§10.3)", () => {
  assert.ok(TOKEN_RE.test(`sr1_abc123_${"a".repeat(48)}`));
  assert.ok(!TOKEN_RE.test(`st2_abc123_${"a".repeat(48)}`));
  assert.ok(!TOKEN_RE.test(`sr1_abc123_${"a".repeat(47)}`)); // wrong secret length
  assert.ok(!TOKEN_RE.test(`sr1_ABC123_${"a".repeat(48)}`)); // space_id must be lowercase
});

test("isRouteAllowed matches the §3 role matrix; owner is a strict source+viewer superset (P0-1)", () => {
  // GET /v1/automations: source + owner, NOT viewer (asymmetric row preserved).
  assert.equal(isRouteAllowed("GET /v1/automations", "source"), true);
  assert.equal(isRouteAllowed("GET /v1/automations", "owner"), true);
  assert.equal(isRouteAllowed("GET /v1/automations", "viewer"), false);

  // POST /v1/automations: owner-only among the three (source+viewer both false).
  assert.equal(isRouteAllowed("POST /v1/automations", "owner"), true);
  assert.equal(isRouteAllowed("POST /v1/automations", "viewer"), false);
  assert.equal(isRouteAllowed("POST /v1/automations", "source"), false);

  // PATCH/DELETE/run: viewer + owner, not source.
  for (const route of ["PATCH /v1/automations/:id", "DELETE /v1/automations/:id", "POST /v1/automations/:id/run"] as const) {
    assert.equal(isRouteAllowed(route, "viewer"), true);
    assert.equal(isRouteAllowed(route, "owner"), true);
    assert.equal(isRouteAllowed(route, "source"), false);
  }

  // Source-only uplinks are now ALSO allowed for owner (P0-1 superset).
  for (const route of ["POST /v1/events", "POST /v1/tasks/:id/log"] as const) {
    assert.equal(isRouteAllowed(route, "source"), true);
    assert.equal(isRouteAllowed(route, "viewer"), false);
    assert.equal(isRouteAllowed(route, "owner"), true);
  }
});

test("assertOwnerIsSuperset passes for the frozen ROUTE_ROLES table (P0-1 invariant)", () => {
  assert.doesNotThrow(() => assertOwnerIsSuperset());
});

test("decodeConnectCode: self-routing layout round-trips (v1-architecture.md §10.5, P0-6)", () => {
  // Matches the fixture in docs/api/v1/fixtures/join.json /invites.json.
  const code = "XK7M3QZX2VTQ7T9M2K4NZ";
  assert.equal(code.length, CONNECT_CODE_LENGTH);
  assert.ok(CONNECT_CODE_RE.test(code));
  const decoded = decodeConnectCode(code);
  assert.deepEqual(decoded, { space_id: "k7m3qzx2vt", secret: "q7t9m2k4n" });

  // Case-insensitive / whitespace-tolerant on the way in; canonical output
  // is always lowercase.
  assert.deepEqual(decodeConnectCode(` ${code.toLowerCase()} `), { space_id: "k7m3qzx2vt", secret: "q7t9m2k4n" });

  // Anchors are positional, not exclusive — X/Z may recur inside [1..19].
  const withInteriorAnchors = "X23456789XZJKMNPQRZTZ";
  assert.equal(withInteriorAnchors.length, CONNECT_CODE_LENGTH);
  assert.ok(decodeConnectCode(withInteriorAnchors) !== null);
});

test("decodeConnectCode: malformed shapes are rejected (400 malformed code path)", () => {
  assert.equal(decodeConnectCode("not-a-real-code"), null); // wrong length/alphabet
  assert.equal(decodeConnectCode("YK7M3QZX2VTQ7T9M2K4NZ"), null); // wrong start anchor
  assert.equal(decodeConnectCode("XK7M3QZX2VTQ7T9M2K4NY"), null); // wrong end anchor
  assert.equal(decodeConnectCode("XK7M3QZX2VTQ7T9M2K4N"), null); // too short (20 chars)
  assert.equal(decodeConnectCode("XK7M3QZX2VTQ7T9M2K4N0Z"), null); // 22 chars
  assert.equal(decodeConnectCode("X0000000000000000000Z"), null); // '0' not in the 31-symbol alphabet
});
