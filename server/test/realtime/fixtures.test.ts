// Fixture round-trip test (required coverage item #11): every fixture under
// proto/realtime/fixtures/valid/ and fixtures/scenarios/**/ must decode,
// re-encode, and decode again to the same value; every fixture under
// fixtures/invalid/ must be rejected by our hand-written guards — either by
// parseEnvelope (schema-shape rejection) or by authorizeClientEnvelope (the
// two role-wrapped fixtures that are only invalid given a sender role).
//
// This is a pure-logic test (no Workers runtime needed), so it runs under
// the plain Node test runner alongside the rest of test:unit.
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { authorizeClientEnvelope, encodeEnvelope, parseEnvelope } from "../../src/realtime/guards.ts";
import type { AnyEnvelope, DeviceRole } from "../../src/realtime/types.ts";

const FIXTURES_ROOT = join(import.meta.dirname, "..", "..", "..", "proto", "realtime", "fixtures");

function listJsonFilesRecursive(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) out.push(...listJsonFilesRecursive(full));
    else if (name.endsWith(".json")) out.push(full);
  }
  return out;
}

const validFiles = [
  ...listJsonFilesRecursive(join(FIXTURES_ROOT, "valid")),
  ...listJsonFilesRecursive(join(FIXTURES_ROOT, "scenarios")),
];
const invalidFiles = listJsonFilesRecursive(join(FIXTURES_ROOT, "invalid"));

test(`found valid fixtures (${validFiles.length}) and invalid fixtures (${invalidFiles.length})`, () => {
  assert.ok(validFiles.length >= 20, "expected at least the 21 valid+scenario fixtures present at freeze time");
  assert.ok(invalidFiles.length >= 14, "expected at least the 15 invalid fixtures present at freeze time");
});

for (const file of validFiles) {
  test(`valid fixture decodes and round-trips: ${file.slice(FIXTURES_ROOT.length + 1)}`, () => {
    const raw = readFileSync(file, "utf8");
    const first = parseEnvelope(raw);
    assert.equal(first.kind, "ok", `expected ok, got ${JSON.stringify(first)}`);
    if (first.kind !== "ok") return;
    const reEncoded = encodeEnvelope(first.envelope);
    const second = parseEnvelope(reEncoded);
    assert.equal(second.kind, "ok");
    if (second.kind !== "ok") return;
    assert.deepEqual(second.envelope, first.envelope, "round-trip must be lossless");
  });
}

for (const file of invalidFiles) {
  test(`invalid fixture is rejected: ${file.slice(FIXTURES_ROOT.length + 1)}`, () => {
    const raw = JSON.parse(readFileSync(file, "utf8")) as unknown;
    const wrapper = raw as { sender_role?: DeviceRole; frame?: AnyEnvelope };
    if (wrapper.sender_role && wrapper.frame) {
      // Role-dependent fixture (proto/realtime/SPEC.md section 16): valid
      // shape, but only invalid when sent by the given role/authorization
      // rule. Body should parse fine; authorization must reject it.
      const parsed = parseEnvelope(JSON.stringify(wrapper.frame));
      assert.equal(parsed.kind, "ok", "wrapped fixture body should be schema-valid on its own");
      if (parsed.kind !== "ok") return;
      const authz = authorizeClientEnvelope(wrapper.sender_role, parsed.envelope);
      assert.equal(authz.ok, false, "expected authorization to reject this role/frame combination");
    } else {
      const parsed = parseEnvelope(JSON.stringify(raw));
      assert.notEqual(parsed.kind, "ok", "expected this fixture to be rejected");
    }
  });
}
