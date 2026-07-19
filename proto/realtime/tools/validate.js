#!/usr/bin/env node
// Self-contained conformance checker for proto/realtime.
//
// What it does:
//   1. Loads common.schema.json, envelope.schema.json, and every
//      messages/*.schema.json into a single Ajv (draft 2020-12) instance.
//   2. Every file under fixtures/valid/**/*.json (including scenario
//      subdirectories under fixtures/scenarios/) MUST validate against the
//      generic envelope schema AND against the specific messages/<type>.schema.json
//      selected by its own `type` field. Any failure is a script failure.
//   3. Every file under fixtures/invalid/**/*.json MUST be rejected, either
//      by one of those two schemas or by the client-authorization matrix
//      (SPEC.md section 10.1). A fixture that passes every check is a script
//      failure (it means the fixture no longer demonstrates the constraint
//      its filename claims to).
//
// Fixture forms:
//   - A bare envelope object: schema-checked only.
//   - A wrapper {"sender_role": "source"|"viewer", "frame": {...}}: the
//     frame is schema-checked AND checked against the authorization matrix
//     as if a client with that role had sent it. This form exists for
//     constraints that are role-dependent rather than shape-dependent
//     (e.g. a viewer sending a command with origin "server", which is a
//     perfectly well-formed frame when the server sends it).
//
// Usage:
//   npm install   (once, to materialize node_modules/ from package-lock.json)
//   npm run validate
// or
//   node validate.js
//
// Exits 0 with a summary on success, non-zero on any unexpected result.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import Ajv2020 from "ajv/dist/2020.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const realtimeRoot = path.resolve(here, "..");
const messagesDir = path.join(realtimeRoot, "messages");
const fixturesDir = path.join(realtimeRoot, "fixtures");

const ajv = new Ajv2020({
  allErrors: true,
  strict: true,
  // anyOf branches like ack's `{"required": ["acked"]}` intentionally test
  // for a property's presence without redeclaring its full shape (that shape
  // already lives in the sibling `properties` at the same schema level) -
  // this is standard practice, not a schema-authoring mistake, so it is
  // exempted from strict mode's usually-helpful "did you forget the shape?"
  // check. Every other strict-mode check stays on.
  strictRequired: false,
});

function loadJson(p) {
  return JSON.parse(readFileSync(p, "utf8"));
}

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = path.join(dir, entry);
    const st = statSync(full);
    if (st.isDirectory()) out.push(...walk(full));
    else if (entry.endsWith(".json")) out.push(full);
  }
  return out;
}

// --- 1. load schemas ---------------------------------------------------

ajv.addSchema(loadJson(path.join(realtimeRoot, "common.schema.json")));
ajv.addSchema(loadJson(path.join(realtimeRoot, "envelope.schema.json")));

const messageSchemaIds = new Map(); // envelope `type` value -> $id
for (const file of readdirSync(messagesDir).filter((f) => f.endsWith(".schema.json"))) {
  const schema = loadJson(path.join(messagesDir, file));
  ajv.addSchema(schema);
  // messages/<type>.schema.json <-> envelope type of the same dotted name
  const type = file.replace(/\.schema\.json$/, "");
  messageSchemaIds.set(type, schema.$id);
}

const envelopeSchema = ajv.getSchema("https://schema.sitrep.dev/realtime/v1/envelope.schema.json");

// Client-side authorization matrix, mirroring SPEC.md section 10.1: the set
// of message types each client role may SEND. snapshot/delta/config.event
// are server-only in the client-send direction.
const CLIENT_SEND_ALLOWED = {
  source: new Set(["hello", "ack", "task.event", "message.event", "metric.frame", "error"]),
  viewer: new Set(["hello", "resume", "subscribe", "unsubscribe", "interest.renew", "command", "error"]),
};

function authorizationErrors(role, frame) {
  const errors = [];
  const allowed = CLIENT_SEND_ALLOWED[role];
  if (!allowed) {
    errors.push(`[auth] unknown sender_role: ${JSON.stringify(role)}`);
    return errors;
  }
  if (!allowed.has(frame?.type)) {
    errors.push(`[auth] role "${role}" may not send ${JSON.stringify(frame?.type)}`);
  }
  if (frame?.type === "command" && frame?.body?.origin !== "viewer") {
    errors.push(`[auth] a client may only send command with origin "viewer" (got ${JSON.stringify(frame?.body?.origin)})`);
  }
  return errors;
}

function validateFrame(frame) {
  const errors = [];

  const envelopeOk = envelopeSchema(frame);
  if (!envelopeOk) {
    errors.push(...(envelopeSchema.errors ?? []).map((e) => `[envelope] ${e.instancePath} ${e.message}`));
  }

  const type = frame && typeof frame === "object" ? frame.type : undefined;
  const schemaId = messageSchemaIds.get(type);
  if (!schemaId) {
    errors.push(`[type] unknown or missing message type: ${JSON.stringify(type)}`);
    return errors;
  }

  const specific = ajv.getSchema(schemaId);
  const specificOk = specific(frame);
  if (!specificOk) {
    errors.push(...(specific.errors ?? []).map((e) => `[${type}] ${e.instancePath} ${e.message}`));
  }

  return errors;
}

// --- 2 & 3. check fixtures ----------------------------------------------

let failures = 0;
let checked = 0;

function relative(p) {
  return path.relative(realtimeRoot, p);
}

const validDir = path.join(fixturesDir, "valid");
const scenariosDir = path.join(fixturesDir, "scenarios");
const invalidDir = path.join(fixturesDir, "invalid");

function unwrap(doc) {
  if (doc && typeof doc === "object" && "frame" in doc && "sender_role" in doc) {
    return { frame: doc.frame, senderRole: doc.sender_role };
  }
  return { frame: doc, senderRole: null };
}

function allErrors(doc) {
  const { frame, senderRole } = unwrap(doc);
  const errors = validateFrame(frame);
  if (senderRole !== null) errors.push(...authorizationErrors(senderRole, frame));
  return errors;
}

for (const file of [...walk(validDir), ...walk(scenariosDir)]) {
  checked++;
  const errors = allErrors(loadJson(file));
  if (errors.length > 0) {
    failures++;
    console.error(`FAIL (expected valid): ${relative(file)}`);
    for (const e of errors) console.error(`    ${e}`);
  } else {
    console.log(`ok       ${relative(file)}`);
  }
}

for (const file of walk(invalidDir)) {
  checked++;
  const errors = allErrors(loadJson(file));
  if (errors.length === 0) {
    failures++;
    console.error(`FAIL (expected invalid, but it validated cleanly): ${relative(file)}`);
  } else {
    console.log(`ok       ${relative(file)}  (rejected as expected: ${errors[0]})`);
  }
}

console.log("");
console.log(`${checked} fixture(s) checked, ${failures} unexpected result(s).`);
if (failures > 0) {
  process.exit(1);
}
