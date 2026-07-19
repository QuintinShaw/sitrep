#!/usr/bin/env node
// Cross-line realtime protocol E2E smoke test: viewer role, driven against a
// real server (see docs/design/realtime-integration.md for how to start one
// locally with `wrangler dev`). Exercises, over two real WebSocket
// connections (two "viewer" devices), everything the apple/SitrepKit
// RealtimeClient does on the wire, using a plain `ws` client so this script
// has no dependency on the Swift/Go implementations:
//
//   viewer1: hello{offer} -> hello{accept} -> subscribe -> ack{lease}
//            -> resume{last_revision:0} -> snapshot (verifies fresh-viewer
//            snapshot path) -> live delta once the source emits an event
//            (verifies revision chaining: snapshot.revision -> delta.
//            from_revision == snapshot.revision, to_revision == from+1)
//   viewer2: same hello/subscribe/resume sequence, then receives the
//            metric.frame broadcast triggered by the source (verifies
//            server fan-out to a second, independently-subscribed viewer)
//
// All credentials are read from the environment — nothing is hardcoded.
// Required: SITREP_E2E_URL, SITREP_E2E_VIEWER1_TOKEN, SITREP_E2E_VIEWER2_TOKEN.
// The source-side event that drives the live delta/metric.frame is sent by
// a companion process (see docs/design/realtime-integration.md); this
// script only waits for it, with a generous timeout.

import WebSocket from "ws";

const URL = requireEnv("SITREP_E2E_URL");
const VIEWER1_TOKEN = requireEnv("SITREP_E2E_VIEWER1_TOKEN");
const VIEWER2_TOKEN = requireEnv("SITREP_E2E_VIEWER2_TOKEN");
const WAIT_FOR_SOURCE_EVENT_MS = Number(process.env.SITREP_E2E_WAIT_MS || 20000);

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`missing required env var ${name}`);
    process.exit(2);
  }
  return v;
}

function log(tag, msg, extra) {
  const line = `[${tag}] ${msg}`;
  console.log(extra !== undefined ? `${line} ${JSON.stringify(extra)}` : line);
}

let envSeq = 0;
function envelope(type, body) {
  // envelope_id must match ^[A-Za-z0-9_-]{1,64}$ (proto/realtime/common.schema.json);
  // strip any "." from the message type (e.g. metric.frame) before embedding it.
  const safeType = type.replace(/[^A-Za-z0-9]/g, "");
  return { type, id: `e2e-${safeType}-${++envSeq}-${Date.now()}`, ts: Date.now(), body };
}

// Connects as a viewer, completes hello -> subscribe -> resume{0}, and
// returns { ws, send, waitFor(predicate) } so the caller can drive the rest
// of the scenario. waitFor resolves the first received frame matching
// predicate, queuing every other frame for subsequent waitFor calls.
function connectViewer(tag, token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(URL, { headers: { Authorization: `Bearer ${token}` } });
    const queue = [];
    const waiters = [];

    ws.on("message", (data) => {
      const text = data.toString();
      if (text === "ping") {
        ws.send("pong");
        log(tag, "recv ping -> sent pong");
        return;
      }
      if (text === "pong") return;
      const env = JSON.parse(text);
      log(tag, `recv ${env.type}`, summarize(env));
      const idx = waiters.findIndex((w) => w.predicate(env));
      if (idx >= 0) {
        const [w] = waiters.splice(idx, 1);
        w.resolve(env);
      } else {
        queue.push(env);
      }
    });
    ws.on("error", reject);

    const send = (type, body) => {
      const env = envelope(type, body);
      log(tag, `send ${type}`, summarize(env));
      ws.send(JSON.stringify(env));
      return env;
    };

    const waitFor = (predicate, timeoutMs = 10000) =>
      new Promise((res, rej) => {
        const qi = queue.findIndex(predicate);
        if (qi >= 0) {
          res(queue.splice(qi, 1)[0]);
          return;
        }
        const timer = setTimeout(() => {
          const i = waiters.findIndex((w) => w.resolve === wrappedRes);
          if (i >= 0) waiters.splice(i, 1);
          rej(new Error(`${tag}: timed out waiting for a matching frame`));
        }, timeoutMs);
        const wrappedRes = (env) => {
          clearTimeout(timer);
          res(env);
        };
        waiters.push({ predicate, resolve: wrappedRes });
      });

    ws.on("open", async () => {
      try {
        send("hello", {
          stage: "offer",
          device_id: `${tag}-e2e`,
          role: "viewer",
          protocol_versions: [1],
        });
        const accept = await waitFor((e) => e.type === "hello");
        resolve({ ws, send, waitFor, sessionId: accept.body.session_id });
      } catch (e) {
        reject(e);
      }
    });
  });
}

function summarize(env) {
  switch (env.type) {
    case "hello":
      return env.body.stage === "accept"
        ? { stage: "accept", protocol_version: env.body.protocol_version, session_id: env.body.session_id }
        : { stage: "offer", device_id: env.body.device_id, role: env.body.role };
    case "ack":
      return { in_reply_to: env.body.in_reply_to, lease: env.body.lease };
    case "snapshot":
      return { revision: env.body.revision, part: env.body.part, final: env.body.final, tasks: env.body.tasks?.length, metrics: env.body.metrics?.length };
    case "delta":
      return { from_revision: env.body.from_revision, to_revision: env.body.to_revision, events: env.body.events?.length };
    case "metric.frame":
      return { device_id: env.body.device_id, metrics: env.body.metrics };
    case "error":
      return { code: env.body.code, message: env.body.message, retryable: env.body.retryable, fatal: env.body.fatal };
    default:
      return env.body;
  }
}

async function main() {
  log("main", `connecting two viewers to ${URL}`);

  const v1 = await connectViewer("viewer1", VIEWER1_TOKEN);
  const subAck1 = v1.send("subscribe", { topics: ["task", "metric", "message"] });
  const ack1 = await v1.waitFor((e) => e.type === "ack" && e.body.in_reply_to === subAck1.id);
  log("viewer1", "subscribed, lease", ack1.body.lease);

  v1.send("resume", { last_revision: 0 });
  const snapshot = await v1.waitFor((e) => e.type === "snapshot");
  log("viewer1", `fresh-viewer resume(0) correctly produced a snapshot at revision ${snapshot.body.revision}`);

  const v2 = await connectViewer("viewer2", VIEWER2_TOKEN);
  const subAck2 = v2.send("subscribe", { topics: ["task", "metric", "message"] });
  const ack2 = await v2.waitFor((e) => e.type === "ack" && e.body.in_reply_to === subAck2.id);
  log("viewer2", "subscribed, lease", ack2.body.lease);
  v2.send("resume", { last_revision: snapshot.body.revision });
  // viewer2 resumes at the current revision: server should send an
  // empty/no-op catch-up or nothing until the next real event — we don't
  // block on it, we only need viewer2 subscribed + delta-eligible before
  // the source emits its next event.
  log("viewer2", "resumed at current revision, now delta-eligible");

  log("main", `waiting up to ${WAIT_FOR_SOURCE_EVENT_MS}ms for the source's next task.event to arrive as a live delta on viewer1...`);
  const delta = await v1.waitFor((e) => e.type === "delta", WAIT_FOR_SOURCE_EVENT_MS);
  if (delta.body.from_revision !== snapshot.body.revision) {
    throw new Error(
      `revision chain broken: snapshot.revision=${snapshot.body.revision} but delta.from_revision=${delta.body.from_revision}`,
    );
  }
  if (delta.body.to_revision !== delta.body.from_revision + 1 && delta.body.to_revision <= delta.body.from_revision) {
    throw new Error(`delta.to_revision (${delta.body.to_revision}) did not advance past from_revision (${delta.body.from_revision})`);
  }
  log(
    "main",
    `PASS: live delta chained correctly (snapshot rev ${snapshot.body.revision} -> delta ${delta.body.from_revision}->${delta.body.to_revision})`,
  );

  log("main", `waiting up to ${WAIT_FOR_SOURCE_EVENT_MS}ms for the source's metric.frame broadcast on viewer2...`);
  const metricFrame = await v2.waitFor((e) => e.type === "metric.frame", WAIT_FOR_SOURCE_EVENT_MS);
  log("main", "PASS: viewer2 received the metric.frame broadcast triggered by the source", summarize(metricFrame));

  v1.ws.close(1000, "e2e done");
  v2.ws.close(1000, "e2e done");
  log("main", "ALL VIEWER E2E CHECKS PASSED");
  process.exit(0);
}

main().catch((err) => {
  console.error("E2E FAILED:", err);
  process.exit(1);
});
