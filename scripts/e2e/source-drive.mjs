#!/usr/bin/env node
// Companion driver for viewer-smoke.mjs: connects as a "source" device and,
// after a short delay (so the viewer script has time to subscribe/resume
// first), emits one task.event (to produce a live delta) and one
// metric.frame (to produce a metric.frame broadcast), then disconnects.
//
// This is a protocol-generic driver used only to exercise the server's
// viewer-side fan-out in scripts/e2e/viewer-smoke.mjs; the daemon (Go)
// source role itself is validated independently and more thoroughly by
// daemon/internal/realtime/client/e2e_test.go (TestE2ESourceLifecycle),
// which drives the real daemon client package against the same server.
//
// Required env: SITREP_E2E_URL, SITREP_E2E_SOURCE_TOKEN, SITREP_E2E_SOURCE_DEVICE_ID.

import WebSocket from "ws";

const URL = requireEnv("SITREP_E2E_URL");
const TOKEN = requireEnv("SITREP_E2E_SOURCE_TOKEN");
const DEVICE_ID = requireEnv("SITREP_E2E_SOURCE_DEVICE_ID");
const DELAY_MS = Number(process.env.SITREP_E2E_SOURCE_DELAY_MS || 4000);

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`missing required env var ${name}`);
    process.exit(2);
  }
  return v;
}

function log(msg, extra) {
  console.log(extra !== undefined ? `[source] ${msg} ${JSON.stringify(extra)}` : `[source] ${msg}`);
}

let seq = 0;
function envelope(type, body) {
  // envelope_id must match ^[A-Za-z0-9_-]{1,64}$ (proto/realtime/common.schema.json)
  // so the message `type` (which may contain a literal "." e.g. task.event,
  // metric.frame) must NOT be embedded verbatim.
  const safeType = type.replace(/[^A-Za-z0-9]/g, "");
  return { type, id: `e2e-src-${safeType}-${++seq}-${Date.now()}`, ts: Date.now(), body };
}

const ws = new WebSocket(URL, { headers: { Authorization: `Bearer ${TOKEN}` } });
let deviceSeq = 1;

ws.on("open", () => {
  const offer = envelope("hello", {
    stage: "offer",
    device_id: DEVICE_ID,
    role: "source",
    protocol_versions: [1],
  });
  log("send hello offer");
  ws.send(JSON.stringify(offer));
});

ws.on("message", async (data) => {
  const text = data.toString();
  if (text === "ping") {
    ws.send("pong");
    return;
  }
  if (text === "pong") return;
  const env = JSON.parse(text);
  log(`recv ${env.type}`, env.type === "hello" ? env.body : env.type === "ack" ? env.body : undefined);

  if (env.type === "hello" && env.body.stage === "accept") {
    log(`hello accepted, waiting ${DELAY_MS}ms before driving events (letting viewers subscribe first)`);
    await new Promise((r) => setTimeout(r, DELAY_MS));

    const taskEvent = envelope("task.event", {
      device_id: DEVICE_ID,
      device_seq: deviceSeq++,
      task_id: "e2e-viewer-drive-task",
      kind: "started",
      occurred_at: Date.now(),
      title: "e2e viewer-smoke drive",
    });
    log("send task.event (should trigger a live delta to subscribed viewers)");
    ws.send(JSON.stringify(taskEvent));

    await new Promise((r) => setTimeout(r, 500));

    const metricFrame = envelope("metric.frame", {
      device_id: DEVICE_ID,
      metrics: [{ metric_id: "e2e.cpu.load", value: "0.42", ts: Date.now() }],
    });
    log("send metric.frame (should broadcast to metric-subscribed viewers)");
    ws.send(JSON.stringify(metricFrame));

    await new Promise((r) => setTimeout(r, 1000));
    ws.close(1000, "e2e source drive done");
    log("done, closing");
  }

  if (env.type === "error") {
    console.error("[source] server error:", env.body);
  }
});

ws.on("error", (e) => {
  console.error("[source] ws error", e);
  process.exit(1);
});
