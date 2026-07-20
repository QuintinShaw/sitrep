import assert from "node:assert/strict";
import test from "node:test";
import { backoffMs, classifyApnsResponse, MAX_TRANSIENT_ATTEMPTS } from "../src/apns.ts";

test("classifyApnsResponse: 2xx is sent", () => {
  assert.deepEqual(classifyApnsResponse(200, undefined, null), { kind: "sent" });
});

test("classifyApnsResponse: known bad-token reasons are permanent + badToken (v1-apns-outbox.md §4.3)", () => {
  for (const reason of ["BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"]) {
    const outcome = classifyApnsResponse(reason === "Unregistered" ? 410 : 400, reason, null);
    assert.deepEqual(outcome, { kind: "permanent", reason, badToken: true });
  }
});

test("classifyApnsResponse: 429/5xx are transient, honoring Retry-After", () => {
  assert.deepEqual(classifyApnsResponse(429, "TooManyRequests", null), { kind: "transient", reason: "TooManyRequests" });
  assert.deepEqual(classifyApnsResponse(500, undefined, "30"), { kind: "transient", reason: "http_500", retryAfterMs: 30_000 });
  assert.deepEqual(classifyApnsResponse(503, undefined, null), { kind: "transient", reason: "http_503" });
});

test("classifyApnsResponse: an unrecognized 4xx is permanent but not a bad token", () => {
  assert.deepEqual(classifyApnsResponse(400, "BadTopic", null), { kind: "permanent", reason: "BadTopic", badToken: false });
});

test("classifyApnsResponse: a 410 with an unparseable/absent body still cleans up the dead token (fault-injection review)", () => {
  // reason undefined (body didn't parse) but status 410 -> must be badToken.
  assert.deepEqual(classifyApnsResponse(410, undefined, null), { kind: "permanent", reason: "Unregistered", badToken: true });
});

test("backoffMs: exponential with a 300s cap and +/-20% jitter", () => {
  const noJitter = () => 0.5; // rand()=0.5 => jitter factor exactly 1.0
  assert.equal(backoffMs(1, noJitter), 2000);
  assert.equal(backoffMs(2, noJitter), 4000);
  assert.equal(backoffMs(3, noJitter), 8000);
  // Cap: 2^9*1000 = 512_000, well past the 300_000 ceiling.
  assert.equal(backoffMs(9, noJitter), 300_000);
  assert.ok(MAX_TRANSIENT_ATTEMPTS <= 9, "sanity: the retry budget is within range of this cap assertion");

  const maxJitter = () => 1; // jitter factor 1.2
  const minJitter = () => 0; // jitter factor 0.8
  assert.equal(backoffMs(1, maxJitter), Math.round(2000 * 1.2));
  assert.equal(backoffMs(1, minJitter), Math.round(2000 * 0.8));
});
