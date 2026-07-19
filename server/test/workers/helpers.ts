// Shared helpers for the vitest-pool-workers realtime test suite. Not a
// test file itself (doesn't match vitest.config.ts's `*.workers.ts`
// include... actually it does match the extension but has no test()/
// describe() calls, so it just runs as an empty, harmless module if vitest
// ever loads it directly).
import { SELF } from "cloudflare:test";

const ORIGIN = "https://example.com";

export interface JoinedDevice {
  token: string;
  device_id: string;
  role: string;
}

export interface Bootstrapped {
  spaceId: string;
  ownerToken: string;
  source: JoinedDevice;
  viewer: JoinedDevice;
  inviteAndJoin: (role: "source" | "viewer") => Promise<JoinedDevice>;
}

/** Creates a fresh space (via the existing /v2 pairing flow) with one
 * source device and one viewer device already joined, plus a helper to
 * mint more devices of either role. */
export async function bootstrapSpace(): Promise<Bootstrapped> {
  const createRes = await SELF.fetch(`${ORIGIN}/v2/spaces`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ platform: "test", name: "owner-mac" }),
  });
  if (createRes.status !== 200) throw new Error(`space creation failed: ${createRes.status} ${await createRes.text()}`);
  const { space_id, owner_token } = (await createRes.json()) as { space_id: string; owner_token: string };

  const inviteAndJoin = async (role: "source" | "viewer"): Promise<JoinedDevice> => {
    const inviteRes = await SELF.fetch(`${ORIGIN}/v2/invites`, {
      method: "POST",
      headers: { authorization: `Bearer ${owner_token}`, "content-type": "application/json" },
      body: JSON.stringify({ role }),
    });
    if (inviteRes.status !== 200) throw new Error(`invite failed: ${inviteRes.status} ${await inviteRes.text()}`);
    const { code } = (await inviteRes.json()) as { code: string };
    const joinRes = await SELF.fetch(`${ORIGIN}/v2/join`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code, space: space_id, name: `${role}-device`, platform: "test" }),
    });
    if (joinRes.status !== 200) throw new Error(`join failed: ${joinRes.status} ${await joinRes.text()}`);
    return (await joinRes.json()) as JoinedDevice;
  };

  const source = await inviteAndJoin("source");
  const viewer = await inviteAndJoin("viewer");
  return { spaceId: space_id, ownerToken: owner_token, source, viewer, inviteAndJoin };
}

/** Opens a /v3/realtime WebSocket authenticated with the given token.
 * Returns the raw upgrade Response so callers can assert on failed
 * upgrades (e.g. invalid token -> 401, no `webSocket`). */
export async function upgrade(token: string | null): Promise<Response> {
  const headers = new Headers({ upgrade: "websocket" });
  if (token !== null) headers.set("authorization", `Bearer ${token}`);
  return SELF.fetch(`${ORIGIN}/v3/realtime`, { headers });
}

/** Thin promise-based wrapper around a Workers-runtime client WebSocket:
 * queues incoming text frames so `recv()` can await the next one in order,
 * exactly as a real client would process them. */
export class WsClient {
  readonly ws: WebSocket;
  private queue: string[] = [];
  private waiters: Array<(v: string) => void> = [];
  private closed: { code: number; reason: string } | null = null;
  private closeWaiters: Array<(v: { code: number; reason: string }) => void> = [];

  constructor(ws: WebSocket) {
    this.ws = ws;
    ws.accept();
    ws.addEventListener("message", (evt: MessageEvent) => {
      const data = typeof evt.data === "string" ? evt.data : new TextDecoder().decode(evt.data as ArrayBuffer);
      const waiter = this.waiters.shift();
      if (waiter) waiter(data);
      else this.queue.push(data);
    });
    ws.addEventListener("close", (evt: CloseEvent) => {
      this.closed = { code: evt.code, reason: evt.reason };
      for (const w of this.closeWaiters.splice(0)) w(this.closed);
    });
  }

  send(envelope: unknown): void {
    this.ws.send(JSON.stringify(envelope));
  }

  sendRaw(text: string): void {
    this.ws.send(text);
  }

  recvRaw(timeoutMs = 3000): Promise<string> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
      const waiter = (v: string) => {
        clearTimeout(timer);
        resolve(v);
      };
      const timer = setTimeout(() => {
        // Critical: drop this waiter on timeout, or a message that arrives
        // later (e.g. right after an expectSilence() timeout) would be
        // handed to this already-settled promise and silently vanish
        // instead of reaching the next real recv()/expectSilence() call.
        const idx = this.waiters.indexOf(waiter);
        if (idx !== -1) this.waiters.splice(idx, 1);
        reject(new Error(`timed out waiting for a message after ${timeoutMs}ms`));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  async recv(timeoutMs = 3000): Promise<any> {
    return JSON.parse(await this.recvRaw(timeoutMs));
  }

  /** Resolves true if no message arrives within timeoutMs (used to assert
   * silence — e.g. no live delta before a resume reply). */
  async expectSilence(timeoutMs = 200): Promise<boolean> {
    try {
      const msg = await this.recvRaw(timeoutMs);
      throw new Error(`expected silence but received: ${msg}`);
    } catch (err) {
      if (err instanceof Error && err.message.startsWith("timed out")) return true;
      throw err;
    }
  }

  waitForClose(timeoutMs = 3000): Promise<{ code: number; reason: string }> {
    if (this.closed) return Promise.resolve(this.closed);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out waiting for close")), timeoutMs);
      this.closeWaiters.push((v) => {
        clearTimeout(timer);
        resolve(v);
      });
    });
  }

  close(): void {
    try {
      this.ws.close();
    } catch {
      // already closed
    }
  }
}

export async function connect(token: string): Promise<WsClient> {
  const res = await upgrade(token);
  if (res.status !== 101 || !res.webSocket) {
    throw new Error(`expected a 101 upgrade, got ${res.status}: ${await res.text().catch(() => "")}`);
  }
  return new WsClient(res.webSocket);
}

let idCounter = 0;
export function nextId(): string {
  idCounter += 1;
  return `t${idCounter}`;
}

export async function helloOffer(client: WsClient, deviceId: string, role: "source" | "viewer"): Promise<any> {
  client.send({
    type: "hello",
    id: nextId(),
    ts: Date.now(),
    body: { stage: "offer", device_id: deviceId, role, protocol_versions: [1] },
  });
  return client.recv();
}

export async function subscribe(client: WsClient, topics?: Array<"task" | "metric" | "message">): Promise<any> {
  client.send({ type: "subscribe", id: nextId(), ts: Date.now(), body: topics ? { topics } : {} });
  return client.recv();
}

export async function unsubscribe(client: WsClient): Promise<any> {
  client.send({ type: "unsubscribe", id: nextId(), ts: Date.now(), body: {} });
  return client.recv();
}

export async function resume(client: WsClient, lastRevision: number): Promise<any> {
  client.send({ type: "resume", id: nextId(), ts: Date.now(), body: { last_revision: lastRevision } });
  return client.recv();
}

export async function sendTaskEvent(
  client: WsClient,
  deviceId: string,
  deviceSeq: number,
  overrides: Partial<{ task_id: string; kind: string; occurred_at: number; percent: number; title: string }> = {},
): Promise<any> {
  const body = {
    device_id: deviceId,
    device_seq: deviceSeq,
    task_id: overrides.task_id ?? "run-1",
    kind: overrides.kind ?? "started",
    occurred_at: overrides.occurred_at ?? Date.now(),
    ...(overrides.percent !== undefined ? { percent: overrides.percent } : {}),
    ...(overrides.title !== undefined ? { title: overrides.title } : {}),
  };
  client.send({ type: "task.event", id: nextId(), ts: Date.now(), body });
  return client.recv();
}
