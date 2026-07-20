// Pure snapshot/delta chunking helpers — no I/O, unit-testable without a
// Workers runtime. SPEC.md section 6.2/11: every chunk of one snapshot
// shares the same `revision`, `part` numbers consecutively from 1, and the
// last chunk carries `final: true`; a chained catch-up delta series splits
// strictly on event boundaries so `d1.to_revision == d2.from_revision`.

import { CHUNK_SOFT_MAX_BYTES } from "./protocol.ts";
import type {
  AutomationState,
  DeltaBody,
  DeltaEventItem,
  MessageRecord,
  MetricSample,
  SnapshotBody,
  TaskState,
} from "./types.ts";

type SnapshotBuckets = {
  tasks: TaskState[];
  metrics: MetricSample[];
  messages: MessageRecord[];
  automations: AutomationState[];
};

/** Packs the four snapshot arrays into one or more chunks under the frame
 * size budget. Always emits at least one chunk (part 1, final true), even
 * when every array is empty (SPEC.md: "A snapshot MAY be empty"). */
export function chunkSnapshot(revision: number, arrays: SnapshotBuckets): SnapshotBody[] {
  const buckets: Array<{ key: keyof SnapshotBuckets; items: unknown[] }> = [
    { key: "tasks", items: arrays.tasks },
    { key: "metrics", items: arrays.metrics },
    { key: "messages", items: arrays.messages },
    { key: "automations", items: arrays.automations },
  ];
  const chunks: SnapshotBody[] = [];
  const empty = (): SnapshotBuckets => ({ tasks: [], metrics: [], messages: [], automations: [] });
  let cur = empty();
  let curSize = 128; // overhead margin for envelope wrapper + fixed keys
  const flush = (final: boolean) => {
    chunks.push({ revision, part: chunks.length + 1, final, ...cur });
    cur = empty();
    curSize = 128;
  };
  for (const bucket of buckets) {
    for (const item of bucket.items) {
      const size = JSON.stringify(item).length + 2;
      const curCount = cur.tasks.length + cur.metrics.length + cur.messages.length + cur.automations.length;
      if (curSize + size > CHUNK_SOFT_MAX_BYTES && curCount > 0) flush(false);
      (cur[bucket.key] as unknown[]).push(item);
      curSize += size;
    }
  }
  flush(true);
  return chunks;
}

/** Splits a catch-up event range into one or more chained deltas whose
 * from/to boundaries connect exactly. Always emits at least one delta
 * (possibly with an empty `events` array) so a caller can send it as the
 * single resume reply. */
export function chunkDeltaEvents(fromRevision: number, events: DeltaEventItem[]): DeltaBody[] {
  const groups: DeltaEventItem[][] = [];
  let cur: DeltaEventItem[] = [];
  let curSize = 128;
  for (const event of events) {
    const size = JSON.stringify(event).length + 2;
    if (curSize + size > CHUNK_SOFT_MAX_BYTES && cur.length > 0) {
      groups.push(cur);
      cur = [];
      curSize = 128;
    }
    cur.push(event);
    curSize += size;
  }
  groups.push(cur);
  let from = fromRevision;
  const out: DeltaBody[] = [];
  for (const group of groups) {
    const to = from + group.length;
    out.push({ from_revision: from, to_revision: to, events: group });
    from = to;
  }
  return out;
}
