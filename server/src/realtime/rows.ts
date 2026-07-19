// SQLite row shapes for SpaceHub storage, and pure mapping functions to/from
// the wire types in ./types.ts. Kept separate from space-hub.ts so the
// mapping logic is easy to read and (where pure) easy to unit test.

import type { AutomationState, DeltaEventItem, MessageRecord, TaskState } from "./types.ts";

export interface TaskRow extends Record<string, SqlStorageValue> {
  task_id: string;
  device_id: string;
  title: string | null;
  state: string;
  percent: number | null;
  step: string | null;
  message: string | null;
  updated_at: number;
  display: string | null;
}

export function rowToTaskState(row: TaskRow): TaskState {
  return {
    task_id: row.task_id,
    device_id: row.device_id,
    state: row.state as TaskState["state"],
    updated_at: row.updated_at,
    ...(row.title !== null ? { title: row.title } : {}),
    ...(row.percent !== null ? { percent: row.percent } : {}),
    ...(row.step !== null ? { step: row.step } : {}),
    ...(row.message !== null ? { message: row.message } : {}),
    ...(row.display !== null ? { display: JSON.parse(row.display) } : {}),
  };
}

export interface AutomationRow extends Record<string, SqlStorageValue> {
  automation_id: string;
  name: string;
  executor_kind: string;
  every_seconds: number;
  state: string;
  last_run_at: number | null;
}

export function rowToAutomationState(row: AutomationRow): AutomationState {
  return {
    automation_id: row.automation_id,
    name: row.name,
    executor_kind: row.executor_kind as AutomationState["executor_kind"],
    schedule: { kind: "interval", every_seconds: row.every_seconds },
    state: row.state as AutomationState["state"],
    ...(row.last_run_at !== null ? { last_run_at: row.last_run_at } : {}),
  };
}

export interface MessageRow extends Record<string, SqlStorageValue> {
  message_id: string;
  device_id: string;
  level: string;
  text: string;
  occurred_at: number;
  revision: number;
}

export function rowToMessageRecord(row: MessageRow): MessageRecord {
  return {
    message_id: row.message_id,
    device_id: row.device_id,
    level: row.level as MessageRecord["level"],
    text: row.text,
    occurred_at: row.occurred_at,
  };
}

export interface EventLogRow extends Record<string, SqlStorageValue> {
  revision: number;
  event_type: string;
  device_id: string | null;
  device_seq: number | null;
  occurred_at: number;
  payload: string;
}

export function rowToDeltaEventItem(row: EventLogRow): DeltaEventItem {
  return { event_type: row.event_type, event: JSON.parse(row.payload) } as DeltaEventItem;
}
