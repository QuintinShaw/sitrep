// Required coverage: DO constructors re-run on every hibernation wake
// (SpaceHub's constructor calls migrate()), so migrate() must not
// unconditionally re-issue its 10+ `CREATE TABLE/INDEX IF NOT EXISTS`
// statements every wake — only when the store's schema version is behind
// what this build expects. See the migrate()/schemaVersion() comments in
// src/realtime/space-hub.ts.
import { env, evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { SpaceHub } from "../../src/realtime/space-hub.ts";
import { bootstrapSpace } from "./helpers";

describe("SpaceHub schema migration", () => {
  it("runs the DDL block on a fresh DO (version table missing)", async () => {
    const stub = env.SPACE_HUB.getByName(`fresh-${crypto.randomUUID()}`);
    const ranDdl = await runInDurableObject(stub, async (instance: SpaceHub) => (instance as any).ranMigrationDdl as boolean);
    expect(ranDdl).toBe(true);

    const version = await runInDurableObject(stub, async (_instance: SpaceHub, state) =>
      state.storage.sql.exec<{ version: number }>("SELECT version FROM _schema_migrations WHERE id = 1").toArray()[0]?.version,
    );
    expect(version).toBe(1);

    // The rest of the schema landed too, not just the version table.
    const tableExists = await runInDurableObject(stub, async (_instance: SpaceHub, state) =>
      state.storage.sql
        .exec<{ n: number }>("SELECT COUNT(*) as n FROM sqlite_master WHERE type='table' AND name='event_log'")
        .toArray()[0].n,
    );
    expect(tableExists).toBe(1);
  });

  it("does not re-run the DDL block on a second construct against an already-migrated store", async () => {
    // bootstrapSpace() drives a real construct through the app's /v2
    // routes (registry -> UserStore -> SpaceHub stub), exercising the
    // exact path a hibernation wake takes in production.
    const { spaceId } = await bootstrapSpace();
    const stub = env.SPACE_HUB.getByName(spaceId);

    // bootstrapSpace() only touches UserStore over HTTP — nothing has
    // addressed this space's SpaceHub yet, so it isn't "running" for
    // evictDurableObject's purposes until something does.
    await runInDurableObject(stub, async () => {});

    // Force a fresh DO *instance* (constructor re-runs) while preserving
    // durable SQLite storage — this is precisely what a hibernation wake
    // does, and the primitive vitest-pool-workers provides to simulate it.
    await evictDurableObject(stub);

    const ranDdl = await runInDurableObject(stub, async (instance: SpaceHub) => (instance as any).ranMigrationDdl as boolean);
    expect(ranDdl).toBe(false);

    // Evict and re-check again for good measure: every subsequent wake
    // against an up-to-date store must skip the DDL, not just the first.
    await evictDurableObject(stub);
    const ranDdlAgain = await runInDurableObject(stub, async (instance: SpaceHub) => (instance as any).ranMigrationDdl as boolean);
    expect(ranDdlAgain).toBe(false);
  });
});
