// Node/Docker entry: `npm run dev:node`. State persists in SQLite at
// SITREP_DB (default ./sitrep.db); auth: set SITREP_TOKEN to require a
// bearer token; unset = open (local dev). Pairing/push endpoints are
// Workers-only in v0 (this path is admin-token single-user).
import { serve } from "@hono/node-server";
import { createApp } from "../app.ts";
import { SqliteStore } from "../sqlite-store.ts";

const store = new SqliteStore(process.env.SITREP_DB || "./sitrep.db");
const token = process.env.SITREP_TOKEN;
const app = createApp({ store: () => store, authToken: () => token });
const port = Number(process.env.PORT || 8787);

serve({ fetch: app.fetch, port });
console.log(`sitrep server listening on :${port}${token ? " (auth enabled)" : ""}`);
