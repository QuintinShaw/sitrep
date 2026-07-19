import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/workers/**/*.workers.ts"],
  },
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      // wrangler.jsonc's REALTIME_ENABLED stays "false" as the production
      // kill-switch default (flipped per-environment at deploy time), but
      // the /v3/realtime protocol suite needs it on to exercise the
      // handshake/broadcast/reliability paths — override it here for the
      // test pool only. Tests covering the disabled path flip it back to
      // `false` per-test via `env.REALTIME_ENABLED` (see
      // realtime-gate.workers.ts).
      miniflare: { bindings: { REALTIME_ENABLED: true } },
    }),
  ],
});
