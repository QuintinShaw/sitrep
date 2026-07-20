import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/workers/**/*.workers.ts"],
  },
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      // wrangler.jsonc's WS_TRANSPORT_ENABLED/APNS_DELIVERY_ENABLED stay
      // "false" as the production kill-switch defaults (flipped per
      // environment at deploy time), but the /v1/realtime + APNs test
      // suites need both on to exercise the handshake/broadcast/reliability
      // and push-outbox paths — override for the test pool only. Tests
      // covering the disabled path flip them back to `false` per-test via
      // `env.WS_TRANSPORT_ENABLED` / `env.APNS_DELIVERY_ENABLED`.
      //
      // APNS_KEY_P8/KEY_ID/TEAM_ID: a throwaway, non-secret P-256 PKCS8 test
      // key (generated once for this suite, never used against real APNs —
      // every outbound call in tests goes through SpaceHub's injectable
      // `apnsFetch` field, never a real `fetch`). It exists only so
      // `apnsJwt()`'s ES256 signing has a structurally valid key to import.
      miniflare: {
        bindings: {
          WS_TRANSPORT_ENABLED: true,
          APNS_DELIVERY_ENABLED: true,
          // Space creation must be ON in the pool so bootstrapSpace works;
          // the "creation blocked when off" test flips it per-test.
          SPACE_CREATION_ENABLED: true,
          // Effectively unlimited in the pool: bootstrapSpace() runs dozens
          // of times across the suite, all sharing one rate-limit bucket
          // (the test harness's SELF.fetch sends no cf-connecting-ip, so
          // every call falls into the same "unknown" key) — a real bound
          // here would spuriously trip well before the suite finishes. The
          // dedicated rate-limit test overrides this back down to a small
          // number for just that test (same per-test override pattern as
          // SPACE_CREATION_ENABLED above).
          SPACE_CREATION_RATE_LIMIT_PER_HOUR: 100_000,
          APNS_KEY_P8:
            "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgICX9WlW045QcUikwNBwR5rUAApkNM3EhbS8nPVvvl0ShRANCAAQHaKb2+B0ZhQBfP3b7YJl2k3GbbSQGAGThGqKM8AjYBdHXWDbITP0EpkXEb/DfUg2ye8t8timBKJ2E6WKCAk15",
          APNS_KEY_ID: "TESTKEY123",
          APNS_TEAM_ID: "TESTTEAM123",
        },
      },
    }),
  ],
});
