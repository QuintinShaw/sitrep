// Type-only ambient declaration for `cloudflare:test` in this directory.
// Not part of the root `tsc --noEmit` project (see tsconfig.json's
// "exclude") — vitest itself transpiles without type-checking, so this
// file exists purely for editor ergonomics when working on these tests.
declare module "cloudflare:test" {
  interface ProvidedEnv extends Env {}
}
