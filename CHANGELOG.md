# Changelog

## Unreleased

- Export `runtime.ToolCallStream` so downstream event sinks can construct and test tool-call stream observations through the public API.

## v2.1.0 - 2026-07-29

- Complete the identity-bound `ThreadReader` surface for canonical overview,
  turn pages, detail events, pending work, approvals, todos, context,
  projections, and artifacts.
- Complete active `TurnRunner` control for retries, pending work, approvals,
  and Agent todos, plus parent-bound SubAgent turn, artifact, and pending-work
  reads.
- Keep every new root-bound request free of a caller-supplied `ThreadID`; the
  issuing handle injects its immutable authority identity.

## v2.0.0 - 2026-07-29

- **Breaking:** Move the module to `github.com/floegence/floret/v2` and remove
  every v1 bootstrap, binder, factory, Host-options, Store facade, host
  generator, alias, and fake-provider configuration contract.
- Add composition-root-owned `runtime.Host`, immutable `runtime.Agent`, and
  identity-bound thread, turn, SubAgent, inventory, and recovery handles.
- Make `provider.Gateway` the only model execution path and expose official
  OpenAI-compatible and Anthropic adapters through the same public contract.
- Add the third-party `storage.Source` and `storage.Backend` SPI, a shared
  domain kernel for memory and SQLite, and `florettest.RunBackendContract`.
- Add the explicit atomic `floret-store migrate-v2` path for exact v1
  schema-v16. Runtime startup never migrates or dual-reads legacy state.
- Freeze the reviewed v2 `go/types` surface and validate candidate and
  published releases from blank modules without workspace, replacement, or
  sibling repository wiring.

## v1.0.0 - 2026-07-29

- Freeze `config`, `runtime`, `tools`, and `observation` as the production
  package surface, with `florettest` reserved for downstream tests.
- Remove v0 runtime aliases for reasoning and lifecycle reasons; callers use
  the authoritative `config` and `observation` types directly.
- Make thread title status and source typed while preserving their JSON field
  names and values and the existing SQLite schema-v16 data contract.
- Require scoped, validated constructors for Turn, thread compaction, and
  SubAgent provider-backed Host options.
- Add root DTO validation, stable error classification guidance, a generated
  `go/types` API baseline, fixed-version `apidiff`, and packaged downstream
  adoption gates.
- Preserve SQLite v3-v15 migration into schema v16 and
  `ThreadTurnFailureLegacyUnclassified` as historical durable facts.
