# Changelog

## v3.2.8 - 2026-08-03

- Return each root thread's optional latest turn from `Threads.ListThreads`
  using the same validated projection as `ThreadReader.ReadOverview`.
  Navigation lists can now obtain status, failure, waiting input, and message
  preview authority without reopening every thread.
- Preserve one bounded inventory read and zero complete session-tree domain
  reads per page. Empty threads omit `latest_turn`, and the session-tree domain
  schema remains v4 with the existing automatic v2-to-v3-to-v4 lineage.

## v3.2.7 - 2026-08-03

- Automatically migrate session-tree domain schema v3 to v4 and commit a
  strict root-thread inventory record atomically with the complete domain.
  Startup verifies exact index/domain agreement; missing, drifted, corrupt, or
  future state fails closed without mutation.
- Read `Threads.ListThreads` from the bounded inventory record so large
  artifacts, provider state, and revision history are not decoded for every
  sidebar refresh. Memory and SQLite retain identical snapshots and ordering.
- Make explicit v2.2-to-v3 physical conversion produce the complete current v4
  record set. Its immutable plan, semantic target hash, atomic apply, replay
  validation, and rollback checks now cover the derived inventory as well as
  canonical session, prompt, and fork-operation state. Plans issued by the
  prior `/1` contract remain applicable and replayable after automatic v4
  upgrade; new preflight operations issue the complete `/2` contract.
- Require Go 1.26.5 for builds and upgrade `golang.org/x/sys` to v0.44.0,
  removing GO-2026-5024 from the dependency graph.

## v3.2.6 - 2026-08-03

- Project each bounded `Threads.ListThreads` page from one canonical domain
  snapshot instead of decoding the complete session tree repeatedly for every
  item. Exact thread snapshots and revisions retain the same authority and
  ordering while approval writes are no longer starved by inventory refreshes.

## v3.2.5 - 2026-08-03

- Close approval activity as `timed_out` when a terminal tool error is the last
  durable event, and prefer `canceled` when an interrupted turn later records
  an aborted marker. Historical stop/dispatch races now remain readable.

## v3.2.4 - 2026-08-03

- Persist complete turn-admission authority when a SubAgent input is admitted,
  so interrupted child turns remain recoverable after process restart.
- Automatically migrate exact session-tree domain state v2 to v3 inside the
  startup backend transaction. Active child turns retain their original lease;
  terminal child turns receive a source-bound read-only proof without an
  executable lease.
- Reject ambiguous, drifted, corrupt, and future domain state without mutation,
  and cover Memory and SQLite migration, restart idempotency, preservation, and
  write-failure, cancellation, and panic rollback.

## v3.2.3 - 2026-08-02

- Keep an approved effect callback attached to the turn's renewable lease
  binding. An approval that spans one or more heartbeat renewals now dispatches
  with the current proof instead of failing with stale authority or an
  active-turn conflict.
- Apply the same strict authority-lineage successor rule at atomic approval,
  provider-request, and turn-finalization boundaries while continuing to reject
  different owners, generations, acquisitions, and turns.
- Cover consecutive approval-gated provider steps across lease renewal with
  both memory storage and the production BackendKernel over SQLite.

## v3.2.2 - 2026-08-02

- Keep historical interrupted turns readable when recovery recorded an error
  tool result after approval was requested but before an approval-resolution
  detail event was journaled. Canonical projection now settles that item from
  the failed or aborted terminal turn without rewriting stored history.
- Make the shared backend transaction fence honor an operation context while
  waiting, so approval, cancellation, and shutdown callers can stop promptly
  instead of waiting indefinitely behind another transaction.

## v3.2.1 - 2026-08-02

- Allow canonical approval resolution while an admitted turn is waiting for
  permission. Long-running provider execution no longer owns the global host
  mutation lock; same-thread execution remains serialized and durable backend
  transactions share one short coordination boundary.
- Keep concurrent execution replay idempotent across Memory and SQLite-backed
  hosts: one admission invokes the provider and approved tool exactly once.

## v3.2.0 - 2026-08-01

- Remove the broad compatibility runtime entry points for thread reads, turn
  execution, SubAgent management, lifecycle mutation, and compaction. Hosts now
  integrate through `Thread.Reader`, `Thread.Lifecycle`,
  `Thread.TurnExecutor`, `Thread.Compactor`, and `Thread.SubAgentManager`.
- Remove command-bearing admitted-turn execution compatibility. Restart-safe
  execution now requires Floret-owned admission plans and
  `TurnExecutor.ExecuteAdmission`.
- Fail explicitly with `ErrExecutionPlanUnavailable` when a stored admitted turn
  lacks the execution plan required for replay, instead of silently rebuilding
  provider work from host command data.

## v3.1.1 - 2026-08-01

- Keep host-facing live turn projections valid when a recorder first observes a
  committed mid-turn tool entry without the earlier turn-start marker. Such
  partial live projections now report `running` until a durable terminal marker
  is observed.

## v3.1.0 - 2026-08-01

- Add native `ThreadReader`, `ThreadLifecycle`, `TurnExecutor`,
  `ThreadCompactor`, and `SubAgentManager` capability views so composition roots
  can grant least authority without downstream adapter graphs.
- Add atomic `ThreadReader.Bootstrap` and authoritative projection provenance
  for gap-free initial read models and subscription handoff.
- Persist immutable admission execution plans in Floret and add
  `TurnExecutor.ExecuteAdmission`, which accepts only a receipt and ephemeral
  execution context after restart.
- Make Agent todo limits and single-in-progress semantics canonical in the
  runtime and storage kernel; distinguish validated offline projections from
  authoritative Floret reads.
- Move deterministic identity injection guidance to `florettest.NewIDSource`, add
  an official-provider SQLite example, validate documented runtime symbols, and
  expand blank-module release adoption coverage.
- Deprecate broad v3.0 thread methods, command-bearing admitted execution,
  caller-provided production ID sources, and inconsistent result naming while
  preserving v3 source compatibility for one minor release series.

## v3.0.3 - 2026-08-01

- Add receipt-first two-stage turn admission for hosts that must durably bind
  product coordination before provider execution.
- Add `Turns.AdmitTurn` and `Turns.ExecuteAdmittedTurn`; admission persists the
  canonical user message and lifecycle identities without issuing a provider
  request, while execution consumes the Floret-owned admission receipt.
- Keep `Turns.StartTurn` source-compatible and make same-process
  `AdmitTurnResult.Execute` a convenience over the receipt-first path.
- Extend the public API baseline, behavior contract, README, and OKF runtime
  documentation for downstream release adoption.

## v3.0.2 - 2026-07-30

- Complete the bound v3 host-adoption surface with canonical root-thread and
  direct-child reads, logical-request-backed title mutation, exact-target
  pending-tool recovery, and exact-proof interrupted-turn recovery.
- Keep every new entry point on `Thread`, `Child`, or a one-time authority
  derived from them; no v2 handle, unbound owner identity, local compatibility
  facade, or caller-allocated lifecycle identity is restored.
- Extend the manually reviewed API and behavior baselines, external-package
  tests, README, and OKF knowledge for downstream production adoption.

## v3.0.1 - 2026-07-30

- Restore the v3 bound runtime capabilities required by complete downstream
  adoption: active-turn manual compaction polling, standalone thread
  compaction, and direct-child wait and close operations.
- Keep every restored operation authority-bound: compaction is issued by an
  exact `Thread`, while wait and close are issued by `SubAgents` bound to an
  exact parent. No v2 handle, caller-assigned lifecycle identity, or unbound
  compatibility DTO is reintroduced.
- Add external-package behavior coverage for manual compaction polling,
  canonical standalone compaction results, and durable SubAgent close replay.

## v3.0.0 - 2026-07-30

- **Breaking:** Move the module to `github.com/floegence/floret/v3` and remove
  the v2 capability-handle graph, caller-assigned lifecycle identities, and all
  compatibility facades and runtime legacy decoders.
- Make `identity` the sole owner of thread, turn, run, prompt-scope, trace,
  logical-request, and artifact identities. Floret allocates lifecycle
  identities and permanently replays logical requests under their bound
  operation authority.
- Replace unbound Host capabilities with `Host.Threads`, `Host.Thread`, bound
  `Thread`, `Turns`, and `SubAgents` handles, exact-revision reads, stable
  pagination, and linearized pull subscriptions with explicit Gap recovery.
- Bind direct-child detail and pending-tool reads to `Child`, and bind turn and
  artifact reads for any validated descendant to `DescendantReader`, without
  repeating the target thread identity in read requests.
- Start and continuously drain committed v3 SubAgent work after spawn, message,
  interrupt, and committed-request replay while keeping read operations
  execution-free.
- Freeze the manually designed v3 API baseline, per-symbol v2 decision matrix,
  behavior contract, ownership matrix, and consumer provenance matrix.
- Separate opaque ordinary-host storage sources from the advanced physical
  storage SPI and add explicit v2.2-to-v3 preflight, plan, preview, apply,
  semantic-hash, and receipt contracts. Unsupported legacy extensions fail
  closed without mutation.
- Make Floret the sole queryable source for admitted conversation, lifecycle,
  approvals, todos, tools, artifacts, provider state, prompt cache, SubAgents,
  and Activity projections.
- Validate candidate and published v3 releases from blank modules without a
  workspace, replacement, or sibling checkout.

## v2.2.0 - 2026-07-29

- Export `runtime.ToolCallStream` so downstream event sinks can construct and
  test tool-call stream observations through the public API.

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
