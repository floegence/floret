# Changelog

## v4.0.13 - 2026-08-21

- Add an explicit, ownership-bound storage reset API for a stopped Host.
  Supported backends clear all Floret-owned opaque records transactionally,
  then initialize and verify a fresh current-schema store without exposing or
  migrating prior records.

## v4.0.12 - 2026-08-18

- Restore the permanent version 2 to version 3 to version 4 to version 5
  session-tree migration lineage. Runtime startup now migrates domain state,
  updates the logical schema version, and verifies the final invariant in one
  backend transaction; supported current stores remain byte-stable, while
  drifted, corrupt, or future state fails closed without mutation.
- Make queued turns and interaction decisions durable before publishing
  success, claim an unknown effect source once before retry dispatch, and fence
  active thread subtrees and effect work during delete and shutdown. These
  corrections preserve the frozen v4 exported API while preventing restart
  resurrection, duplicate irreversible effects, and late terminal-state
  regression.
- Keep hydrated interrupted and effect-retry lifecycle state aligned with live
  projection, enforce monotonic per-thread view publication, and remove retired
  internal storage and fork-operation machinery with no supported consumer.

## v4.0.11 - 2026-08-16

- Keep fallback thread titles trim-stable when whitespace lands exactly on the
  canonical length boundary, so accepting a long first message cannot corrupt
  otherwise valid session-tree authority state.

## v4.0.10 - 2026-08-16

- Make typed `ThreadService.Send` complete canonical turn acceptance before it
  returns or publishes the user segment. Acceptance failure now returns to the
  caller with the thread view unchanged, while provider execution remains
  asynchronous after the accepted receipt.

## v4.0.9 - 2026-08-16

- Settle ordered input and tool presentation against canonical terminal facts.
  Resolved input interactions retain one stable segment, schema-correction tool
  drafts cannot remain live after recovery, and an input continuation claims
  its waiting run exactly once.

## v4.0.8 - 2026-08-16

- Expose thinking and all other typed current-view items as one stable ordered
  presentation sequence. Live text grows in place, tool approval and results
  retain the original segment identity, and canonical History/reopen rebuilds
  the same IDs and ordinals without a second presentation ledger.

## v4.0.6 - 2026-08-16

- Expose canonical thread context and compaction snapshots through the narrow
  `runtime.ThreadContextReader` capability implemented by `Host.ThreadService`.
  Compaction lifecycle checkpoints merge by operation identity and remain
  readable after runtime restart without exposing storage internals.

## v4.0.4 - 2026-08-15

- Install a canonical fallback thread title atomically with the first accepted
  user message. Automatic title generation now preserves that title while
  pending or failed and replaces it only after provider success; startup also
  repairs historical empty titles from canonical user input.

## v4.0.3 - 2026-08-15

- Exclude provider-only schema correction messages from the engine result used
  to append canonical thread history, so invalid calls cannot create runtime
  current-view tool rows after the model recovers.

## v4.0.2 - 2026-08-15

- Keep schema-invalid local tool calls inside the provider correction loop.
  Validation failures still return an ordered tool result to the model, but no
  public tool activity is emitted and no handler is dispatched.

## v4.0.0 - 2026-08-14

- Replace the parallel v3 lifecycle paths with the typed ThreadService runtime.
  Each thread now owns one in-memory runtime while the canonical journal remains
  the durable source for accepted input, interactions, queue intent, and terminal
  outcomes.
- Make Send, Respond, Cancel, Retry, and effect retry idempotent and immediate;
  normal stop and user rejection remain quiet local outcomes instead of product
  failures. Runtime restart hydrates active input, approval, and queued work.
- Remove the v3 admission, authority, recovery-handle, projection-delta, and
  duplicate lifecycle APIs from the v4 module surface.

## v4.0.5 - 2026-08-15

- Preserve the public dynamic tool surface distinction between omitted and
  explicitly empty tool definitions. Omitted local or hosted definitions now
  inherit their configured registry/defaults, while explicit empty slices
  continue to clear provider-visible definitions.

## Unreleased

- Fix terminal thread presentation so the run-level `TurnResult.Output` is not
  appended as a second assistant item after streamed text. Terminal current
  views now settle from canonical ordered segments, preventing duplicate final
  replies while preserving legitimate multi-segment assistant output.

- Add the internal Phase 1 unified Thread/Turn/event primitives and contiguous
  revision log. Duplicate event IDs are idempotent, revision gaps request a
  canonical resync, and the model keeps pending approval/input as one
  interaction boundary without changing the published v3 surface.

- Classify malformed or invalid control-signal projection as the public
  `control_error` turn failure instead of an undifferentiated engine-contract
  failure. Preserve assistant text, the original control tool-call identity,
  and its expected disposition while projecting a terminal control activity
  error so hosts can render the local failure without replacing the reply or
  reopening a user-input wait.

## v3.2.39 - 2026-08-11

- Keep live turn projections monotonic by canonical journal ordinal. Late or
  conflicting committed entries can no longer revive a requested approval
  after its terminal decision, and control activities now preserve the
  canonical waiting, continue, or terminal disposition.
- Return a durable rejection receipt without waiting for provider continuation
  or downstream observation sinks. Rejection observations remain ordered and
  runtime-owned background work that is drained during shutdown.

## v3.2.38 - 2026-08-11

- Preserve canonical approval lifecycle precedence while merging repeated tool
  activity segments. Large or rapidly settled rejection batches no longer
  retain `requires_approval` or attention on declined items, and later generic
  pending activity cannot regress a requested item away from `waiting`.

## v3.2.37 - 2026-08-11

- Keep structured user-declined tool results as quiet terminal activity when a
  live projection does not contain the matching approval-resolution event.
  Accept `declined` in the sanitized tool-result status contract so requested
  approvals cannot be projected as `success` with an unresolved approval state.

## v3.2.35 - 2026-08-10

- Allow approval decisions for independent calls in the same provider batch to
  arrive in source, reverse, or concurrent order while preserving exact target
  identity, decision replay, effect authorization, and ordered provider
  continuation results.
- Represent user rejection as a structured provider-visible `declined` result
  and quiet observation status instead of a dispatch error or failed tool.
  Rejection never invokes the handler and does not cancel sibling approvals.

## v3.2.34 - 2026-08-10

- Preserve the product-neutral `logical_request_id` from receipt-first turn
  admission through canonical turn snapshots, pages, bootstrap, replay, and
  restart. Retry projections inherit their source logical lifecycle identity,
  while legacy turns without the metadata remain compatible.

## v3.2.30 - 2026-08-08

- Make terminal `ThreadTurnProjectionDelta` values self-contained base-zero
  checkpoints so hosts can converge after dropped live observation events.
  Running deltas remain minimal and ordinal-bound.

## v3.2.29 - 2026-08-05

- Add validated `ThreadTurnProjectionDelta` updates to committed runtime events.
  Live projection now retains only a bounded open event suffix instead of
  replaying the complete turn journal for every commit. Hosts can apply exact
  ordinal-bound segment replacements and reload canonical state on identity or
  ordinal gaps; the existing full projection remains available for compatible
  consumers and the domain schema stays at v4.

## v3.2.28 - 2026-08-05

- Add optional host-authored answer summaries to question activity
  presentation. Non-secret answers retain ordered display values, while secret
  answers must use an explicit redacted marker with no values. Validation,
  cloning, merging, event sanitization, and observation projection preserve the
  same public contract without changing the domain schema v4 lineage.

## v3.2.27 - 2026-08-05

- Return a user-rejected approval to the provider as a failed tool result
  without entering the tool handler or failing the whole turn. Durable
  approval identity remains `rejected` with reason `user_rejected`, exact
  decision replay stays idempotent, and memory and SQLite hosts continue the
  provider loop so the assistant can respond naturally to the decision.
- Require effect-result finalization only when the authorization dispatcher
  actually enters the handler. Pre-handler rejection remains non-executable,
  while authorization, authority, proof, and persistence failures continue to
  stop the turn fail-closed.

## v3.2.26 - 2026-08-04

- Cancel a turn waiting for approval with the current renewable lease proof,
  so normal heartbeat renewals cannot leave the canonical approval or turn
  stuck after a stop request.

## v3.2.25 - 2026-08-04

- Preserve tool-authored activity presentation in durable approval entries and
  public effect authorization requests. Subagent detail reads retain the exact
  command or resource renderer while approval is waiting and after restart;
  journals written by earlier v3 releases recover presentation from the
  matching durable tool call without changing approval identity or schema v4.

## v3.2.24 - 2026-08-04

- Share one live turn projection recorder across the host harness and execution
  event paths. Tool-authored presentation observed during execution is now
  available when committed approval details are projected, so requested and
  approved lifecycle updates cannot briefly regress to a generic tool label.

## v3.2.23 - 2026-08-04

- Preserve the observed tool presentation when a live committed approval
  detail carries both a generic message activity and a generic timeline item.
  Approval lifecycle facts still update the matching tool only, and tools with
  no observed presentation retain the neutral fallback.

## v3.2.22 - 2026-08-04

- Preserve the tool-authored activity presentation in host-facing live turn
  projections while approval request and resolution details commit before
  dispatch. Approval-only history retains its neutral fallback, and the
  durable journal remains unchanged.

## v3.2.20 - 2026-08-04

- Preserve the process-local turn execution proof in the single-snapshot
  `ThreadReader.Bootstrap` path, so an admitted or executing turn cannot be
  transiently projected as a recoverable interruption. Restarted hosts still
  fail closed without that in-memory proof.

## v3.2.19 - 2026-08-04

- Keep a thread projected as `running` across the short admission handoff
  between its durable lease commit and process-local execution registration.
  The handoff is tracked only in memory and does not weaken expiry, restart,
  owner, generation, acquisition, thread, or turn drift checks.

## v3.2.18 - 2026-08-04

- Keep a running turn projected as `running` while a durable lease renewal has
  committed but the process-local execution registry still holds the preceding
  heartbeat proof. The reader accepts only a validated monotonic successor in
  the same owner, generation, acquisition, thread, and turn lineage; expired,
  replaced, restarted, or authority-claimed executions remain interrupted.

## v3.2.17 - 2026-08-04

- Reuse the validated decoded session-tree domain while the exact durable
  envelope bytes remain unchanged. Every read still enters one backend
  snapshot and compares its complete persisted bytes; external changes are
  decoded and validated strictly, while corrupt or future state continues to
  fail closed without changing schema v4 or its migration lineage.

## v3.2.16 - 2026-08-04

- Cache validated host-facing thread-list projections by their canonical
  inventory fingerprints, avoiding repeated projection work when the durable
  bounded inventory has not changed.

## v3.2.15 - 2026-08-04

- Cache the validated decoded root-thread inventory while its exact durable
  record bytes remain unchanged. External drift is still detected and rejected
  before any cached projection is served.

## v3.2.14 - 2026-08-04

- Close a requested approval as `canceled` when an interrupted tool batch
  persists a canceled terminal result without a later run-end marker. Canonical
  thread projections remain readable during startup recovery while preserving
  the existing session-tree schema v4 lineage.

## v3.2.13 - 2026-08-04

- Add `Threads.ListInterruptedTurnRecoveryCandidates` for one-snapshot,
  deterministic discovery of root and SubAgent interrupted-turn identities.
  The read-only API leaves recovery authorization and lease proof binding to
  the host and does not change the session-tree schema v4 lineage.

## v3.2.12 - 2026-08-03

- Classify every failed engine result with an explicit failure origin. Existing
  provider, tool-dispatch, storage, and cancellation classifications retain
  precedence; otherwise an internal validation failure is classified as an
  engine contract failure so AgentHarness persists the original error instead
  of replacing it with a secondary failure-classification error.

## v3.2.11 - 2026-08-03

- Preserve `ErrThreadDeleted` when `ThreadReader.Bootstrap` targets a
  tombstoned identity through the single-snapshot backend path. Missing
  identities remain `ErrThreadNotFound`, and deleted snapshots never invoke
  the bootstrap projection callback.

## v3.2.10 - 2026-08-03

- Project `ThreadReader.Bootstrap` from one canonical backend snapshot instead
  of reopening and decoding the complete session-tree domain for each included
  read model. Thread, turn, approval, todo, context, pending-tool, and SubAgent
  projections retain one exact revision and the existing subscription handoff.
- Preserve the public API, JSON contract, error classification, and domain
  schema v4 while reducing bootstrap backend reads from 30 to one for both
  Memory and SQLite stores.

## v3.2.9 - 2026-08-03

- Let host tool definitions preserve sanitized presentation metadata from a
  parseable schema-invalid JSON object without granting it invocation authority.
  Invalid calls still fail before resource, permission, effect, and handler
  execution, while canonical activity can retain labels such as an attempted
  terminal command.
- Contain invalid-activity callback failures and panics, and never invoke the
  callback for valid calls or malformed JSON.

## v3.2.8 - 2026-08-03

- Return each root thread's optional latest turn from `Threads.ListThreads`
  using the same validated projection as `ThreadReader.ReadOverview`.
  Navigation lists can now obtain status, failure, waiting input, and message
  preview authority without reopening every thread.
- Preserve one bounded inventory read and zero complete session-tree domain
  reads per page. Empty threads omit `latest_turn`, and the session-tree domain
  schema remains v4 with the existing automatic v2-to-v3-to-v4 lineage.
- Reconcile stale heartbeat renewal with terminal lease settlement so a slow
  approved effect cannot commit `completed` and still return a stale-authority
  error when finalization removes the durable lease at the same instant.

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

- **Breaking:** Move the module to `github.com/floegence/floret/v4` and remove
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
