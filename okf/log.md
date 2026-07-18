# Floret OKF Update Log

## 2026-07-19
* **Breaking**: Removed public fork source/destination turn and run identity
  mappings. `ForkThreadResult` now exposes only the operation identity and
  canonical destination thread summary; hosts read destination turns through
  the canonical thread read APIs.
* **Breaking**: Removed the duplicate `ListSubAgentDetailEvents` request/result
  contract. `ReadSubAgentDetail` is now the single parent-scoped paginated
  SubAgent detail API and continues to return unified `ThreadDetailEvent` rows.
* **Boundary**: Added architecture guards so fork identity maps and duplicate
  SubAgent detail page contracts cannot return to the public runtime surface.

## 2026-07-18
* **Breaking**: Removed the broad provider-free `ThreadMaintenanceHost` facade.
  `Host` no longer exposes top-level thread creation, title, fork, delete, or
  bulk child-close operations. These transitions now use the single-purpose
  `ThreadCreateHost`, `ThreadTitleHost`, `ThreadForkHost`, `ThreadDeleteHost`,
  and `SubAgentMaintenanceHost` capabilities; canonical reads use
  `ThreadReadHost`, and provider-free pending settlement uses the dedicated
  `PendingToolSettlementHost`.
* **Boundary**: Replaced `HostOptions.Store` and raw Store capability options
  with opaque `HostRuntime`/`ThreadCapabilityOptions.Runtime`. Bootstrap owns
  Store construction and lifetime; long-lived coordinators receive only the
  narrow handle for their lifecycle transition.
* **Test**: Added exact public method-set checks for `HostRuntime`, `Host`, and
  every narrow capability so removed lifecycle aliases cannot return silently.

## 2026-07-18
* **Fix**: Made every parent-scoped SubAgent operation fail with canonical
  thread-not-found when the parent journal is missing, even if orphaned child
  metadata remains in storage.
* **Breaking**: Removed the duplicate public `Host.StartThread` creation path.
  `CreateThread` is now the only public operation that can create a missing
  canonical journal, keeping creation capability explicit for downstream hosts.
* **Breaking**: Renamed the idempotent public thread creation contract from
  `EnsureThread` to `CreateThread`. Missing journals are created only through
  that explicit API; runtime, maintenance, and downstream host read paths must
  surface `ErrThreadNotFound` instead of treating creation as recovery.
* **Breaking**: Replaced string `RunTurnRequest.Input` with structured
  `TurnInput`, added canonical opaque message attachments through journal,
  prompt cache, model gateway, detail, turn, fork, and reopen projections, and
  rejected attachments when no host resolver is available.
* **Feature**: Added same-active-path `ReadThreadOverview` and canonical,
  idempotent `SetThreadTitle` with public title lifecycle events.
* **Breaking**: Unified root and child detail rows on `ThreadDetailEvent` and
  removed the public duplicate SubAgent detail DTO family and `subagent_id`
  identity alias.
* **Fix**: Added a strict transactional SQLite v11-to-v12 migration for the
  published v0.10 store shape, creating Floret-owned Agent todo state without
  rewriting journal or provider data; unknown versions and fingerprints remain
  rejected.
* **Feature**: Added bounded active-path paging and `ReadLatestThreadTurn` so
  hosts can read the latest admitted turn without replaying or caching the full
  journal.
* **Fix**: Required canonical user entries to carry the exact turn identity and
  removed predecessor-message substitution from turn projection.

## 2026-07-17
* **Fix**: Kept marker-only turns out of public turn pages unless their canonical
  user entry is committed, while preserving the full journal through ordinal.
* **Breaking**: Removed the transcript shortcut from `ThreadSnapshot` and added
  provider-backed and provider-free `ListThreadTurns`/`ReadThread` contracts for
  canonical ordinal history, run identity, failure, projection, and verified
  control payloads.
* **Feature**: Added typed Agent todo state with CAS updates, canonical tool
  authorship, Memory/SQLite persistence, fork identity rewriting, deletion, and
  reopen behavior.
* **Fix**: Removed branch-scanning interrupted recovery and leaf rewind. Resume
  now handles only the active lease turn or one unfinished active-path turn,
  rejects multiple unfinished turns, and never writes tool results for control
  signals.
* **Fix**: Made projection and provider-state persistence precede terminal
  markers so persistence failures remain unfinished and can be recovered
  without fabricating a terminal result.
* **Breaking**: Required caller-owned `runtime.Store`, removed facade `Close`
  methods and hidden memory Store creation, and reset SQLite to one canonical
  schema with explicit metadata and fingerprint validation.
* **Breaking**: Replaced flat gateway messages with typed roles, grouped ordered
  tool calls, typed tool results, strict JSON/adjacency validation, and required
  `ModelGatewayIdentity.StateCompatibilityKey`.
* **Boundary**: Moved opaque provider continuation persistence fully into
  Floret Store with journal-leaf and compatibility-key matching, restart-safe
  loading, fork isolation, thread-tree deletion, and failed-finalization
  semantics for persistence errors.
* **Feature**: Added canonical `ReadThreadContext` on provider-backed and
  maintenance facades, shared the snapshot type with subagent detail, and made
  malformed or identity-conflicting context journal data fail explicitly.
* **Breaking**: Made turn, compaction, committed-detail, and context identities
  explicit; removed `TurnID = RunID`, compaction metadata aliases, and inferred
  compaction generation/window fields.
* **Update**: Required manual compaction request/source identity, generated
  canonical automatic request IDs with `Source=engine`, and made compact-only
  results carry one validated terminal compaction event without provider state.

## 2026-07-16
* **Breaking**: Replaced the flat pending-tool settlement identity fields with
  `PendingToolSettlementTarget` and made settlement results validate and echo
  the complete target identity.
* **Boundary**: Declared the Floret journal and public projections as the sole
  queryable tool lifecycle source while allowing downstream product audit and
  diagnostics that do not duplicate tool state.
* **Fix**: Allowed a provider-backed Host to settle pending tool activity through
  its already-active thread without re-entering turn admission, while keeping
  maintenance hosts isolated by the existing turn lease.
* **Feature**: Added validated polling identity exclusions for presentation-only
  tool arguments so changing user-facing activity copy cannot bypass the
  no-progress duplicate-call guard.
* **Breaking**: Renamed turn projection availability from `ProjectionStatus` to
  `ProjectionAvailability` and removed the old Go and JSON contract names.
* **Fix**: Made live turn projections explicitly `running` until a terminal turn
  marker is durable and added Floret-owned projection validation.
* **Fix**: Kept private harness lifecycle events off the public runtime event
  sink and made runtime event validation cover nested stream, activity, and
  projection contracts.

## 2026-07-15
* **Breaking**: Made thread titles host-owned by default and added the explicit
  `ThreadTitleModeProvider` opt-in for Floret provider title requests.
* **Breaking**: Added typed normalized finish, completion, and continuation
  reasons plus raw finish and inference fields to public runtime and observation
  events, removing metadata as a lifecycle-reason contract.
* **Fix**: Added durable monotonic `ThroughOrdinal` versioning to every hosted
  turn projection and clarified that `ProjectedAt` is not an ordering key.
* **Breaking**: Replaced public string event/context/compaction states with
  finite typed contracts and explicit validation.
* **Breaking**: Separated turn execution errors from explicit projection
  availability and removed the projection-failure sentinel error.
* **Breaking**: Required `ForkOperationID` for public thread forks and added
  replayable operation results with explicit request, destination, and missing
  target conflicts.
* **Update**: Added dedicated memory and SQLite fork-operation storage, durable
  target markers, immutable parent/terminal-child plans, and
  restart-safe exact result replay.
* **Breaking**: Replaced the broad public `Host` and `ThreadMaintenanceHost`
  interfaces with concrete facade types and constructors returning pointers, so
  downstream packages own local minimal capability interfaces.

## 2026-07-14
* **Breaking**: Removed tool-declared parallel-safety scheduling. Ordinary calls
  in one model batch now execute concurrently, while provider-visible results
  remain in original call order.
* **Update**: Added product-neutral batch index and batch size metadata to tool
  approval requests and pending approval snapshots for stable host presentation.

## 2026-07-13
* **Update**: Documented cumulative run-level `MaxInputTokens` independently
  from cumulative total-token and per-request output limits.
* **Fix**: Documented one-operation thread-tree deletion and SQLite transaction
  rollback across journals, leases, metadata, artifacts, prompt scopes, and
  provider ledgers.
* **Update**: Documented `RebuildActivitySummary` as the shared public reducer
  for item-derived counts, status, severity, approval, attention, duration, and
  settled run-level terminal preservation.

## 2026-07-10
* **Update**: Documented `RunTurnRequest.SupplementalContext` as host-provided
  current-turn context that is rendered into provider requests without changing
  user input, durable thread history, permissions, approvals, working directory,
  or opaque provider state.

## 2026-07-07
* **Update**: Documented active turn lease reconciliation as part of Floret's
  interrupted-turn durable ledger recovery invariant, so terminal or recoverable
  turns cannot continue to block later `RunTurn` admission.

## 2026-07-06
* **Fix**: Aligned SQLite-backed thread forks with the runtime fork contract by
  rewriting forked turn/run metadata so reopened `ReadTurnProjection` calls
  resolve the destination execution identities returned by `ForkThread`.
* **Update**: Documented `runtime.ForkThread` as the public durable thread fork
  contract that rewrites destination execution identities and keeps host
  products from cloning Floret storage or shadowing display projections.
* **Update**: Documented interrupted-turn restart recovery as an AgentHarness
  durable ledger responsibility that restores provider-safe active history
  without requiring hosts to inspect or edit Floret storage.

## 2026-07-05
* **Update**: Documented `tool_result_batch` save points as durable turn
  activity segment boundaries while keeping repeated facts for one tool
  invocation merged into the original activity item.

## 2026-07-01
* **Update**: Clarified that `ThreadMaintenanceHost.ListSubAgents` is the
  canonical provider-free reload source for parent-scoped child-thread lists
  after host restart, while subagent activity timelines continue to expose only
  product-neutral child identity facts.
* **Update**: Documented subagent detail context snapshots as neutral
  model-bound facts whose context window comes from resolved model capability
  and policy, not parent/child thread identity or fork mode.
* **Update**: Documented canonical top-level subagent detail activity timelines
  rebuilt from retained child journal events so stale running row snapshots do
  not represent current tool state.
* **Update**: Documented neutral subagent task descriptions as durable runtime
  metadata while keeping product UI routing and presentation outside Floret.
* **Update**: Documented neutral pre-dispatch local tool error activity payloads
  so framework-layer failures still expose sanitized failure reasons in activity
  timelines without adding downstream UI policy to Floret.
* **Update**: Documented idempotent, out-of-order pending tool settlement and
  polling progress metadata for duplicate-call guard handling.
* **Update**: Documented `runtime.Event.Projection` as the live hosted-turn
  display projection emitted on committed thread-entry events, with aggregate
  `ActivityTimeline` retained only for lifecycle observation.
* **Update**: Extended `ThreadMaintenanceHost` with provider-free subagent list,
  activity timeline, detail, and detail-event read APIs so host UI reload/detail
  paths do not need provider-backed runtime hosts.
* **Update**: Renamed the provider-free maintenance facade to
  `NewThreadMaintenanceHost`/`ThreadMaintenanceHost`, documented that it is an
  independent non-provider host implementation with an explicit required store,
  and clarified that gateway-backed hosts use `ModelGatewayIdentity` instead of
  provider transport fields in runtime config.

## 2026-06-30
* **Update**: Documented the provider-free public facade for thread maintenance
  summary, turn projection read-back, pending tool
  settlement, child close, and thread-tree deletion paths.
* **Update**: Documented `ReadTurnProjection`, `ErrTurnNotFound`, and
  `ErrRunNotFound` for durable hosted-turn projection reloads that require
  explicit `ThreadID`, `TurnID`, and `RunID`.
* **Update**: Clarified that parallel tool observations may arrive by completion
  order while provider-visible tool result transcript messages are appended in
  original tool-call order.
* **Update**: Documented `tool_activity_updated` and
  `Invocation.UpdateActivity` as product-neutral running activity presentation
  updates that merge into the original tool item without completing it.
* **Update**: Clarified that activity renderer payloads are host-supplied public
  display data that Floret validates and preserves without defining downstream
  UI layout or product field priority.
* **Update**: Clarified that approved tool activity may remain pending before
  dispatch starts, and that `requires_approval` is lifecycle history rather than
  an active decision-needed flag.
* **Update**: Clarified that caller cancellation during runtime projection or
  turn finalization remains a cancelled terminal fact, preserving canceled
  activity settlement after a host stops a run.
* **Update**: Documented queued local tool calls and
  `tool_dispatch_started` as separate lifecycle facts, so pending batched
  siblings remain pending until permission, approval, and dispatch gates pass.
* **Update**: Documented runtime turn-result projections as canonical
  current-turn display projections built from raw-capable journal facts, while
  default detail reads remain preview-only inspection surfaces.

## 2026-06-29
* **Update**: Documented failed/cancelled terminal turn projection as the
  cross-segment unavailable-state settlement source and `SettlePendingTool` as
  the public host-owned pending-work settlement API.
* **Update**: Documented concurrent tool result observation before slower
  sibling completion while preserving provider-safe durable save points.
* **Update**: Documented tool approval activity as a lifecycle state on the tool
  item itself, preventing duplicate tool and approval rows for one invocation.
* **Update**: Documented live `runtime.Event.ActivityTimeline`, event-time tool
  detail projection, and duration-consistent activity validation for tool
  lifecycle rows.
* **Update**: Documented `SettlePendingTool` as the detail-only counterpart to
  `CompletePendingTool`, including the rule that host-owned pending outcomes
  update the original tool activity without adding provider-visible context.
* **Update**: Documented terminal activity settlement for cancelled and failed
  turns, including the rule that hosts consume Floret terminal projections
  instead of synthesizing final tool state.
* **Update**: Documented public runtime not-found sentinels for host facade
  integrations so downstream products can use `errors.Is` without parsing error
  strings or importing internal packages.

## 2026-06-28
* **Update**: Documented `ThreadTurnProjection` as the Floret-owned display
  projection for hosted turns, including control-signal segments and the rule
  that hosts must not synthesize main-thread activity timelines.
* **Update**: Documented Floret-owned row activity timelines and structured tool
  result status for thread and subagent detail APIs, so hosts do not rebuild
  activity from raw content or audit tables.
* **Update**: Documented product-neutral pending approval snapshots as the
  current-state companion to approval detail events, while keeping approval UI
  and product policy in downstream hosts.
* **Update**: Documented parent stop versus thread delete lifecycle boundaries
  for subagents, including public `CloseSubAgents`, cascading
  `DeleteThread`, and engine-owned subagent fork mode.
* **Update**: Documented transcript-free thread summary recovery and
  parent-scoped subagent activity timelines for host UI integration.
* **Update**: Documented that provider continuation recovery preserves the
  Floret ordered transcript by committing live assistant prefixes and
  backfilling only uncommitted suffixes.
* **Update**: Documented ordered hosted thread detail events and committed
  thread-entry observations as Floret's public durable execution transcript
  read model for downstream hosts.

## 2026-06-26
* **Update**: Documented `runtime.ToolSurfaceProvider` as the product-neutral
  dynamic tool surface hook for refreshing tools, hosted tools, prompts, and
  host context at provider-loop safe points.
* **Update**: Documented the hosted context lifecycle boundary and removed the
  old projected transcript integration path from public runtime guidance.
* **Update**: Documented terminal control-signal output normalization from
  signal payloads or same-step assistant text.
* **Update**: Documented Floret-owned manual compaction admission and terminal
  `noop` observations for contexts that are too small, lack a safe cut point,
  or would not shrink enough to justify checkpoint creation.

## 2026-06-25
* **Update**: Documented public manual compaction operation identity, cancelled
  lifecycle observations, poll-stage diagnostics, and the Test UI context
  compaction scenario check.
* **Update**: Documented public compaction debug observations for safe
  diagnostics across generation, projected request rebuild, validation, and
  installation stages.
* **Update**: Documented projected manual context compaction, including active
  safe-point polling, idle compaction-only checkpoint results, and observation
  request correlation.

## 2026-06-24
* **Update**: Documented compacted context targets and the requirement that a
  complete compaction event follows full provider request validation.
* **Update**: Documented that durable compaction entries are committed only
  after the compacted provider request has passed validation.
* **Update**: Documented parent-scoped subagent detail APIs, bounded wait
  semantics, child run timeout behavior, and close-as-stop lifecycle rules.

## 2026-06-23
* **Update**: Documented provider-neutral reasoning selection in the public
  config and runtime APIs.
* **Update**: Added provider workflow guidance for model-level reasoning
  capabilities, official provenance, dynamic metadata, and adapter validation.

## 2026-06-20
* **Creation**: Added the initial OKF v0.1 project knowledge bundle.
* **Update**: Documented OKF maintenance rules in the repository guide.

## 2026-06-23
* **Update**: Documented parent-managed durable child threads in the runtime API,
  runtime layers, and execution identity concepts.
