# Floret OKF Update Log

## 2026-08-03
* **Terminal lease settlement**: Turn finalization and stale-renewal
  classification now share a narrow fence from durable commit through local
  lease cleanup. Slow approved effects no longer commit a completed turn and
  then surface a stale-authority error from a concurrent final heartbeat,
  while blocked renewal I/O and detached thread-title work remain independent.
* **Batch latest-turn projection**: `Threads.ListThreads` now returns each
  optional latest turn from the same validated v4 inventory path as its root
  snapshot and revision. Hosts can build navigation lists without reopening
  and decoding every canonical thread domain; empty threads omit the field and
  the durable schema remains v4.
* **Indexed inventory migration**: Session-tree domain schema v4 adds a strict
  root-thread inventory record committed atomically with the complete domain.
  Startup migrates v3 automatically, verifies current index/domain agreement,
  and lets list reads avoid decoding unrelated artifacts and provider state.
  Explicit v2.2 physical conversion emits and validates the same complete v4
  record set and includes the inventory in its target semantic commitment,
  while previously issued `/1` plans remain applicable and replayable across
  the automatic v4 upgrade.
* **Bounded inventory projection**: `Threads.ListThreads` now derives a complete
  page of exact root snapshots and revisions from one canonical domain read.
  Inventory refresh no longer multiplies full session-tree decoding per item or
  starves concurrent approval mutations behind repeated read fences.
* **Interrupted approval timeline closure**: Terminal tool errors now close a
  requested approval as `timed_out` when no later run marker exists, while an
  aborted marker takes precedence with `canceled`. Stop/dispatch races no
  longer make historical Floret projections unreadable.
* **Automatic domain migration**: The v3 backend session-tree schema now has a
  permanent contiguous lineage. Startup atomically migrates exact v2 state to
  v3, reconstructs missing SubAgent admission authority from canonical source
  facts, preserves active leases, uses read-only terminal proofs, and fails
  closed without mutation for drift, ambiguity, corruption, or future state.

## 2026-08-02
* **Renewable approval dispatch authority**: Approved effect callbacks now
  inherit the active turn's renewable lease binding, so host authorization that
  crosses a heartbeat uses the current proof without weakening owner,
  generation, or authority-lineage validation.
* **Interrupted approval compatibility**: Canonical projection now settles a
  requested approval from failed or aborted terminal authority when older
  recovery history contains a terminal tool result without the corresponding
  approval-resolution detail event. The journal and schema remain unchanged.
* **Cancelable backend serialization**: Waiting for the shared backend
  transaction fence now respects the operation context, keeping approval,
  cancellation, and shutdown latency bounded behind active transactions.
* **Approval execution concurrency**: Long-running provider turns no longer
  retain the host-wide mutation lock while waiting for canonical approval.
  Same-thread execution remains serialized, and one backend transaction fence
  coordinates session-tree, prompt, and logical-request ledger writes.

## 2026-08-01
* **Partial live projection status**: Host-facing committed-event recorders now
  report `running` when their local mid-turn window starts after the durable
  turn-start marker, keeping every emitted live projection valid until a
  terminal marker is observed.
* **Least-authority host SDK**: Added native read, lifecycle, turn execution,
  compaction, and SubAgent capability views issued from an exact bound Thread.
  Added an atomic bootstrap read model that connects directly to pull
  subscriptions at one revision.
* **Durable admission plans**: Floret now persists canonical execution intent at
  admission. Restart execution consumes only the admission receipt and
  ephemeral `ExecutionContext`; supplemental context is excluded from durable
  request fingerprints and lifecycle state.
* **Projection and todo authority**: Canonical projection reads carry
  authoritative revision/provenance, offline derivation has a distinct validated
  type, and the storage kernel owns the 40-item/single-in-progress todo rules.
* **Adoption quality**: Added a production OpenAI-compatible SQLite example,
  documented-symbol validation, deterministic `florettest.NewIDSource`, and
  blank-module bootstrap/restart/provider-constructor coverage.
* **Receipt-first turn admission**: Added `TurnExecutor.AdmitTurn` and
  `TurnExecutor.ExecuteAdmission` so hosts can persist product coordination
  after Floret admits the canonical user message and before provider execution.
  The admission receipt is the durable handoff; Floret remains the sole source
  of admitted conversation and turn lifecycle facts, and no command-bearing
  execution compatibility API is exposed.

## 2026-07-30
* **v3 host adoption completion**: Added bound root-thread and direct-child
  canonical reads, replayable title mutation, exact-target pending-tool
  recovery, and exact-proof interrupted-turn recovery required by downstream
  production adoption without restoring v2 capability handles.
* **v3 capability completion**: Added exact-thread standalone compaction,
  immutable Agent manual-compaction polling, and parent-bound SubAgent wait and
  durable close operations without restoring v2 handles or unbound requests.
* **v3 public boundary**: Replaced the v2 capability-handle graph with bound
  Host, Threads, Thread, Turns, and SubAgents surfaces, Floret-allocated
  lifecycle identities, exact-revision reads, and pull subscriptions with
  explicit Gap recovery.
* **Bound SubAgent reads**: Added direct-child detail and pending-tool reads on
  `Child`, plus turn and artifact reads for one validated descendant on
  `DescendantReader`, without caller-supplied target identities after binding.
* **SubAgent execution**: Made committed v3 spawn, message, interrupt, and
  request replay activate and continuously drain durable child input while
  preserving execution-free read operations.
* **Single source of truth**: Documented Floret ownership of admitted Agent
  lifecycle and the prohibition on host-side shadow stores and projections.
* **Storage and migration**: Separated opaque ordinary-host Sources from the
  advanced storage SPI and documented the explicit v2.2-to-v3 representability
  and migration contract.
* **Release governance**: Moved compatibility and blank-module adoption gates
  to the `/v3` module and the manually designed v3 API baseline.

## 2026-07-29
* **Runtime observation API**: Exported `runtime.ToolCallStream` so downstream
  event sinks can construct and test provider-neutral tool-call stream facts.

## 2026-07-29
* **v2.1 public API**: Completed the identity-bound canonical read, approval,
  todo, pending-work, artifact, retry, and SubAgent handle surfaces required by
  downstream hosts while keeping bound root identities out of requests.

## 2026-07-29
* **Public API**: Established `/v2` packages for config, provider, runtime,
  storage, tools, observation, and test-only florettest; deleted the v1
  bootstrap/binder/factory and generator surface without aliases.
* **Runtime**: Added a composition-root-only Host, immutable Agent, and exact
  identity-bound handles for conversation, SubAgent, inventory, and recovery
  authority.
* **Provider**: Unified every model call on `provider.Gateway`; deterministic
  fake behavior now exists only in `florettest.ScriptedGateway`.
* **Storage**: Unified memory and SQLite on the Backend domain kernel and added
  the third-party conformance suite.
* **Migration**: Added an explicit exact schema-v16 to v2 transaction with
  replay identity, authority/count/hash verification, metadata rejection, and
  failure/cancellation/panic rollback tests. Normal startup never migrates.
* **Release**: Replaced v1 compatibility and generated-host gates with a v2 API
  baseline plus blank-module candidate and published adoption checks.

## 2026-07-29
* **v1 public contract**: Removed runtime reasoning and lifecycle-reason
  aliases, made thread title status/source fields typed without changing JSON,
  and preserved schema-v16 plus v3-v15 migration semantics.
* **Provider Host construction**: Replaced public option struct literals with
  opaque Turn, compaction, and SubAgent constructor values plus family-scoped
  options. Factories revalidate config and current authority.
* **Boundary validation**: Added self-contained validation for Agent todo,
  SubAgent, detail-page, activity, and Store maintenance root DTOs and mapped
  corrupt production results to stable typed error families.
* **API governance**: Added the generated `go/types` v1 surface baseline,
  fixed-version `apidiff`, SemVer rules, and packaged generated-host adoption
  coverage.
* **Adoption**: Updated all five scaffold profiles, public examples, localized
  README snippets, and package/OKF guidance for the v1 constructor surface.

## 2026-07-26
* **Exact canonical turn reads**: Added root-bound and parent-scoped
  `ReadThreadTurn` over active-path admission authority, consistent Memory and
  SQLite behavior, public read DTO validation, and downstream adoption gates.
* **Typed message provenance**: Added canonical `ThreadUserMessageOrigin` to
  public turn snapshots, atomically classified SubAgent mission, follow-up, and
  pending-completion admissions, recovered pre-marker origins from exact durable
  input authority across immutable full-path fork lineage, and removed internal
  input authority metadata from public detail events.
* **Breaking read boundary**: Replaced public entry-shaped turn cursors with
  versioned opaque thread/mode-bound tokens and reduced public retry source to
  `TurnID`; exact entry anchors remain validated inside the journal authority.
* **Canonical discovery**: Added a composition-only paginated root-thread
  inventory over validated `ThreadSummary` values and added parent-bound typed
  turn pages for direct and deep SubAgent descendants.
* **Detail boundary**: Removed compaction window, generation, entry-anchor, and
  raw summary internals from public detail events while retaining sanitized
  lifecycle facts. Canonical `UserEntryID` remains an opaque presentation anchor.
* **Public read validation**: Added finite thread-title status and source
  vocabulary plus public thread snapshot and summary validation. Unknown or
  contradictory values fail an explicit host-boundary contract.

## 2026-07-25
* **Store startup**: Made compatible migration identity lazy and Floret-derived,
  added typed inspecting/migrating/verifying/opening progress, explicit optional
  completed-phase facts, and one bounded stale/busy re-inspection so ordinary
  hosts do not rebuild the maintenance state machine.
* **SDK adoption**: Added safe SQLite startup, shared request validation,
  sealed turn-execution presets, and versioned host-owned composition profiles
  with deterministic fake-provider smoke tests and immutable candidate module
  adoption checks.
* **Recovery boundary**: Added complete typed canonical pending settlement
  target reads for root and directly parent-bound SubAgent capabilities.
  Production recovery now traverses the full descendant tree and reconciles
  these targets before readiness instead of rebuilding authority from paginated
  presentation events.
* **Progressive disclosure**: Moved the README starter to generated local
  create/read/run interfaces while retaining the existing narrow runtime
  capability APIs as the complete advanced surface.

## 2026-07-24
* **Published adoption**: Added one post-release gate that resolves an exact tag
  from a blank downstream module, verifies module and checksum identity, and
  runs public durable-host, gateway, approval, recovery, and Store maintenance
  smoke coverage without workspace or local replacement wiring.
* **Storage open**: Replaced context-free implicit-migration SQLite open with an
  inspection-bound, context-aware request. Missing/empty creation now requires
  no user table, index, view, or trigger inside the retained open transaction;
  current open requires the exact reader identity, matching lease, valid
  contract, and valid authority graph. All open failures are typed maintenance
  outcomes and explicit apply is the only public migration path.
* **Attachments**: Added bounded optional host-attested text statistics to
  canonical message attachments, deep-copying them through admission, journal,
  cache, retry, fork, SubAgent, and public projections while keeping attachment
  bytes and content truth host-owned. New admission and Append paths enforce
  strict limits while stored schema-v16 journals and exact replay retain their
  historical compatibility boundary.
* **Model gateway**: Added an optional prepared-request contract for gateways
  that expand attachment content. Complete rendered estimates now participate
  in pre-request pressure and input limits, stable fingerprints use the existing
  payload-hash ledger, and every linear in-memory handle is consumed or closed
  across compaction, gate/storage failures, cancellation, overflow, and Store
  shutdown paths, including standalone manual compaction. Descriptor-only
  attachment requests now use a complete serialized-request UTF-8-byte
  conservative upper bound for projected pressure with additive anchor-safe
  components, without labeling it an exact token count; attachment-free
  estimates are unchanged.
* **Adoption**: Added task-focused guides for composition-root capability
  issuance, host-owned model gateways, effect approval, SQLite Store
  maintenance, exact interrupted-turn recovery, and durable projection
  rendering, with runnable examples and `florettest` as the shortest paths.
* **Storage**: Documented public SQLite lease-policy options, zero-write
  inspection and verification, explicit plan/apply migration, typed
  state/reason/progress outcomes, and continuous supported v3-v15 migration with
  quiescent-authority rejection for ambiguous v3-v13 data. Legacy artifacts
  migrate only with an exact canonical result binding and no obsolete product
  URL; otherwise the source Store rolls back unchanged. Inspection and
  verification now use byte-stable private DB-plus-sidecar snapshots, recover
  stable live or crash-left WAL and rollback-journal facts only inside the copy
  without touching source DB/WAL/SHM files, type source drift during capture as
  busy, and keep apply's final verification inside the committing maintenance
  transaction.
* **Boundary**: Clarified that Floret owns product-neutral Agent lifecycle and
  Store maintenance facts while hosts own transport, concrete effects, policy,
  operator authorization, and polished localized UI presentation.
* **Observation**: Declared `runtime.Event` and `observation.Event` transient and
  lossy rather than persistent replay contracts; durable UI recovery reloads
  Floret public snapshots and projections.
* **Quality**: Expanded the documented gate to cover tests, vet, focused race
  detection, public examples and conformance helpers, vulnerability scanning,
  and `GOWORK=off` dependency isolation.
* **Lifecycle**: Removed the obsolete single-thread delete primitive from the
  general internal repository contract and its Memory, File, and SQLite
  implementations. Canonical deletion now exists only as root-tree authority
  on Memory and SQLite, with tombstone replay and fail-closed missing-root
  semantics; the file-backed test journal exposes no deletion capability.

## 2026-07-22
* **Effects**: Made the host authorization callback context-aware, propagated
  the selected execution scope to local handlers, and bounded it by canonical
  turn cancellation without reclassifying pre-dispatch cancellation as an
  authorization failure.
* **Lifecycle**: Defined an exact valid terminal `RunTurn` result, or an exact
  canonical terminal read after an incomplete return, as the active-authority
  release barrier before pending-work recovery settlement.
* **Reads**: Exposed canonical approval-queue reads through the bound
  `ThreadReadHost` capability so reconnect/bootstrap projections can reload
  root and descendant pending approvals without an active turn host or a
  second host-owned queue.
* **Fix**: Canonicalized projected control-signal payloads into standard JSON
  value trees before transcript and journal persistence, preserving nested
  named struct and slice payloads across Memory and SQLite reads while rejecting
  non-JSON values before they can create corrupt authority state.

## 2026-07-20
* **Authority**: Added exact durable turn admission/replay and canonical turn
  entry indexes with before/since cursors, retry-source identity, typed terminal
  failures, and fail-closed interrupted recovery precedence.
* **Messages**: Added ordered canonical user-message references for text, file,
  directory, terminal, and process display facts while keeping validated
  `SupplementalContext` current-turn-only and out of cache, ledgers,
  compaction, continuation state, and later provider history.
* **Approvals**: Made the aggregate root and descendant approval queue durable,
  with one decisionable current item, exact decision replay, atomic
  approval/effect/proof settlement, and deterministic promotion.
* **Titles**: Started durable automatic-title generation immediately after
  first user admission, concurrently with the main turn, with generation/token
  CAS, manual-title precedence, and Store-owned cancellation/join/recovery.
* **Effects**: Gave each ordered batch result a fresh finalization window,
  continued later sibling finalizers after earlier errors, and converged every
  post-dispatch persistence failure to a known terminal outcome or `unknown`
  without handler replay.
* **Storage**: Upgraded the authority schema to v16 with exact v14/v15
  migrations, canonical path-depth indexing, raw-entry integrity checks, and
  bounded turn-page reads across Memory, File, and SQLite backends. V15 failures
  with no durable origin migrate to `legacy_unclassified` instead of using
  error-text heuristics or rejecting otherwise valid predecessor data.
* **API**: Made committed runtime events require their durable detail, validated
  canonical user admission identity and payload, and proved Memory and SQLite
  turn pages are synchronously readable before provider lifecycle begins.
* **Boundary**: Amended the frozen interrupted-turn recovery object graph so the
  composition root delivers a factory bound to one exact root or parent-child
  turn owner and generation. Recovery retries may refresh only that target's
  heartbeat proof and cannot follow replacement or future-turn authority.
* **Storage**: Added atomic interrupted-turn resolution validation across the
  current authority snapshot, admission proof, finish ledger, and terminal
  entry; recovery takeover now rejects missing or drifted admission authority
  before generation, effect, or journal mutation.
* **Storage**: Made SQLite writer admission context-cancellable and keyed by the
  canonical physical path while keeping readers outside the writer queue;
  uncoordinated cross-process writer conflicts now fail immediately without
  busy-timeout polling or retry.
* **Boundary**: Moved Test UI provider and local session metadata to a separate
  WAL sidecar and removed runtime-database metadata import. The Test UI never
  reads, maps, repairs, or repopulates host records from Floret-owned storage.

## 2026-07-19
* **Boundary**: Bound each Test UI session to its exact turn and SubAgent
  factories instead of retaining the composition-root binder bundle; an active
  session can no longer mint authority for another canonical thread.
* **API**: Allowed parent-bound SubAgent reads to be rebound for open, closing,
  or closed parents and made detail reads cover any validated descendant while
  keeping mutation operations restricted to direct children.
* **Storage**: Bound fork-mode SubAgent publication and replay to the exact
  parent source, child ownership metadata, pinned source leaf, destination
  metadata, and artifact closure in one atomic transition.
* **Storage**: Made new fork-mode SubAgent publication compare its pinned source
  leaf with the parent's current leaf inside the same Memory lock or SQLite
  transaction, reject unknown fork positions, and keep exact replay read-only
  after the parent advances.
* **Boundary**: Made Test UI runtime rebuild construct its exact turn and
  SubAgent authorities before tool registration, skill discovery, MCP startup,
  capability events, or provider construction; failed later setup closes any
  newly started MCP manager.
* **Storage**: Made Memory deletion reject every active lease purpose, including
  mutation leases, without changing the thread, lease, journal, or prepared
  compaction authority.
* **Effects**: Made the registry distinguish pre-dispatch tool errors from
  results that crossed effect authority. Authorized results now require an
  exact one-shot finalizer, and caller cancellation after dispatch begins
  cannot bypass atomic effect-result persistence.
* **Documentation**: Corrected restart guidance: `ResumeThread` is a read-only
  attachment to canonical state, while interrupted-turn recovery requires an
  exact lease-proof-bound `InterruptedTurnRecoveryHost`.
* **Storage**: Made artifact reads validate the complete live/tombstoned
  ancestry shape and exact composite record ownership. Cycles, broken ancestry,
  malformed bound parents, and map-key/record drift now return authority
  corruption instead of being hidden as a missing SubAgent or artifact.
* **Boundary**: Made Test UI canonical journal, context, SubAgent, and ID
  sequence reads fail closed. Running-session inspection no longer retains an
  older snapshot or restarts identifier allocation when Floret authority is
  unavailable.
* **Boundary**: Removed host URL assumptions and generic pre-journal writes from
  Floret-owned full tool output. `FinishEffectDispatch` now admits immutable
  payload plus its canonical result reference atomically, while exact root and
  parent-descendant read capabilities return content without a global resolver.
* **Storage**: Bound root fork v5 and SubAgent fork-copy publication to the exact
  on-path artifact reference/payload closure, with atomic same-ID destination
  copies, strict replay validation, and rollback on drift or collision.
* **Boundary**: Migrated the Test UI to public runtime capabilities for
  canonical create/read/title/turn/SubAgent/delete work. Persisted snapshots no
  longer hold raw repos or rebuild provider-request internals, and host artifact
  URLs resolve through exact bound read authority.
* **Storage**: Moved interrupted-turn run, terminal status, failure, and outcome
  derivation into the Memory critical section and SQLite immediate transaction.
  Recovery replay now revalidates the complete expired lease proof and exact
  parent authority against the canonical terminal ledger.
* **Boundary**: Removed the orphan public `tools` approval callback types after
  `EffectAuthorizationGate` became the sole host authorization boundary; test
  approval helpers now remain internal-only.
* **Boundary**: Removed production `AgentHarness` root creation and narrowed its
  repository to `JournalRepo`; only the exact create coordinator can publish a
  missing canonical root.
* **Boundary**: Made provider-free capability construction context-aware and
  fail before returning a handle when root/parent identity is missing, deleted,
  closed, or has the wrong root/child shape. Delete alone accepts its exact
  tombstoned root for replay.
* **Storage**: Made terminal turn outcome, provider continuation replacement or
  clearing, and exact lease release one `FinishTurn` transition. Incompatible
  continuation is no longer deleted as a construction-time cleanup side effect.
* **Storage**: Made root-tree deletion retain host-owned generic metadata and
  retain the authority ledgers needed to reject deleted identity/request reuse;
  removed alternate tree-delete entry points.
* **Storage**: Froze SQLite authority schema v14 with only exact empty v13
  migration, persisted lease-policy equality, typed non-destructive schema
  errors, and explicit File-backend rejection where semantic mutation atomicity
  is unavailable.
* **API**: Added typed `AuthorityBusyError`, `RequestConflictError`, and Store
  maintenance facts so hosts can branch without importing internals or parsing
  text.
* **Lifecycle**: Added Store lifetime fencing: close rejects new work, cancels
  active execution, waits for terminal finalization, and makes every retained
  binder, factory, handle, and create capability return `ErrStoreClosed`.
* **Boundary**: Required provider-backed host construction to validate its bound
  canonical root or parent before skill discovery, event emission, tool
  registration, or other provider initialization effects.
* **Boundary**: Added public runtime sentinels for active thread mutation,
  missing retry targets, pending-tool target state and settlement conflicts, and
  closed SubAgent writes. Downstream hosts can classify every branchable
  capability failure with `errors.Is` without importing internal packages.
* **Documentation**: Clarified that one-time bootstrap prevents a reusable
  cross-family issuer; responsibility-specific binders remain composition-root
  issuers for their single capability family.
* **Breaking**: Replaced reusable `NewHostBootstrap` with one-time
  `ConfigureHostCapabilities`. The Store rejects reconfiguration and value
  copies, all bootstrap copies share one sealed state, and failed or panicked
  configuration attempts leave every leaked binder inactive.
* **Boundary**: Added responsibility-specific binders for create, read, title,
  fork, delete, turn execution, compaction, SubAgent lifecycle/read/maintenance,
  and pending recovery. A retained binder cannot mint another capability family.
* **Breaking**: Removed thread authority from provider host options. Provider
  binders fix one root or parent before returning a configurable factory; root
  and SubAgent recovery use separate binder methods with no invalid mixed shape.
* **Boundary**: Fixed the host authority contract as an actor/capability matrix
  plus create, admit, settle, fork, delete, and recover transition ownership.
  Binders remain at the composition root; services and runs receive only an
  exact authority-bound factory or handle.

## 2026-07-19
* **Breaking**: Replaced the reusable `HostRuntime` constructor token with a
  composition-root-only `HostBootstrap`. Provider, SubAgent read/maintenance,
  and recovery settlement options no longer carry a root authority; exact
  factories issue only their corresponding bound capability.
* **Storage**: Separated ordinary fork lineage from SubAgent ownership.
  `ForkedFromThreadID` now records lineage while `ParentThreadID` is reserved
  for parent-owned child threads, whose ownership metadata is written in the
  same Memory lock or SQLite transaction as the fork destination.
* **Migration**: Added strict SQLite schema v12-to-v13 authority migration.
  Persisted replayable fork plans repair known operation boundaries; ambiguous
  parent metadata rolls back and rejects open instead of being guessed. The
  migration rewrites fork plans to the v2 atomic `destination_meta` contract and
  removes obsolete turn/run mapping rows from persisted fork results.
* **Breaking**: Removed the broad public `Host`, `NewHost`, and `HostOptions`
  surface. Provider-backed work now uses thread-bound `TurnExecutionHost`,
  thread-bound `ThreadCompactionHost`, and parent-bound `SubAgentHost`
  capabilities with tailored options.
* **Boundary**: Made explicit request identities fail when they do not match a
  provider capability's bound thread or parent, so long-lived runs and
  coordinators cannot operate on arbitrary canonical journals.
* **Boundary**: Made `PendingToolSettlementHost` the only public pending
  settlement surface. Active turn and SubAgent owners derive a settlement
  handle that shares their harness and fails rather than falling back after the
  owner becomes inactive; restart coordinators construct a handle
  bound to exactly one thread or parent. Bulk SubAgent maintenance is likewise
  bound to one canonical parent.
* **Boundary**: Serialized root create, ordinary fork, SubAgent spawn, and root
  tree delete through one Store-level authority mutation gate. Delete keeps the
  gate across parent validation, descendant discovery, and durable deletion so
  concurrent spawn cannot leave an orphan child.
* **Storage**: Changed SQLite thread-tree deletion to accept only a root identity
  and derive the validated current descendant set inside the immediate
  transaction. Multiple Store instances or processes cannot create a child in
  the runtime snapshot gap, and every v13 open rejects missing or cyclic parent
  authority.
* **Storage**: Made Memory fork publication all-or-nothing and FileRepo fork
  persistence publish through one atomic directory rename. Malformed todo state,
  incomplete fork data, and cyclic parent authority now fail closed.
* **Test**: Added exact method-set, options-surface, authority mismatch, and
  whole-package source guards for the narrowed public runtime API.
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
