# Changelog

## Unreleased

- Upgrade golang.org/x/net to v0.62.0 to fix reachable HTML parser and renderer
  vulnerabilities GO-2026-6860 and GO-2026-6866. Update its required x/sys and
  x/text dependencies without changing Floret APIs or conversation formats.

- Add optional host-authored tools.Definition.BatchExclusiveGroup to reject
  conflicting prepared calls before authorization while preserving independent
  calls, paired feedback, default behavior and existing resource permissions.

- Retain confirmed intermediate response membership across interaction continuations
  within one turn. Preserve run-local supersession, unknown partial output and
  bounded cold/warm window reads without rewriting historical conversations.

- Bind Goal tool composition, dispatch, paired-result handling and reflection to
  one Harness execution installation. Fix explicit Goal continuation without
  agent-wide opt-in. Declare canonical Goal writes accurately, reject host tool
  collisions, and retain ordinary tool authorization and historical journal shapes.

- Preserve complete tool diagnostics when adding an uncertain-effect warning.
  Apply payload limits before the engine warning, retaining it under head and
  tail policies and keeping artifact hashes and restart/retry facts consistent.

- Settle admitted execution cancellation before aborting shared history
  preparation during shutdown, preventing competing terminal commits while
  preserving queued inputs and joining preparation work.

- Admit cold SendAck through bounded current facts and defer complete history
  preparation to execution. Preserve request replay, queued inputs, cancellation,
  window continuity and complete legacy subscriptions. Reconstruct cold query
  continuations in bounded verified snapshots without new indexes or migrations.

- Retain bounded single-file mutation names and change types in Activity
  summaries using existing canonical array locations. Collapsed patch titles
  require no detail preparation, storage migration or new index writes.

- Add optional item presentation range reads and per-item window preview budgets.
  Preserve exact text, source identity, authorization and encoded response bounds.
- Defer large activity detail indexes to explicit field reads; ordinary startup
  no longer prepares unopened display histories. Keep the existing canonical
  schema and explicit full display preparation contract.
- Batch activity locator writes and avoid SQLite updates for identical values.


- Retain canonical Goal disposition in activity summaries while deferring Goal
  text and review evidence. Preserve complete view and restart semantics.

- Add optional `ThreadActivityBody.DefaultPresentation` for empty historical
  activity sources. Preserve ordered canonical merge semantics without requiring
  hosts to load a complete thread or synthesize engine-owned defaults. Source
  bytes, hashes, schema and complete-view contracts remain unchanged.

- Preserve selected-window priority when current sidebar summaries use bounded
  source reads. Summaries yield through the existing foreground read owner,
  honor cancellation, and resume after window publication without a new queue.

- Reuse source-validated parent-event recovery coverage in the existing query
  manifest. Prepared restarts check one immutable source per child instead of
  scanning every child journal. Enabled writes advance coverage atomically
  with source facts and receipts; disabled writes and copied forks retain the
  existing unresolved-interaction recovery path. No canonical schema changes.

- Automatically prepare missing browsing sources in the Host-owned upgrade
  worker, including retained branches. Resume bounded metadata batches without
  rewriting canonical history; prepared restart checks readiness anchors only.
  Add separate `ThreadReadiness.DisplayReady` / `DisplayError` and cancellable
  `Host.PrepareDisplayThread`, preserving published canonical readiness behavior
  and joining explicit work during shutdown.
- Read current summaries through exact transactional lifecycle source references
  and current queues. Preserve failure fallback/reset, cancellation, timeout,
  pending input and path semantics without replaying resolved controls.

- Add exclusive canonical-position bounds to parent receipt listing. Seek by
  existing path-bound receipt ranks, preserve event cursors and chronological
  results, and reject conflicting position selectors. No domain schema or
  report-body format changes. Optional leaf selection survives append, rejects
  removed branches, and reports the exact selected path in every page.
- Batch parent source references with the existing bounded projection write set
  instead of issuing one extra storage write per canonical record.

## v7.84.0 - 2026-10-24

- Add optional latest/backward receipt selection and an earlier-page cursor to
  existing parent-event contracts, retaining chronological results and default
  forward semantics. Hosts can scroll bounded metadata and disclose one report.

- Keep navigation over an accepted display leaf independent of complete execution
  recovery. Read another bounded interval outside the actor and discard it if
  publication advances before interest installation.
- Resolve parent receipt pages through transactional path ranks and canonical
  admission sources. Preserve disjoint admissions, retained branches, and exact
  interaction semantics without scanning unrelated parent or child turns.

- Add opt-in summaries on explicit window baselines. Freeze activity facts and
  preview with the accepted view, avoiding a second host preparation/read cycle.

- Expose optional source-bound whitespace extents on selected activity string
  locators. Compute exact decoded byte positions during canonical indexing and
  from frozen live sources, preserving original text, hashes and released reads.

- Prepare a missing preview locator independently when window membership is
  already current. Preserve all prepared membership and boundary records, and
  publish only after verifying the unchanged source owner.

- Add source-relative around-window seeks over visible membership ranks. Bind
  accepted results to stable item identities for observation, preserve failed
  interests, and avoid reading intervening hidden history.

- Retain the latest canonical preview source with transactional window metadata.
  Read its bounded text at the accepted leaf so tool-only and historical windows
  avoid a subsequent full summary scan; metadata-only readers retain zero body
  reads. Validate source authority, rollback and restart without storing previews.

- Allow atomic selected activity values to include up to eight frozen string
  locators from the same snapshot. Bound locator metadata independently and
  leave deferred canonical string bodies unread until explicitly requested.

- Add optional authorized atomic activity value reads with explicit byte/node
  budgets and direct-child selection. Canonical transactions and immutable live
  snapshots prevent mixing fields across concurrent replacements.
- Expose activity-local summary versions and bounded typed display attributes.
  Keep hidden payloads out of windows; live replacement clears stale retained
  summaries and unrelated thread growth does not invalidate activity details.

- Retain exact immediate activity field shapes and counts in summary windows.
  Historical summary readiness does not expose incomplete text locators, and
  public summary copies cannot mutate source-bound descriptor values.

- Preserve independent sparse reading interests and legacy activity display on
  canonical refresh. Retain valid selected identities after branch replacement,
  and preserve the requested anchor when the same actor prepares execution.
- Add opt-in UTF-8 start alignment to decoded activity field reads. Random
  reverse ranges need no retained prefix; strict offset behavior is unchanged.

- Correlate explicit window baselines on the existing observation channel with
  an optional bounded request token. Keep automatic replacements untagged and
  failed reads on the previous accepted interest; persist no additional state.

- Add optional authorized canonical item-range queries with source-bound counts
  and explicit safety facts; keep live drafts and product grouping outside this
  frozen read contract.
- Keep compaction anchors bound to immutable item identity when later tool
  results and approval facts update the same member. Preserve source, ordering,
  visibility, turn/run, and canonical-hash validation.

- Add opt-in finite window item selection with canonical ordering, explicit gap
  semantics, one existing observation interest and independently reachable pending
  controls. Preserve legacy omitted-body hydration for window replacements.

- Prepare the requested cold window directly and hydrate omitted source bodies
  outside the execution actor. Cancel superseded reads, preserve baseline/delta
  ordering and reuse frozen live prefixes while their source grows.
- Limit ordinary field-window invocation sources to effective canonical
  references, preserving complete control and interaction dependencies.

- Add opt-in activity field windows with bounded visible summaries and typed safety facts; preserve existing default window behavior.

- Preserve private window preparation across ordinary appends, catch up only
  new canonical sources, and publish matching readiness anchors atomically.
  Branch replacement and deletion still reject stale preparation.

- Read actor-resident activity fields through bounded frozen string sources. Keep
  detail reads independent of unrelated live growth and preserve source identity
  across replacement and completion; reauthorize every field read.

- Add optional effective activity structure paging with source references and bounded decoded field reads; share typed merge decisions and canonical historical defaults.

- Add optional decoded Activity field ranges with fresh authorization and
  immutable source/hash fences. Maintain escape-safe field locators in canonical
  write transactions, avoiding sibling payload reads for prepared sources.

- Resolve current queued inputs and restore sources without replaying consumed
  queue history. Maintain source membership atomically with canonical writes and
  preserve ordering, duplicate-add semantics, inactive branches and restart.

- Initialize window observers from transactionally maintained item membership
  and exact canonical dependencies, preserving late tool results, waiting
  controls, Retry, Fork, Goal, context and continuous actor versions. Complete
  readers promote the same actor only after reconstruction and catch-up succeed.
- Share private, bounded source preparation across narrow and complete window
  readers. Keep selected-source validation and published repository options on
  the same owner. Missing historical metadata still needs a full source scan;
  no subsecond first-upgrade or complete product acceptance is claimed.
- Reuse append-ordered source groups instead of rewriting complete Goal and turn
  source lists. Validate compaction source identity and item anchors, retain
  sidebar previews before an all-tool window, and deliver active recovery
  failures to affected observers and the Host error channel.

- Coalesce canonical body metadata and blocks into bounded SQLite reads within
  the existing page transaction and 1 MiB retention budget. Preserve exact
  source validation, transaction ownership, and the host storage SPI.
- Let sidebar summaries join selected-thread preparation and yield between
  metadata pages to foreground reads on the prepared repository. Cancellation
  releases only the waiting consumer; it does not restart shared preparation.
  Retain foreground priority through selected body reads and window publication.

- Decode resident canonical JSON directly with released v1 field semantics and
  strict unknown-field validation. Avoid nested payload and SQLite result copies,
  reuse Retry projection capacity and bounded hydration buffers, preallocate
  known page and attribution sizes, avoid per-field maps and ordered-context
  staging copies, and validate context measurements once.
  These allocation reductions do not turn cold display into an indexed read.

- Initialize the canonical item projection with an absent Goal before the first
  entry, so empty-thread full and bounded views agree without invalid Goal state.

- Reuse published thread readiness during current-store startup instead of
  revalidating every historical entry, compaction prefix and related thread.
  Preserve full migration and missing/stale metadata preparation, explicit
  inspection, requested-source integrity checks and pending parent-event
  recovery. Prepared restarts no longer rewrite matching query manifests.
- Share cold thread preparation across cancelled readers with two active workers
  per Host; shutdown and deletion cancel and join the owned preparation.
- Resolve managed-context request attribution by exact canonical source ordinal,
  lazily backfill released ledgers from the newest requests, and index sparse
  queue and parent-event sources transactionally without copying conversation state.
- Share paged historical bodies between timeline and checked context projection,
  including Retry selection. Validate successive same-path compaction checkpoints
  incrementally while preserving branch and Retry validation.
- Reuse validated duplicate body bytes within one canonical page transaction,
  with at most 1 MiB retained. Revalidate sources on later transactions and
  reject actor requests cancelled while waiting for the mutation boundary.
- Defer ordinary local tool presentation bodies during cold actor preparation.
  Reconstruct exact call/result presentations only for selected windows and
  complete reads; retain hosted-tool, Goal, control and provider-context semantics.
- Reuse one frozen, validated metadata traversal for cold context selection and
  timeline body pages. Selected bodies retain per-page verification and active
  ancestry fences; cancellation, deletion and branch changes cannot publish a
  partial actor. Temporary metadata residency remains proportional to history.
- Preserve bounded window baselines and observation versions. Missing-index
  preparation and explicit full initialization still traverse historical sources;
  no subsecond first-upgrade or complete product guarantee is introduced.
- Omit ordinary tool parameters and results from cold timeline reads when they
  are outside the exact checked-context selection. Preserve selected context,
  control-result equality validation, Goal inputs and tool presentation sources.
- Pause background migration between transactions while a foreground cold
  projection reads its pages. Reuse the existing migration scheduler; explicit
  preparation and deletion remain eligible, and background work resumes after
  the final reader settles, including failure, cancellation and panic.

## v7.83.0 - 2026-10-24

- Add reviewed Moonshot Kimi K3.1 Pro and Highspeed models. Render K3-family
  output budgets through capability-selected max_completion_tokens, preserve
  reasoning and tool history, and reject unsupported reasoning controls.
- Scope catalog refreshes by provider with exact source hashes and review dates;
  preserve unrelated inventories, host model choices and conversation identities.

## v7.82.2 - 2026-10-23

- Preserve storage and contract failure classification when overflow recovery
  fails, while retaining the original provider rejection as diagnostic evidence.

## v7.82.1 - 2026-10-23

- Summarize the final complete visual tool exchange when overflow leaves no
  older cut point. Consume every image through ordered byte-bounded batches,
  retain canonical facts and original media, and never replay tool actions.

## v7.82.0 - 2026-10-23

- Add explicit model-only image preparation and exact rendered request byte
  limits. Preserve original image identity, dimensions and transparent pixels;
  retain smaller source encodings and freeze prepared bytes for retries.
- Recover preparation-time capacity errors through the existing compaction
  boundary. Count encoded media in summary batches, preserve ordered coverage,
  and stop when an indivisible image cannot fit.
- Preserve the original provider cause through overflow recovery and exhausted
  retries. Never split the last complete tool exchange after a checkpoint.
  Existing canonical histories and domain schema remain unchanged.

## v7.81.0 - 2026-10-23

- Add optional canonical output start/end timestamps to runtime thread items.
  Preserve thinking and assistant receipt intervals through completion,
  cancellation, cold history, restart and Fork without inventing historical
  timings or counting delivery delay after the final chunk.

## v7.80.2 - 2026-10-23

- Keep canonical media-retention reads in bounded transactions and
  reject concurrent changes before returning an inventory. Do not hydrate
  complete history bodies or block foreground reads for a complete sweep;
  preserve streamed canonical checksum verification before media deletion.
- Read contiguous canonical path windows with exact bounded ranges, preserving
  branch ancestry validation. Reuse SQLite read statements within a transaction
  and classify interrupted canonical reads as cancellation rather than corruption.

- Return pending child terminal events from bounded waits only after the matching
  actor installs its complete terminal view. Preserve passive event admission,
  cancellation, attention priority and the original elapsed timeout.

## v7.80.1 - 2026-10-22

- Read cancellation requests through canonical control locators so Stop,
  shutdown and execution cancellation do not hydrate conversation bodies during
  bounded settlement. Preserve the original request identity and provenance.

## v7.80.0 - 2026-10-22

- Validate final-response boundaries from the current canonical turn, including
  resumed runs, without rereading unrelated history during bounded finalization.

- Validate every legacy admission collection before filtering thread closures,
  including malformed orphan records and orphan terminal proofs.

- Preserve released fallback-title repair, reject incomplete source identity
  directories before cleanup, and retain failed snapshots unchanged. Resume
  identity-directory batches after write failure, cancellation, panic or restart.

- Add bounded immutable-source indexes for released snapshot recovery frames
  and root inventories. Match released checksum serialization, validate each
  intermediate authority state, and preserve atomic locator/checkpoint rollback.
  Connect these primitives to the production startup worker with resumable
  identity-directory publication and bounded source cleanup.

- Bound resident query continuations; reconstruct cold histories outside write
  transactions and validate their snapshot before mutation. Traverse canonical
  history in fixed windows without repeated per-entry ancestry searches.

- Preserve the published comparability of `runtime.Host`; reject newer migration
  fields in historical manifests. Verify and reclaim query metadata in bounded
  transactions while detecting concurrent canonical changes before publication.

- Add optional bounded record reads and source-bound resumable indexing primitives
  for historical snapshot migration; retain exact released UTF-8 repairs.
  Monolithic v2–v5 stores reserve historical identities before admitting new
  threads, then publish independently through the canonical owner.

- Add `Host.ThreadMediaInventory` so hosts can retain canonical media by thread
  without collecting descriptors for every thread in one in-memory snapshot.

- Return `runtime.Host` after bounded storage inspection and run domain
  migration and final verification in a Host-owned worker. Add migration status,
  thread readiness and cancellable preparation APIs; v6–v31 sources
  publish each validated thread atomically with resumable body checkpoints.

- Treat SQLite v1 records with `WITHOUT ROWID` as a supported physical layout
  and remove the v1 full-table copy. Physical format upgrades preserve canonical
  record bytes.

## v7.79.0 - 2026-10-22

- Remove full-text token generation, SQLite postings, incremental term append,
  and optional physical search dispatch. Explicit authorized text queries match
  canonical sources on demand; metadata browsing loads only the requested page.
- Append SQLite physical 4 -> 5 and logical storage 9 -> 10. Delete only derived
  search state, preserve canonical bytes and every historical migration edge,
  and keep retirement, metadata preparation and verification atomic.
- Deprecate the optional index SPI and startup indexing phase without removing
  published v7 symbols. Startup and canonical writes never construct text indexes.

- Read current-turn provider configuration once per Engine execution and advance
  its bounded facts only after successful request checkpoints. Use exact latest
  request reads for ordinary lineage and surface comparisons, preserving older
  matching render checks, model drift, retries, restarts and dynamic tools.
  This request-read optimization adds no public API or durable schema.

## v7.78.2 - 2026-10-22

- Limit model-facing `ask_user` response modes to `write` and `select_or_write`.
  Normalize legacy `select` requests and saved pending interactions to
  `select_or_write` with `choices_exhaustive=false`, preserving a writeable
  answer without rewriting historical records. Keep ordinary tool
  `Result.InputRequired` selection semantics unchanged.

## v7.78.1 - 2026-10-21

- Settle ordinary turns from the existing ordered turn index instead of
  materializing the complete canonical path. Keep the full path validation for
  Goal-guarded completion and preserve pending interaction, effect, compaction,
  steering, cancellation and replay semantics; no public API or schema change.

## v7.78.0 - 2026-10-21

- Skip path-reference regular expressions for text with no possible path
  separator. Preserve all path sanitization and URL handling while avoiding
  repeated Unicode regexp scans over giant ordinary output.

- Add opt-in deterministic dense-tool SQLite qualification fixtures at 1x,
  10x and 100x history, with journal, public item and database hashes; validate
  restart and continuation through public runtime APIs.

- Require Go 1.27.3, including the standard-library certificate verification
  correction for GO-2026-6809 reported by the release vulnerability gate.

- Add optional `ThreadItemBodyReader.ItemBody` to resolve current canonical or
  live text independently of window preview size, without reading body bytes or
  opening another observation subscription. Preserve fresh scope authorization,
  active-path membership and explicit unavailable-source errors.

## v7.77.1 - 2026-10-21

- Initialize the existing actor through its bounded acknowledgment read before
  a first Goal mutation. Preserve thread identity and idle lifecycle when
  CreateAck or a restart precedes SetGoal/DeleteGoal without a View read.

## v7.77.0 - 2026-10-21

- Bound ordinary live-text coalescing to 25ms while preserving immediate
  semantic boundaries and the existing byte limit.

- Resolve send request identities through the existing canonical control index.
  Preserve last-started replay semantics, transactional rollback and restart
  rebuilds without scanning unrelated historical entries.

- Resolve stable admission and terminal identities through the existing exact
  canonical entry index. Reject corrupt locators without fallback scans and
  keep accepted-turn reads independent of unrelated history size.

- Resolve confirmed tool results from exact canonical query-index source pairs
  instead of scanning the entire active path. Validate execution identity,
  current path membership and canonical integrity; return detached source facts.

- Keep bounded control-output locators at their exact raw canonical byte extent,
  including large whitespace-only output. Reconstruct trimmed legacy display
  text from the same source instead of retaining the transformed full body.

## v7.76.0 - 2026-10-20

- Add optional ThreadFactsSource for in-process cleanup observers through the
  existing actor publication owner, without historical body materialization.

## v7.75.0 - 2026-10-20

- Drop duplicate private source-membership arrays from retained actor items and
  metadata-only reducers. Complete adapters preserve their exact source mapping;
  bounded reads continue to use canonical manifests without prefix-sized copies.

- Replace growing query-source lists with fixed canonical boundaries and add
  optional atomic index-document append capability. Preserve cross-segment Unicode
  grams while reading at most two short source tails for merged assistant appends.
  SQLite physical format 3 -> 4 transactionally replaces derived FTS documents
  with unique indexed term postings; all canonical bytes and earlier migration
  edges remain intact. Published custom index replacement contracts remain valid.

- Stream canonical source chunks through the shared Unicode search-term collector
  during derived-index rebuilds. Keep exact separators, references, attachments
  and hosted presentation order without retaining a merged document body or
  loading private reasoning; cancellation returns no partial terms.

- Verify current canonical entries individually before retaining body locators
  during startup. Reuse ancestry for metadata-only Goal/parent-event validation,
  compare reasoning source hashes, and validate query-index correspondence with
  the same canonical projector without retaining transcript bodies. Rebuild
  postings from explicit per-document source reads. Historical migration inputs,
  compaction validation and public complete reads keep their complete semantics;
  metadata and complete checkpoints remain
  proportional to the requested history.

- Allocate one aggregate window preview source budget before reading or cloning
  text and completed activity. Preserve exact UTF-8 prefixes and explicit full
  body locators; keep the encoded metadata budget and stable-anchor reduction.

- Stream cold canonical summaries through frozen metadata pages instead of
  decoding complete historical bodies. Share one chronological summary reducer
  with complete adapters, preserve waiting-input admission and historical
  interaction order, and reject failed/cancelled/path-invalidated reads without
  partial state. The metadata scan and lifecycle/interaction maps remain
  proportional to history; this is not bounded cold-summary indexing.

- Add optional frozen live-body search with bounded UTF-8 pages, fresh authorization and explicit source expiry; share canonical search overlap and progress logic.

- Release canonical display reducers, window membership and interaction indexes with deleted actors after canonical commit; preserve all indexes when deletion rolls back.

- Share active text with the Engine attempt owner instead of retaining a second
  actor string. Bound aggregate draft source memory to 4 MiB and spill larger
  bytes to private disposable files. Preserve accepted prefixes on write failure,
  existing display path redaction, graceful cancellation and canonical restart
  bytes. Add optional freshly scoped ThreadLiveBodyReader with frozen run/source
  identity, bounded UTF-8 chunks and explicit expiry. Complete commit
  materialization and actor metadata residency remain unbounded.
  Exhausted window previews advance through explicit metadata-only body deltas
  instead of rereading and retransmitting their unchanged 64 KiB prefix.

- Hydrate cold actors through cancellable frozen canonical pages, releasing
  accepted text and completed ordinary activity after each page. Restore queue
  and snapshot inputs through one body-free physical inventory, preserving
  inactive branches and exact historical removal semantics. Context-dependent
  paths keep complete canonical validation; retained metadata remains unbounded.

- Remove quadratic mutation-batch lookup and use existing turn-marker locators
  for ordinary record validation, including historical turns without a start
  marker. Validate typed-turn admission against all prior members and preserve
  transactional rejection of invalid context-only and Goal continuation inputs.

- Install ordinary execution completion from frozen appended canonical intervals
  outside the actor lock, reconcile only observed presentation overlays, and
  retain SessionLifecycle failure/cancellation precedence. Maintain terminal
  presentation presence by affected item instead of scanning historical turns.
  Explicit complete reads and unsupported custom-store adapters retain their
  established semantics; bounded cold initialization remains unfinished.

- Read existing actor summaries before canonical history, remove root inventory
  materialization from list refreshes, and maintain preview/time positions by
  explicit item changes. Unloaded summaries still use canonical projection.

- Append the atomic domain 30 -> 31 migration for typed activity body blocks;
  preserve complete API results, cancellation, unknown-effect settlement and
  exact restart bytes while releasing completed ordinary tool presentations.
- Add explicit ActivityBodies locators and ThreadBodyActivityJSON for oversized
  bounded-window presentations; partial JSON never substitutes a complete body.

- Release accepted ordinary text from actor and incremental reducers, retaining exact source extents and bounded summaries; restore explicit full views and freeze complete legacy publications before enqueueing. Preserve UTF-8 prefixes, citation offsets, summary whitespace, restart and deletion semantics. Metadata residency and bounded initialization remain unfinished.

- Add optional scoped item-window observation through the existing actor/version channel, with explicit replacement baselines, stable anchors, pending controls, selected turn membership and in-place tail eviction; serialize queue reads and clears to prevent concurrent deadlocks. Actor residency and complete production performance qualification remain unfinished.

- Advance ordinary send and queued admission through appended canonical intervals; borrow immutable turn membership inside the actor and copy it only at public snapshot boundaries.

- Segment canonical control output alongside other body fields; preserve exact raw whitespace, empty-source presentation, restart integrity and nested-message ownership.

- Add optional scoped ThreadBodyManifestReader for bounded, frozen canonical output-source parts; preserve segmented content, deduplicate reasoning and authorize every page without materializing a ThreadView.

- Validate repeated canonical output sources from immutable identity and reasoning hashes without hydrating old fields or reading inactive sibling bodies.

- Maintain actor interaction positions, item associations and attention counts by affected control; rebuild only at full replacement and preserve answered controls against stale canonical requests.

- Resolve exact active-path canonical body metadata without reading source blocks; share integrity checks with chunk reads.

- Advance canonical provider/context commits by appended storage intervals and affected item/turn reducers; share full-read semantics, preserve delayed interaction order, invalidate failed projections and avoid retaining historical mutation-map capacity.
- Read the latest scope-bound provider request by exact append ordinal with restart, pending-observation and source-integrity checks.

- Read bidirectional body-free canonical windows by stable anchors and frozen active-path boundaries, retaining append validity and rejecting changed branches without whole-path materialization.

- Advance canonical item presentation through shared chronological reducers for Goal, controls, steering, tool inputs, approvals and hosted activity; preserve original output and arbitrary chunk boundaries.
- Add internal body-free canonical metadata paging through the existing ancestry/cursor kernel without hydrating historical source fields.

- Add optional scoped ThreadBodySearcher with bounded exact UTF-8 matching, cross-page overlap, source-pinned continuation and cancellation.

- Add optional freshly scoped ThreadBodyReader for exact current-path canonical fields with 16 KiB source chunks, encoded response budgets, complete-field hashes and restart-stable identity; no full-entry materialization.

- Evict extracted historical bodies from retained canonical journals, hydrate requested entries from their backend snapshot, and bound the shared verified source cache to 128 MiB.
- Settle early execution cancellation before publishing idle; replay the exact canonical terminal after concurrent effect settlement, including its paired failure and safety classification.

- Append the transactional domain 29 -> 30 migration for shared canonical body blocks; preserve complete message bytes, raw hashes, Fork ownership, deletion, fail-closed validation and current restart idempotency.

- Stream startup record decoding by storage page, generate Unicode search terms without full-field copies or per-character encoded allocations, and assemble shared assistant sources without quadratic text copying or citation-offset rescans.
- Add optional ThreadCommandService acknowledgments using existing command admission and idempotency without materializing complete results.
- Add optional scoped IndexDocumentReader for exact derived-document lookup while preserving custom IndexReader contracts.
- Page canonical ancestry through derived locators; remove historical journal copies from append rollback and projected-body cache rebuilds from ordinary index updates.
- Index turn markers and output-source ancestry without scanning complete history for each append; preserve branch-local source validation and transactional rollback.
- Resolve active turns, canonical run identity and cancellation from canonical control locators on provider commits; preserve rollback, restart and migration validation semantics.
- Bound native observation queues by encoded bytes as well as count and release queued payloads on every terminal path.

## v7.74.1 - 2026-10-20

- Release deleted thread actors' presentation, draft buffers, indexes, request caches and execution references only after canonical deletion and execution/effect drains. Preserve permanent identity fences, exact deletion replay and failure rollback.

## v7.74.0 - 2026-10-20

- Add optional CompactContextInput.ContinueGoal and TurnKindGoalContinuation: atomically admit explicit intent, prepare compaction, and continue the canonical Goal through the existing Engine and review loop without fabricated user input.
- Retain pure-compaction defaults and exact replay; preserve Stop, ordinary Retry, questions, approvals, canonical preparation and confirmed tools across restart. Settle recovery preparation failures instead of leaving active executions stranded.
- Append transactional domain schema 28 -> 29 without rewriting historical facts; share current validation while retaining every released migration and old-schema constraints.

## v7.73.1 - 2026-10-20

- Classify compaction request timeouts and recovery exhaustion as Provider failures while the execution context remains active; share cancellation authority across lifecycle, diagnostics and turn settlement.
- Preserve typed timeout details, frozen retries, original context and continuation across failed compaction and restart; keep explicit Stop and atomic installation authoritative.

## v7.73.0 - 2026-10-20

- Add optional typed provider transport observations with Engine-issued attempt identity and memory-only ThreadRunProgress snapshots, preserving semantic timeouts and canonical history.
- Refresh retry measurement prefixes from fresh prepared handles while retaining frozen model input, payload fingerprints and estimates.

## v7.72.0 - 2026-10-20

- Reset tool argument correction after each fully committed validation-clean batch, sharing one bounded episode across ordinary and control tools while retaining valid sibling results.
- Add ThreadTurnFailureToolArgumentCorrectionExhausted for safe live and historical failure projection without changing failure codes, storage schemas or published Engine error formats.
- Qualify exact published v7.69.1 synthetic failures through Retry, Send, Fork and restart, including schema 28 upgrade and confirmed tool-result preservation.

## v7.71.0 - 2026-10-19

- Add optional actor-native ThreadUpdateSource with explicit item mutations, component clears, UTF-8 append offsets, continuous view versions, bounded growth coalescing, and explicit baseline invalidation.
- Share one publication kernel with existing View and Subscribe APIs; materialize legacy views only on demand and use lightweight wakeups for internal waits.
- Accumulate Engine and thread output without repeatedly copying growing text; index native fragments and live items and publish only changed Activity facts through one reducer.
- Preserve canonical settlement, historical schemas, attempt fences, approvals, cancellation, and released v7 interfaces; reject late facts that would reopen terminal tools.

## v7.70.0 - 2026-10-19

- Add optional attempt-bound native input/response measurement and frozen protocol-prefix mapping; estimate only unmatched suffix input, including tools, media and context updates, without reapplying whole-history margin.
- Separate native total qualification from content attribution; require explicit encrypted-reasoning coverage and preserve protocol usage scope, cache and reasoning semantics.
- Evaluate automatic pressure once at frozen request preparation with the existing 90% threshold; remove post-response pending triggers, share output budgeting, and bound no-op, cancellation and overflow recovery.
- Append transactional domain schema 27 -> 28 without rewriting canonical history or native usage; revalidate calibration after restart, Retry and Fork and retain every historical migration.

## v7.69.1 - 2026-10-19

- Scope Ask User guidance to required execution input and requested guided questions; allow ordinary discussion and optional feedback in prose without changing schemas or interaction lifecycle.

## v7.69.0 - 2026-10-19

- Add tools.InputPropertyOrder and provider.OrderToolSchemas for deterministic required-first root property rendering without changing validation semantics.
- Share final request and raw tool segment rendering across official adapters; estimate and retry frozen bytes, preserving nested schemas and hosted tools.
- Advance tool-schema render lineage once at the existing provider-surface boundary, retaining canonical history, native history and domain schema 27.

## v7.68.4 - 2026-10-19

- Clarify compact Goal guidance, preserve existing unfinished objectives, and distinguish new logical requests from frozen transport retries.
- Explain that schema-invalid calls did not execute and that the model rewrites arguments within runtime-owned correction limits; preserve canonical feedback and recovery boundaries.

## v7.68.3 - 2026-10-19

- Apply committed journal identities consistently to Engine output, effect results, validation feedback and continuation; remove duplicate event-driven persistence.
- Share strict provider-context projection across live execution, restart, Retry, Fork and compaction. Install the full canonical replacement context and reject invalid boundaries without guessing or replaying tools.
- Append atomic domain schema 26 -> 27 for proven historical compaction boundaries, including released Fork and nested Fork copies; preserve conversation facts and every earlier migration, with full rollback and byte-stable restart.
- Upgrade provider-context lineage to v11 without rewriting historical request, response or usage records; invalidate incompatible continuation at the existing explicit reset boundary.
- Require immutable released-writer historical conversation continuation and restart checks in both candidate and published release adoption gates; retain the exact pinned Go toolchain when isolating downstream module caches.

## v7.68.2 - 2026-10-19

- Give the independent-compaction cancellation fixture a bounded five-second preparation guard under race instrumentation; keep production cancellation deadlines unchanged.


## v7.68.1 - 2026-10-19

- Keep waiting-run finalization under its original owner, then atomically admit the continuation marker before publishing the successor Run so Stop cannot strand an answered thread during preparation.
- Remove duplicate continuation admission from AgentHarness; preserve one canonical cancellation path, accepted answers and valid tool history.

## v7.68.0 - 2026-10-19

- Publish typed frozen compaction trigger and validated replacement diagnostics through the existing canonical lifecycle, preserving estimator scope and calibration identity without a second ledger.
- Append automatic domain schema 25 -> 26, preserving historical payloads and missing evidence while rejecting older writers; settle interrupted compaction observations from canonical terminal turns.
- Preserve decision isolation across observation, current views, reconnect, fork and restart without changing token measurement, compaction thresholds or retry limits.

## v7.67.1 - 2026-10-19

- Install admitted managed context and canonical item positions before publishing an accepted execution, including promoted queued inputs, while keeping execution settlement independent of display projection failures.

## v7.67.0 - 2026-10-19

- Add complete actor-owned context snapshots to current thread views and subscriptions; share the canonical projection with Context reads and publish committed context changes during active execution.
- Expose stable first-lifecycle canonical compaction ordinals while preserving insertion anchors and execution identities across terminal updates, forks and restart.
- Preserve snapshot isolation and streaming reconciliation; retry failed canonical reads without blocking execution settlement or cancellation. Recover an admitted run's context policy before its first provider request.

## v7.66.1 - 2026-10-19

- Align the native DeepSeek Search count schema with its accepted 1–50 range and preserve Brave descriptions in model-visible results.

## v7.66.0 - 2026-10-19

- Add shared Brave/DeepSeek Search clients and native tools, explicit Markdown DeepSeek Fetch, bounded authenticated transport and factual Activity.
- Publish reviewed Flash 1017/Pro search capabilities; preserve aliases, Responses replay and Direct security. Remove unused internal search policy and duplicate Brave HTTP transport.

## v7.65.1 - 2026-10-19

- Stop counting encrypted and redacted reasoning transport bytes as input tokens; preserve opaque diagnostics, partial attribution, conservative unknown/media bounds, and bounded overflow recovery.
- Version rendered request estimates independently of unchanged breakdowns so prior pressure anchors expire without rewriting canonical history.

## v7.65.0 - 2026-10-19

- Add optional native provider output parts and unify Engine response fragment ownership, atomic canonical commit and memory-only view takeover.
- Preserve schema-correction reasoning in original order without exposing invalid tool calls; remove visible-count identities, text-based deduplication and live overrides of canonical text.
- Append domain schema 24 -> 25 with explicit canonical source references, stable historical item IDs and transactional rollback; retain all released migrations and byte-stable current startup.
- Preserve DeepSeek native positions and terminal suffix identity, and adapt Chat Completions request rendering to canonical fragments.
- Preserve partial output sources on Stop without authorizing calls or provider-state advancement.
- Preserve exact activity navigation paths and URIs through tool execution, canonical history, live observations, forks, and restart, without rewriting existing records.
- Redact nested structured-row and patch-mutation navigation paths in diagnostic traces without mutating canonical presentation.

## v7.64.0 - 2026-10-18

- Add the typed SubAgentOperationStatus action for metadata-only child checks, preserving complete inspect and wait result contracts.
- Guide parent event consumers to reuse sufficient reports and read final child results separately from completion notifications, without changing durable history or event delivery.

## v7.63.1 - 2026-10-18

- Preserve historical Goal review evidence after forks re-key journal entries by matching the confirmed invocation span.

## v7.63.0 - 2026-10-18

- Expose detached typed Goal invocation presentation with exact objective text and journal-confirmed review evidence, without equating a completed review with committed completion.
- Restore historical Goal tool presentation from its canonical invocation and review at the original execution position without rewriting history.

## v7.62.3 - 2026-10-18

- Reconcile live text and newly persisted Goal facts by stable item identity, retaining canonical Path positions and rejecting stale durable snapshots.
- Serialize user Goal mutations with canonical view installation so edits and deletion remain immediately visible during streaming without dropping text or starting extra execution.

## v7.62.2 - 2026-10-18

- Install the canonical admitted item prefix when Goal submission starts execution, preserving immediate Goal edits, deletion and timeline ordering during the active Turn.

## v7.62.1 - 2026-10-18

- Publish GoalSubmissionRequestKey for product execution authorization before admission. Preserve the existing canonical identity across execution, exact retry and restart.

## v7.62.0 - 2026-10-18

- Add optional ThreadGoalSubmitter for atomic user Goal submission and idle-only execution admission. Preserve SetGoal and DeleteGoal as save-only controls.
- Preserve active questions, approvals and execution without queuing another Turn; deduplicate the original admission across Stop and restart, and retain Goals on normal recoverable execution failures.
- Reuse canonical Goal and ordinary user Turn records without changing domain schema 24.

## v7.61.0 - 2026-10-18

- Add canonical Path ordinals to thread items and parent events, retaining original row positions across tool updates, history and forks independently of clocks.
- Expose confirmed intermediate assistant item references from segmented canonical responses while preserving final segments, length continuations, trailing partial answers and unknown legacy prose.
- Preserve every copied Fork record occurrence time uniformly. All new facts are read projections; domain schema remains 24 and historical bytes are unchanged.

## v7.60.3 - 2026-10-18

- Add public event-contract regression coverage for output-only usage and explicit zero input, including rejection of inverted confirmed/estimate placement.


## v7.60.2 - 2026-10-18

- Keep Goal review replay idempotent and accept typed Goal facts after canonical retry checkpoints. Preserve frozen transport inputs when the Goal changes during a request.
- Validate output-only provider usage as an estimate, matching the canonical accounting projection and preserving length continuation.

## v7.60.1 - 2026-10-18

- Serialize Goal timeline facts before the first Turn without inventing execution identity. Preserve mandatory populated run identity on executable items.

## v7.60.0 - 2026-10-18

- Add frozen Unicode code point character attribution alongside compatible token budget contracts, with one collector and one reconciliation entry. Keep native input overview independent of visible content distribution.
- Add explicit native input presence and measurement scope. Reject unqualified hosted, opaque and historical pressure anchors while preserving bounded compaction, cancellation and frozen retries.
- Append transactional domain schema 23 -> 24, preserving canonical message, checkpoint and historical status bytes and every released migration edge.

## v7.59.0 - 2026-10-18

- Add opt-in canonical thread Goals with typed user management, model maintenance, version conflicts and deduplicated journal mutations independent of todos.
- Reflect in the executing model context before natural completion, validate evidence against current work and atomically commit completion. Preserve next-request timing, frozen retries, exact compacted text, Fork isolation and bounded recovery.
- Append atomic domain schema 22 -> 23 without rewriting existing conversation facts; retain every released migration.

## v7.58.0 - 2026-10-18

- Share provider transport classification through RetryAdviceForError and recover ordinary wrapped EOF using the existing bounded Engine retry owner. Preserve explicit advice, cancellation and non-provider failure boundaries.



## v7.57.0 - 2026-10-18

- Add optional ParentEvent.ParentTurnID derived from the most recent ordinary turn in canonical path order, including late delivery, forks and restart.
- Keep stored receipts, admission, compaction and domain schema 22 unchanged; presentation remains host-owned.

## v7.56.0 - 2026-10-18

- Add explicit query-thread identity to parent event pages, including empty pages and fork-inherited reports, without changing historical event recipients or canonical bytes.
- Verify nested fork isolation, fixed-range reads, passive waits, subscriptions, clones, restart and published downstream adoption. Domain schema remains 22.

## v7.55.1 - 2026-10-18

- Never select a retry savepoint that precedes a returned uncertain effect, including interruption between result commit and batch savepoint. Preserve ordinary completed regeneration.
- Verify returned uncertainty settles under Stop without requesting another model response.

## v7.55.0 - 2026-10-18

- Add request-bound context reconciliation, attribution coverage and opaque byte diagnostics. Preserve native totals and raw category estimates; expose unattributed input without proportional scaling.
- Separate ciphertext/media budget proxies from token categories and mark rendered estimates approximate. Retain anchored pressure, bounded compaction and frozen retries.
- Add explicit output/reasoning inclusion and native-total presence with one usage arithmetic owner. Preserve published output semantics and historical totals.
- Append transactional domain schema 21 -> 22 with unchanged canonical entry bytes and all historical migrations.


## v7.54.0 - 2026-10-18

- Add explicit returned effect uncertainty: commit failed tool facts and continue model reasoning without replay or mandatory input. Keep unsettled dispatches terminal.
- Persist the outcome through atomic result settlement, replay and readback, and retain post-result retry boundaries for turns containing uncertainty.
- Append domain schema 20 -> 21, preserve historical facts and all migration edges, and reject upgraded stores in older writers.

## v7.53.0 - 2026-10-18

- Add independent OutputPolicy.ArchiveAboveBytes archival through existing atomic result/artifact settlement without falsely marking complete output truncated.
- Allow confirmed completed error results to request user input while preserving error status, batch settlement, Stop and restart behavior without tool replay.
- Append domain schema 19 -> 20 with strict historical validation, unchanged canonical facts and rejection by older writers. Preserve every released migration and protected public API.

## v7.52.1 - 2026-10-18

- Preserve empty parent event arrays through public thread snapshot cloning and JSON, while retaining disabled-event omission and detached nonempty pages. No public API or domain schema changes.

## v7.52.0 - 2026-10-17

- Persist canonical query source locators and contentless SQLite FTS5 postings in canonical transactions; reuse them on restart and remove the full-database Go inverted index and position maps.
- Add optional transaction-local physical index capabilities, lightweight ThreadSummaryReader and counted startup index progress without changing existing required interfaces.
- Append SQLite physical 2 -> 3 and logical storage 8 -> 9 while retaining domain schema 19 and all historical migration edges.

## v7.51.2 - 2026-10-17

- Classify the exact tool-argument stall protocol code as a recoverable stream interruption, including HTTP 200 errors, through the existing bounded Engine retry loop. Preserve frozen requests, confirmed tools, Stop, permanent failures and explicit retry prohibitions without changing public API shapes or schema 19.

## v7.51.1 - 2026-10-17

- Respect server-owned output control for visual summaries; reserve frozen output refits only where the provider supports client output limits.

## v7.51.0 - 2026-10-17

- Add stable MessageAttachment.Locator descriptor identifiers and project confirmed tool attachments through canonical ThreadItem views and scoped history search/read.
- Include native user and tool images in one lossless ordered summary batch pipeline; enforce complete rendered budgets and frozen transport retry/output refit without partial checkpoints.
- Preserve attachment labels in checkpoints, require original-image rereads for exact details, and advance context projection to v10 without changing schema 19 or rewriting old checkpoints.

## v7.50.0 - 2026-10-16

- Add optional scoped ThreadWaiter with frozen targets, explicit missing/attention/completion/timeout outcomes, deletion wakeups, and distinct cancellation/Host shutdown errors.
- Unify batch repeat evaluation with dispatcher-measured bounded waits; preserve polling progress and mixed-effect protections.
- Derive typed loop failure reasons consistently from canonical live and historical facts without rewriting storage or changing schema 19.

## v7.49.0 - 2026-10-16

- Add opt-in passive parent events through the optional runtime service, canonical authorized child reports, critical source references, explicit full reads and pagination. Preserve legacy host model inputs by default.
- Admit one bounded typed event message only at natural provider boundaries after tool pairs; retain normal handoffs, interaction authority, retry, fork and compaction semantics without waking idle parents.
- Append the atomic domain 18 -> 19 migration, retaining old facts and every historical edge; bootstrap only unresolved interactions when enabled.

## v7.48.1 - 2026-10-16

- Bound automatic summary and title output by the opted-in model capability before provider dispatch. Preserve legacy policies and explicit title limits, and remove the need for downstream silent clamping.

## v7.48.0 - 2026-10-16

- Add opt-in model-capability output budgeting with one shared solver, 90% automatic compaction and a bounded safety margin. Preserve explicit output limits and all legacy v7 policy semantics.
- Declare client/server output control and refit prepared requests from frozen input without repeated attachment reads, fact collection or authorization. Retain final wire fingerprints and exact transport retries.
- Expose request output budget facts separately from input pressure, and append the atomic domain 17 -> 18 migration without changing historical facts or removing any migration.

## v7.47.1 - 2026-10-16

- Preserve transient canonical managed-context source intervals through both new and reused prompt-cache plans. Derive attribution from the canonical projection without adding durable metadata or changing request bytes. Retain those intervals across frozen network retries and compare detached estimate details by value.

## v7.47.0 - 2026-10-16

- Unify prepared-request budget and disjoint content estimation across provider protocols. Count decoded text once, select vocabularies from actual provider/model identity, retain separate protocol framing and conservative reserves, and remove the independent message breakdown path.
- Add optional typed message/rendered-path attribution and prepared TokenEstimate.Breakdown. Expose tokenizer identity, display quality and sanitized attribution diagnostics without changing wire payloads, canonical schema 17 or historical records. Unknown vocabularies and opaque/media input support relative shares only; optional attribution failure leaves valid budgets executable.
- Preserve fixed DeepSeek Flash baseline evidence and deterministic protocol, source partition, retry and lifecycle regressions.

## v7.46.1 - 2026-10-15

- Preserve the canonical completion marker when forking from the last final assistant item. Earlier item anchors keep their exact boundary; historical answer forks retain completion and final-response metadata without later turns.

## v7.46.0 - 2026-10-15

- Add optional canonical assistant-item Fork anchors for host answer actions. Resolve active-path presentation IDs inside the runtime; reject unknown, foreign and conflicting anchors without copying or exposing journal state. Preserve request-key replay and schema 17.

## v7.45.0 - 2026-10-15

- Add compatible request-scoped token attribution by exact tool name, registry source/namespace and canonical managed-context section key. Preserve existing aggregate fields, comparability, native usage authority, prompt rendering, schema 17 and historical records; keep product source grouping in hosts.

## v7.44.1 - 2026-10-15

- Preserve URL-only Web Search sources through validation, sanitized observation and canonical history. Source titles and snippets remain optional provider facts; no title synthesis, public API shape or domain schema change.

## v7.44.0 - 2026-10-15

- Add optional request-scoped used-context composition estimates through canonical reads and live observation: instructions, conversation, tools, retained outputs, managed facts, summaries, reasoning and expanded media. Keep native totals authoritative, exclude discarded artifacts and supplemental secrets, and preserve schema 17 and every migration edge.
- Expose the media/file subset of prepared token estimates without changing request budgets or provider usage.

## v7.43.0 - 2026-10-15

- Expose optional Ask User reason, required-input and evidence references through one detached canonical input presentation across subscriptions, current views, summaries, history and restart. References never grant access or permission. Preserve existing tool-requested input, answer semantics, historical records and domain schema 17.

## v7.42.0 - 2026-10-15

- Add validated Agent physical-request deadlines shared by normal requests, compaction and titles. Bound first output, semantic output pauses, hosted-tool waits and total attempt duration while preserving the existing frozen recovery budget and execution cancellation authority.
- Add typed timeout errors, detached last-progress timestamps and optional canonical terminal timeout diagnostics surviving restart. Preserve all v7 contracts, schema 17, historical migrations, storage-failure priority and safe tool retry boundaries.

## v7.41.0 - 2026-10-15

- Add optional per-row and per-mutation Activity targets with validation and deep-copy isolation. Preserve existing fields, comparability, canonical history and domain schema 17; resource paths never authorize host navigation.

## v7.40.3 - 2026-10-15

- Preserve typed Provider failures when a request or recovery window times out while execution remains active. Remove competing error-chain cancellation inference from Harness and runtime settlement; actual Stop and execution deadlines retain cancellation authority.
- Keep storage failures distinct from cancellation and retain original execution diagnostics when finalization fails. Preserve confirmed-tool checkpoints, unknown-effect rejection, public API, schema 17 and every migration edge.

## v7.40.2 - 2026-10-15

- Keep canonical acceptance fault injection installed before concurrent runtime work starts. Remove a race in the promotion regression fixture without changing runtime behavior, public API, or domain schema.

## v7.40.1 - 2026-10-15

- Validate provider-hosted tool events against the exact frozen request rather than independently advancing run options. Preserve rejection of unrequested tools, provider failure classification, public API, and domain schema 17.

## v7.40.0 - 2026-10-15

- Add compatible managed model-context snapshots at user admission and dynamic request preparation, with bounded validation, canonical full-record storage, stable user-role change projection and authorized current-context reads. Preserve published message-attached context semantics.
- Preserve exact cut-point facts across compaction, retry, fork and restart; freeze transport retries and fail required collection with `model_context_unavailable`. Facts never authorize tools or become conversation/title anchors.
- Append domain migration 16 -> 17 without rewriting historical facts or parsing opaque prose. Upgrade the envelope in the existing physical namespace so older writers reject upgraded stores.


## v7.39.2 - 2026-10-15

- Avoid building disposable text-search postings during read-only SQLite inspection while retaining complete canonical and projection validation. Formal runtime startup still prepares the full search index.
- Reduce Unicode n-gram allocation during index construction by deduplicating fragments before assembling posting keys. Preserve search semantics, canonical bytes, schema 16, and every released migration edge.

## v7.39.1 - 2026-10-15

- Require expanded gateways to replay their frozen prepared rendering instead of preparing mutable inputs again. Preserve one-use transport ownership and physical attempt identities.

## v7.39.0 - 2026-10-15

- Add memory-only provider request timing and incremental received-output estimates, separate from billable usage. Preserve argument deltas through the OpenAI-compatible adapter.
- Add optional bounded tool argument preview factories and attempt-fenced current-view previews. Unvalidated previews cannot authorize or dispatch tools, reserve canonical ordinals, or enter durable storage. Keep schema 16 unchanged.

## v7.38.1 - 2026-10-15

- Route expanded gateway recovery through fresh prepared request handles.

## v7.38.0 - 2026-10-15

- Add validated Agent provider retry policy, structured protocol advice and live retry progress. Recover temporary HTTP, transport and interrupted-stream failures with one bounded, cancellable budget shared by ordinary requests, compaction and titles.
- Preserve execution identities and confirmed tools; fence and replace failed streaming drafts immediately. Require explicit hosted-tool replay safety and frozen replay for expanded prepared requests. Preserve error causes, schema 16 and all published migration edges.

## v7.37.2 - 2026-10-15

- Republish the v7.37.1 waiting-execution fixes under a fresh patch tag after a premature public module lookup cached the earlier tag as unavailable. Runtime behavior, public API, and schema are unchanged from v7.37.1.

## v7.37.1 - 2026-10-15

- Preserve waiting Run settlement when an early answer starts its continuation. Serialize graceful Stop with the actor completion boundary so a pending question cannot become a failed, unresolved interaction.

## v7.37.0 - 2026-10-15

- Add optional `provider.Request.ReusePolicy` for Engine-owned prompt-cache and native-state decisions through direct, prepared and serialized gateways. Supplemental context cannot enable application reuse or retain native state.
- Resolve queue admission bases consistently for acceptance, replay and restart. Preserve native history across queue-only edits with exact-anchor validation and transactional rollback. Keep schema 16 and historical records unchanged.

## v7.36.1 - 2026-10-15

- Accept complete steering inputs with validated attachments, references, and runtime context in the canonical journal. Preserve them through application, fork, retry, and restart without broadening assistant or control-entry placement.

## v7.36.0 - 2026-10-15

- Add optional `runtime.ThreadSteeringController` for immediate parent guidance of active threads. Steering preserves the current turn, safely settles cancellable work, withdraws pending interactions, rejects unknown side effects, and keeps ordinary queued `Send` behavior unchanged.
- Add typed steering journal facts and schema 15 -> 16 migration with restart-safe pending/applied/cancelled projections.

## v7.35.0 - 2026-10-14

- Add optional `DeleteQueuedInput.ExpectedInputSHA256` for atomic conditional removal of complete pending input. Reject changed or started messages and preserve exact retry behavior across restart. Existing unconditional deletion remains available; schema 15 is unchanged.

## v7.34.0 - 2026-10-14

- Add optional `provider.State.ReusePolicy`: preserve exact-surface semantics by
  default and allow verified canonical native-history replay across tool and
  permission changes. Reject unknown policies and retain identity/secret fences.
- Commit completed provider state with canonical assistant/tool-call output
  before dispatch. Preserve the last valid state and source Run/Turn across
  provider failures, immediate/graceful Stop and restart; reject late responses.
- Reanchor canonical state at terminal settlement instead of saving stale Engine
  results. Installed compaction and unknown effects continue to invalidate state.
- Preserve schema 15, every migration edge, and unchanged historical record bytes.

## v7.33.0 - 2026-10-14

- Extend `EditQueuedInput` with paired `ExpectedInput` and `Input` for atomic
  complete queued-input editing. Reject stale attachments and references even
  when text is unchanged; preserve queue identity, order and supplemental context.
- Add read-only `ThreadQueueEditReader.LookupQueuedEdit` for immutable edit
  replay validation from existing journal facts without another receipt store.
- Preserve existing text edits, original-send replay, canonical media retention
  and schema 15. Cover complete edits, validation, SQLite restart and adoption.

## v7.32.1 - 2026-10-14

- Preserve canonical local-tool execution start and end times in current and
  history views, including terminal refresh, SQLite restart and forks. Dispatch
  time excludes admission and approval waits; absent dispatch facts leave the
  execution interval unavailable rather than inventing a start.
- Bind timing observations to exact canonical invocations without deriving Run
  identity from observation metadata. Preserve existing public API and schema 15.

## v7.32.0 - 2026-10-14

- Persist provider-neutral assistant citations with optional end-exclusive Unicode
  ranges; project them through current/history/detail views and preserve them in
  Memory/SQLite, continuations, forks and restarts.
- Keep hosted activity live while waiting for late response annotations before
  committing immutable assistant segments. Reject unsafe links and remove invalid
  positional claims without guessing bindings.
- Preserve schema 15, historical canonical bytes and opaque provider replay.

## v7.31.1 - 2026-10-14

- Preserve the referenced ThreadID in direct public `MessageReference.Validate`
  calls. Accept valid thread snapshots and reject target identities on other
  reference kinds consistently with canonical admission.

## v7.31.0 - 2026-10-14

- Add typed `MessageReferenceThread` and optional `ReferencedThreadID`, rendered
  as explicit untrusted model facts. References never authorize access.
- Add optional `ThreadQueryReader` and public `threadquery` contracts for indexed
  canonical thread, message and tool queries, bounded full reads, exact counts,
  UTC calendar grouping, stable sorts and frozen cursor pagination.
- Keep derived indexes with the canonical in-process journal owner. Rebuild
  after startup validation, update affected paths after successful commits,
  and exclude reasoning, secret/private state, queued input and correction records.
- Append atomic schema 14 -> 15, retaining all historical migration edges,
  canonical bytes, fingerprints, identities and restart behavior.
- Share lifecycle and assistant segmentation rules with current presentation;
  cover Unicode search, Memory/SQLite parity, scope/cursor invalidation, rollback,
  confirmed tools, fork/restart and lossless full-content chunking.

## v7.30.0 - 2026-10-13

- Preserve creation times for live text, tool, and input-interaction items and
  canonical historical items, including the original occurrence time in forks.
- Add optional read-only `ThreadToolResultReader.LookupToolResult` for confirmed
  local tool arguments and output on the active journal path. Exact thread,
  turn, run, and tool-call identity is required; hosts retain authorization and
  interpretation. No new store, execution, or domain schema is introduced.
- Cover live publication, completed results, SQLite restart, fork, deletion,
  and mismatched execution identity.

## v7.29.2 - 2026-10-12

- Refresh the reviewed nine-provider model catalog from one pinned models.dev
  response. Add Claude Haiku 5.5 with adaptive thinking, image input, 1M context,
  128K output, and medium default effort; correct Sonnet 4.5 context to 200K.
- Preserve host model selections and conversation identities. Public Go APIs,
  provider ownership, and domain schemas remain unchanged.

## v7.29.1 - 2026-10-12

- Make canonical marker `Entry.RunID` the sole runtime execution identity and
  remove normal-path observation metadata fallbacks. Add the atomic schema 13 ->
  14 migration for historical markers, preserving journal content, entry
  Raw/hash, provider state, and execution lineage while rejecting conflicting or
  ambiguous sources.
- Preserve live `ThreadView` JSON encoding for tool activity and continuation
  runs, including waiting-input, approval, retry, fork, cancellation, and
  restart paths. This compatibility patch adds no public API or unsupported
  identity fallback.

## v7.29.0 - 2026-10-11

- Add canonical `ThreadView.Turns` and `HistoryPage.Turns` with historical outcomes, exact run identity, elapsed time, unioned user/approval waiting time, and work duration. Queue time and parallel tool totals are excluded.
- Record accepted natural-stop assistant entry references in existing terminal metadata. Preserve length-continuation segments and stable live item IDs; tool and hook continuations discard draft candidates. Missing legacy boundaries remain unavailable rather than inferred.
- Preserve original occurrence times and rewrite final references across nested forks. Optional journal metadata keeps domain schema 13 and does not rewrite existing stores on open.

## v7.28.0 - 2026-10-11

- Stage, validate, and persist only affected domain records during normal writes. Preserve complete startup and migration validation, schema 13, durable bytes, and rollback on callback, cancellation, panic, or backend commit failure.
- Add the read-only `Host.MediaInventory` canonical inventory for immutable host media retention, including archived threads, inactive paths, forked history, and queue facts. Current views cannot safely decide byte-store collection; descriptors grant no resource access.
- Keep existing typed multimodal tool results and provider contracts unchanged. Cover large unrelated history, bounded write work, shared media ownership, restart, and deletion.

## v7.27.2 - 2026-10-10

- Align the module, CI, local checks, and build instructions to Go 1.27.2, matching the downstream toolchain and resolving the Go 1.27.1 standard-library vulnerability findings.
- Require named task worktrees until cleanup completes; do not leave detached checkouts as a substitute for removing merged work.
- Public API, domain schema, and compaction behavior remain unchanged.

## v7.27.1 - 2026-10-10

- Preserve complete compaction input, including formatting, tool arguments, canonical references, and runtime facts. Remove fixed message previews and silent transcript/output truncation.
- Use one ordered, budgeted summary loop for manual and automatic compaction. Carry the latest summary through lossless Unicode fragments; validate rendered requests, close prepared handles, and bound overflow/output retries before one atomic installation.
- Keep intermediate candidates in memory and leave effective context unchanged on failure or cancellation. Move the extractive fixture out of production, and cover long-message facts, exact fragment coverage, request budgets, and failed generation. Public API and domain schema remain unchanged.

## v7.27.0 - 2026-10-09

- Separate SQLite record values from primary-key indexes through atomic physical format 1 -> 2 conversion. Reuse exact format recognition for open, inspection, backup, and space maintenance.
- Replace global prompt snapshots with append-ordered scope/category records and small counters. Dispatch checkpoints write only relevant additions; pending responses settle atomically with turns, and unchanged records are never rewritten.
- Advance logical storage 7 -> 8 while preserving session-tree schema 13 and every supported logical source. Migrate cache content and duplicate attempts without changing compaction, continuation, usage, or historical conversation behavior.
- Reuse merged inspection namespaces across pages; cover rollback, replay, opaque preservation, write cost, large cache benchmarks, and isolated startup measurements. Public Go and thread protocol contracts remain unchanged.

## v7.26.0 - 2026-10-09

- Add optional `ThreadContextCompactor.CompactContext` and typed context-only turns. Admit idle-thread compaction without a user command message, ordinary answer, or tool dispatch; return after admission and resolve the configured Agent in the background.
- Share manual and automatic summary generation, validation, and atomic installation. Commit manual completion with installation; preserve original context on failure, cancellation, and noop, invalidate continuation on success, and expose the canonical `AfterItemID` divider anchor.
- Append the byte-preserving automatic domain v12 -> v13 migration. Preserve historical slash-command conversations and all earlier migration edges. Reject ordinary retries of context-only turns; request-key replay returns the latest view without duplicate execution.
- Cover provider failures, bounded truncation retries, preparation cancellation, install/stop races, durable write rollback, restart replay, and published-module adoption.

## v7.25.2 - 2026-10-05

- Reuse the original canonical cancellation request when shutdown, immediate stop, or execution-context settlement follows graceful Stop. Keep one cancellation fact per execution and preserve its source, mode, and timestamp. Centralize lookup under the thread actor lock and propagate journal read errors without fabricating replacement requests. No public API or schema change.

## v7.25.1 - 2026-10-05

- Preserve pure-thinking tool-call reasoning in the public gateway message, emitting complete step reasoning once across parallel calls and hosted assistant fragments. Canonical history stays unchanged.
- Settle active executions through canonical cancellation before Host shutdown closes actors. Resolve pending approvals, preserve queues and durable input waits, join effects, retain unknown-effect failures, and return settlement errors. No public API or schema change.

## v7.25.0 - 2026-10-05

- Add opt-in `Capabilities.ReasoningHistory` with `ReasoningHistoryCurrentUser` to omit reasoning before the latest canonical user input from provider transport. Preserve canonical history, visible answers, tool pairs, current-input continuations, and supplemental-answer boundaries; keep all-history replay as the default.
- Apply the same projection to preparation and token estimates, with tool continuation, fork, restart, and immutable-history regressions. No domain schema change.

## v7.24.0 - 2026-10-05

- Add the optional read-only `ThreadSendReader.LookupSend` contract to recover original admitted input by request key without rebuilding host context or attachment state. Queue editing, promotion, removal, and restart retain the original send identity; no new journal record or schema change is required.
- Fix replay after an edited queue item is promoted: validate against the original queue admission instead of the edited user message. Changed input remains a request conflict.

## v7.23.1 - 2026-10-05

- Close copied in-flight tool calls in a fork before its branch boundary, explicitly recording that execution remains owned by the source. Full-history child threads can accept their first mission without an incomplete tool exchange; source execution, completed results, public API, and domain schema v12 remain unchanged.

## v7.23.0 - 2026-10-05

- Preserve confirmed tool results when retrying a failed turn from its existing durable save point, including repeated failures and restart. Project provider retry boundaries from canonical messages rather than non-message observations.
- Accept verified thread-level queue records between canonical turn entries so answering a pending question after restart can continue and drain queued inputs.
- Add opt-in `CancelInput.IncludeDescendants` to stop the existing child subtree under the same admission boundary as child creation. Default selected-thread cancellation, independent threads, queued inputs, and domain schema v12 remain unchanged.

## v7.22.1 - 2026-10-05

- Reserve up to 1,024 output tokens for automatic titles when reasoning cannot be disabled or its control is unknown. Provider limits count hidden reasoning as output; the visible title remains limited to 48 Unicode characters. Keep the 64-token fast path for explicit non-reasoning and disable-capable models, and preserve explicit budgets, truncation failures, and lifecycle ownership.
- No public API, wire shape, or domain schema change.

## v7.22.0 - 2026-10-04

- Add explicit `ReasoningLevelOn` without changing Default or Off semantics. Verified toggle catalogs expose On; effort-only models continue to reject it. Existing reasoning selections and schema v12 remain unchanged.
- Add metadata-driven Ollama thinking capability parsing and validated OpenAI-compatible effort mapping. Advertise only exact supported levels, distinguish boolean controls and fixed states, and retain provider-managed behavior for absent or unknown metadata instead of guessing from model names.

## v7.21.0 - 2026-10-04

- Add typed context-budget failures for invalid output reservations, fixed input overhead, and requests still oversized after compaction. Preserve these codes in live views, summaries, and restart reads without changing schema v12.
- Reject impossible output budgets and known oversized fixed input before automatic compaction, avoiding futile summary generation. Hosts can offer configuration changes and explicit retry through the existing runtime boundary.

## v7.20.0 - 2026-10-03

- Add the optional `runtime.ThreadQueueController` for atomic pending-text edits and graceful stop-and-send of a chosen queued input. Preserve attachments, context, order, existing `ThreadService` implementations, and schema v12.
- Resolve edited input at canonical admission; reject stale edits and preserve unknown-effect boundaries. Request disconnection cannot abandon an accepted stop-and-send transition.

## v7.19.0 - 2026-10-03

- Refresh the reviewed Agent catalog and add DeepSeek V4.1 Flash (`deepseek-flash`) through the existing Responses gateway. Preserve both official legacy Flash aliases and their original conversation identities; enable their current image capabilities and Pro low-effort reasoning.
- Align DeepSeek output ceilings and reasoning defaults with current metadata; disable the default hosted-search preset because the official API ignores built-in tools. Preserve explicit v7 hosted-tool wire and replay compatibility.
- Keep GPT-6.1 Sol outside the built-in Chat catalog because tool calls require a host Responses gateway. No exported Go API or domain schema change.

## v7.18.4 - 2026-10-02

- Keep graceful cancellation under the stop owner's authority when a projection write is canceled. Pending approvals and tool results now settle together instead of competing with failure finalization and leaving an unresolved approval behind.

## v7.18.3 - 2026-09-28

- Select the first usable fallback title directly, preserving source priority and Unicode truncation while avoiding unchecked combined slice-capacity arithmetic and unnecessary candidate allocation.

## v7.18.2 - 2026-09-21

- Prioritize the user's language over title compression and allow automatic titles up to 48 Unicode characters. This addresses English requests still producing Chinese titles with DeepSeek under the previous 16-character limit.
- Add opt-in live DeepSeek qualification through the real automatic-title lifecycle, using isolated storage and no ordinary agent provider calls or tools.

## v7.18.1 - 2026-09-21

- Require automatic title prompts to preserve the user's request language, explicitly including English. Keep title input isolated from host context and preserve existing title lifecycle behavior.

## v7.18.0 - 2026-09-20

- Add `ToolSurfaceRequest.InitialProviderSurface`, a detached first-checkpoint snapshot of system text and local/hosted definitions. Dynamic hosts can preserve unrelated capabilities while refreshing live policy, including after restart.
- Keep earlier request records immutable and reuse existing schema v12 storage. No executable handlers or host persistence mirror are exposed.

Compatibility: additive to the protected v7.16.2 baseline and v7.17.0 additions.

## v7.17.0 - 2026-09-20

- Add opt-in `ToolSurface.RefreshProviderSurface` for live system text and tool capability changes at provider boundaries. Preserve frozen model/reasoning configuration, immutable historical checkpoints, default behavior, and schema v12.
- Start a new render lineage and clear opaque provider continuation state when the envelope changes; retain canonical history, approvals and effect ownership.

Compatibility: additive to the published v7.16.2 API.

- Establish published v7.16.2 as the public API compatibility baseline. Retire source/API compatibility promises for earlier releases; protect the new baseline and compatible additions released afterward.
- Require the published-module comparison even when the local checkout has no baseline tag. Keep the current API snapshot, strict incompatibility failures, and permanent conversation-schema migration lineage.

## v7.16.2 - 2026-09-18

- Preserve every model call in mixed ordinary/control batches. Execute valid ordinary tools once and return paired, bounded correction feedback for controls that must be resubmitted separately.
- Keep correction messages identical across live projection and final persistence; they never acquire effect authority or create interactions.
- Resume provider-native reasoning and search state from the exact waiting run after input, including restart. Keep strict canonical history checks, public APIs, and schema v12 unchanged.

## v7.16.1 - 2026-09-18

- Derive Ask User waits directly from committed control calls, removing the asynchronous duplicate request that could overtake an answer.
- Keep answered interactions settled in cancellation, terminal validation, history, and summaries even when historical request records follow the resolution.
- Preserve public contracts and schema v12; add deterministic journal-order regressions and durable continuation coverage.

## v7.16.0 - 2026-09-18

- Add public model-aware estimation of frozen provider requests, excluding transport metadata and separating actual media from text.
- Embed hash-verified official OpenAI vocabularies; use an explicit multilingual proxy for unknown models and opaque host transports, with documented uncertainty.
- Share bounded BPE counting with DeepSeek; preserve canonical calibration, overflow recovery and schema v12.

Compatibility: additive API relative to v7.15.1. The v7.0.0 baseline retains the documented pre-existing ToolResult comparability loss; this release does not change that type.

## v7.15.1 - 2026-09-18

- Upgrade regexp2 to fix stale timeout deadlines on the first tokenizer request after an idle conversation.
- Sanitize tokenizer failures so model input cannot enter error messages; retain the official token counts and bounded long-span BPE work.

## v7.15.0 - 2026-09-18

- Preserve native context calibration across durable Turn boundaries and restart using canonical history prefixes and complete request deltas.
- Add committed confirmed/estimated context snapshots for consistent live and reconnect presentation, retaining existing v7 fields and schema v12.
- Replace DeepSeek UTF-8 byte estimates with an offline official V4 tokenizer, explicit text headroom, and image budgets; preserve overflow recovery and invalidate incompatible old estimates.

Compatibility: no incompatible API changes from published v7.14.0. The v7.0.0 comparison still reports the pre-existing `provider.ToolResult` comparability loss from the image attachment addition in v7.10.2; this release does not change that type.

## v7.14.0 - 2026-09-17

- Preserve safe structured tool inputs independently of result rows, including explicit empty output snapshots.
- Add code language and truncation metadata, with complete code display up to 64 KiB and detached sanitization.
- Verify live subscriptions, canonical history and SQLite restart without rewriting old Activity records or changing domain schema v12.

## v7.13.0 - 2026-09-15

- Preserve Ask User headers, input placeholders, declared choice exhaustiveness, and original choice identifiers, labels, and descriptions in live views, summaries, and restored interaction history.
- Add optional runtime presentation fields and `InputChoice`; retain v7 `options` strings and submitted answer values unchanged. Missing historical metadata remains absent, with no storage migration or journal rewrite.
- Detach nested interaction presentation data at public view boundaries so consumer edits cannot mutate runtime state.

## v7.12.0 - 2026-09-15

- Add completed-tool user input requests through the existing runtime interaction and Respond boundary; wait for the entire batch and every answer without replaying tools.
- Preserve waits and partial answers across restart, reject incomplete or invalid answers, and retain cancellation and unknown-effect safety.
- Append automatic domain migration v11 to v12 for validated tool input requests, preserving canonical history and existing v7 public contracts.

## v7.11.1 - 2026-09-14

- Advance the provider context projection revision for tool-result media replay and reset old render lineages once so historical requests are rebuilt with preserved attachment descriptors.

## v7.11.0 - 2026-09-14

- Add opaque Activity target resource references without treating media identifiers as navigable URLs.
- Preserve tool-result image descriptors in model request snapshots and replay; verify both model input and Activity through the durable thread runtime and restart.

## v7.10.3 - 2026-09-13

- Preserve sanitized DeepSeek HTTP error codes and messages through the public provider error contract while retaining context-overflow classification.

## v7.10.2 - 2026-09-12

- Add opaque image attachments to provider tool results so hosts can return screenshots and other media through a resolver without persisting bytes.
- Expand DeepSeek Vision tool-result images into `function_call_output` image parts and preserve attachment descriptors through runtime replay.
- Preserve the v7 public API and reject unresolved or unsupported image tool outputs at the provider boundary.

## v7.10.1 - 2026-09-10

- Install terminal lifecycle and canonical results in one actor transition, preventing polling and new input from observing partially settled Stop outcomes.
- Preserve confirmed cancellation and real errors in tool-result event projection. Add a blocked-snapshot regression and repeated race validation.

## v7.10.0 - 2026-09-10

- Add opt-in graceful Stop with canonical provenance, immediate acknowledgement, and a shared five-second tool settlement window; preserve the default immediate Cancel contract.
- Preserve confirmed canceled tool results, partial output, artifacts, and real execution errors. Fence new dispatch and Ask User continuation after Stop, retain queued input, and reject late results.
- Keep unknown effects protected from replay and retry, and preserve their canonical terminal classification across live views and restart.
- Add `Registry.DispatchBatchObserved` so Stop can settle ready results within the shared deadline while normal Engine execution retains model call order.
- Add `tools.ResultOutcomeCanceled` and `tools.CanceledResult`; bare context cancellation cannot confirm an effect outcome.
- Migrate domain schema v10 to v11 automatically, preserving old records and missing historical stop provenance.

## v7.9.2 - 2026-09-10

- Publish validated tool descriptions, commands, output updates, and status directly through the thread actor, without waiting for canonical refresh or turn completion.
- Keep provider argument streams out of running tool cards, preserve observed progress across older journal snapshots, and serialize Activity snapshot publication for parallel tools.
- Preserve existing public APIs, presentation sanitization, canonical history, and domain schema.

## v7.9.1 - 2026-09-09

- Own submitted input slices before asynchronous Agent preparation and execution, so callers cannot change admitted references, context, or queued input after `Send` returns.
- Reuse the existing input normalization boundary and verify ownership with a deterministic blocked-factory regression and race detection.

## v7.9.0 - 2026-09-09

- Support DeepSeek Vision through optional host-authorized image resolution, gated by each model's image capability.
- Freeze image payloads before estimation and streaming; retain only image references and digests in opaque replay state.
- Add `DeepSeekOptions.ResolveAttachment` without changing existing defaults, public message shapes, or domain storage schema.

## v7.8.0 - 2026-09-09

- Preserve submitted reference content in model history and compaction while excluding opaque resource locators.
- Add durable `UserInput.Context` snapshots, validated and fingerprinted atomically with canonical input; preserve them through response, retry, restart, and fork.
- Freeze the complete rendered request history; establish explicit projection v8 and canonical retry boundaries without rewriting old requests.
- Record schema-invalid tool calls and results in canonical history and allow two regeneration opportunities. Keep invalid calls out of dispatch, authorization, interactions, and Activity; preserve `control_error` on exhaustion.
- Migrate domain schema v9 to v10 atomically without modifying old journal bytes or reconstructing missing ephemeral material.


## v7.7.0 - 2026-09-09

- Generate native Agent model metadata from an auditable offline models.dev snapshot, with explicit official corrections and lifecycle labels.
- Update model token limits, image metadata, prices, and reasoning levels; remove retired and unsupported presets.
- Send current Kimi, Qwen, OpenAI, and adaptive Claude reasoning parameters correctly. Keep credentials, model selection, and live catalog discovery in hosts.
- Preserve the v7 public API and domain schema.

## v7.6.1 - 2026-09-08

- Remove temporary SQLite sidecars created by read-only maintenance after closing its connection. Preserve existing WAL and shared-memory files.
- Verify that inspection and backup leave a closed database's file set unchanged, so staged restore checksums contain only owned durable data.

## v7.6.0 - 2026-09-08

- Add read-only SQLite compatibility inspection and complete offline snapshots, including committed WAL records.
- Add deferred Host activation so coordinated application startup cannot dispatch providers or tools before product storage and authorization are ready.
- Prepare staged snapshot restores by stopping old execution and approvals, preserving queue input for explicit resubmission, and rejecting unsafe turn retries.
- Keep existing startup defaults and canonical migration chains unchanged.


## v7.5.0 - 2026-09-08

- Expose canonical `ThreadSummary.TitleGeneration` so hosts can order complete
  title snapshots independently of runtime views and activity timestamps.
- Preserve generation through inactive and active summaries, forks, and restart
  without changing title generation behavior or the domain schema.

## v7.4.0 - 2026-09-08

- Add web Activity operation, URL, find pattern, result availability, and source
  snippets without changing existing v7 fields or the domain schema.
- Preserve DeepSeek operation facts across sparse status/completion events and
  distinguish unavailable source details from explicitly empty results.
- Keep source facts through Activity merging, sanitization, persistence, and
  restart. Answer citations remain separate from per-call results.

## v7.3.4 - 2026-09-08

- Persist the Engine's canonical local tool-call messages in live thread
  projection, preserving complete reasoning across hosted search boundaries
  without appending duplicate calls during turn finalization.
- Keep assistant fragment boundaries consistent between execution and durable
  history so source threads and ordinary or nested forks can continue after
  restart without context-prefix drift. Internal call messages remain private
  to projection and are stripped from every observation boundary.
- Preserve strict tool-history validation and occurrence-aware reconciliation.
  This prevents new corruption; existing invalid histories are not rewritten.

## v7.3.3 - 2026-09-08

- Install an independent canonical fallback title when a fork copies user
  history, so list-only consumers can display the destination immediately.
  Empty boundaries remain untitled; source title generation is never copied.

## v7.3.2 - 2026-09-08

- Preserve provider-native search activity in canonical thread views, history,
  and restarts without creating local tool exchanges or retaining raw results.
- Report failed DeepSeek searches as errors, display multi-query searches, and
  normalize answer segments around hosted activity for stateless continuation.
- Require Go 1.27.1 consistently for module builds and CI.

## v7.3.1 - 2026-09-08

- Bind a user stop to the selected Turn's current Run under the cancellation
  lock, so answering a question concurrently cannot turn the stop into a no-op.
  Execution-context cancellation remains bound to its exact Run.

## v7.3.0 - 2026-09-08

- Add compatible terminal Activity facts for exec/read/write/terminate, sent
  bytes, output cursors, remaining output, total bytes, public execution
  location, and timeouts. Read snapshots can clear `has_more` after draining.
- Validate, sanitize, clone, and preserve the typed metadata through updates,
  finalization, and canonical history without admitting raw stdin fields.

## v7.2.0 - 2026-09-08

- Release an effect finalizer when cancellation seals a dispatched tool before its result is finalized, preventing shutdown from waiting on an abandoned callback.

- Add `provider.NewDeepSeek` for DeepSeek V4 Pro/Flash using the stateless Responses API, with streamed reasoning and local tool calls, provider-native web search and citations, complete request estimates, and opaque history replay across runtime restarts.
- Add `runtime.WithAgentHostedTools` to freeze provider-native capabilities on an immutable Agent without a host dynamic tool callback.
- Advertise DeepSeek Responses and hosted search in the provider catalog. Preserve the explicit OpenAI-compatible Chat Completions constructor.

## v7.1.5 - 2026-09-07

- Preserve complete local tool exchanges after canceled control interactions so provider requests never contain orphaned tool results.


## v7.1.4 - 2026-09-06

- Keep active thread views and summaries contract-complete while an approval or
  user-input interaction is being installed: `run_progress` is cleared only
  with the published unresolved interaction and remains present for other
  active snapshots.
- Reserve local interaction waiters before durable admission and claim them
  atomically when a response arrives, preventing duplicate provider recovery
  after an approval is answered.

## v7.1.3 - 2026-09-04

- Treat every failed control projection as a terminal control failure. Invalid
  `ask_user` calls no longer create pending input, including when reading
  historical `waiting + control_error` records.
- Pair failed control calls with a safe provider-visible error result so a
  later Turn can continue without exposing arguments or validation details.

## v7.1.2 - 2026-09-02

- Migrate context records from Turns resumed after interaction boundaries by
  following the Run identity on each canonical entry. A resumed Run no longer
  gets mistaken for corruption merely because its Turn started under an older
  Run.

## v7.1.1 - 2026-09-02

- Make canonical context entries the sole source of Thread, Turn, and Run
  identity. Context payloads no longer duplicate execution identity, and
  direct or nested forks rebuild snapshots with the destination Thread ID.
- Add atomic domain schema v9 migration. Exact v8 context records are renamed
  and normalized; the released fork mismatch is repaired only when canonical
  ancestry and copied-entry evidence prove its source. Drifted, malformed,
  mixed, or future authority fails closed without mutation.
- Remove the unused fork identity-map and entry-rewrite extension path. Forks
  retain historical Turn and Run identity while remapping only destination
  Thread and entry references.

## v7.1.0 - 2026-09-02

- Add `runtime.WithAgentRunLabels` so immutable Agents carry correlation and
  opaque host execution context through provider, permission, and tool calls.
- Apply Agent labels at the single provider-host execution boundary for fresh
  Turns and Ask User continuation, preventing command-specific paths from
  dropping effect authorization context.

## v7.0.5 - 2026-09-01

- Reset the latest per-request context usage when a new canonical Turn policy
  is committed, so model switches do not compare the previous model's usage
  against the new model identity before its first usage arrives.
- Preserve canonical whole-thread token totals across that policy boundary.

## v7.0.4 - 2026-09-01

- Hash canonical lineage from durable typed conversation facts, excluding
  journal entry and parent identities that are assigned differently during an
  active tool loop and later canonical reconstruction.
- Keep unavailable-tool call/result pairs append-only across later Turns
  without restoring removed definitions or weakening journal integrity checks.
- Advance the internal provider-context projection to v6. Existing v5 threads
  start one new render lineage with full canonical history, no compaction, and
  no journal rewrite.

## v7.0.3 - 2026-09-01

- Preserve durable tool calls and results as typed provider history, including
  strict `ask_user` call/result pairs. Public answers occur once in the result;
  secret answers remain ephemeral and leave only a redacted result.
- Keep removed ordinary and control tools valid in historical context without
  restoring obsolete definitions. New calls to unavailable tools still return
  a safe ordinary result and continue the provider loop.
- Advance the internal provider-context projection to v5. Existing v4 threads
  start one new render lineage with their full canonical history and no
  compaction or journal rewrite.
- Checkpoint every provider dispatch, including requests with supplemental
  context. Ephemeral checkpoints preserve the Turn surface and canonical
  lineage while omitting payload hashes, continuation state, and request
  measurements derived from private overlay content.

## v7.0.2 - 2026-09-01

- Report product-neutral `migrating` and `verifying` phases from
  `runtime.Open` so hosts can explain long first-start upgrades without
  inspecting Floret-owned records.
- Detect one exact session-tree source format before migration, reject mixed
  authority without mutation, and avoid a second persistent decode for an
  unchanged current-v8 store.
- Reuse SQLite write and delete statements within each transaction and skip
  no-op unknown-effect cloning during startup while preserving atomic final
  verification after every startup write.
- Add large-store and real-thread copy qualifications that leave source
  databases untouched.

## v7.0.1 - 2026-09-01

- Treat the exact v6.1.1 and v7.0.0 provider-context revisions as a one-time
  render-lineage boundary after the v8 continuation-message repair. The first
  later request sends the complete repaired canonical history without compacting
  it or carrying provider continuation state.
- Add a runtime qualification that opens a copy of a real migrated thread,
  preserves its historical item prefix, and completes a new Turn while proving
  the source database is untouched.

## v7.0.0 - 2026-09-01

- Complete a Turn on terminal provider natural stop and remove the explicit
  completion policy, reserved completion tool, forced continuation, and public
  lifecycle contracts from the v7 module.
- Freeze provider, model, reasoning, system prompt, local and hosted tools,
  adapter, context policy, and continuation compatibility in one durable Turn
  surface checkpoint before provider dispatch. Ask User, tool loops, retries,
  and restart recovery reuse the same surface; a new Turn uses current settings.
- Separate append-only canonical history from content-addressed render
  lineages. Execution-surface changes preserve full history without compaction,
  and provider-native continuation is reused only for an immediately adjacent,
  exactly matching surface.
- Add domain schema v8. Its strict v7 migration reclassifies only the historical
  Engine continuation message paired with an exact `context_continue` save
  point, restoring canonical user identity without mutating ambiguous data.
- Preserve historical tool activity independently of the current registry.
  Removed tools render neutrally, while new unavailable calls return a safe
  ordinary tool result and continue the provider loop.

## v6.1.1 - 2026-08-31

- Persist public Ask User answers as canonical user messages. Secret answers
  remain ephemeral and leave only a redacted marker in later context.
- Separate provider-neutral conversation lineage from provider/model render
  lineage. New Turns may switch models without rewriting history or forcing
  compaction; same-Turn drift and canonical prefix mutation fail closed.
- Checkpoint raw segments, toolsets, and provider request lineage before model
  dispatch so restart preserves the exact validated prefix.

## v6.1.0 - 2026-08-31

- Preserve one append-only provider context lineage within each prompt scope and
  compaction generation. Previously admitted reasoning, assistant text, tool
  calls, tool results, and control results cannot be replaced or reordered;
  capability changes establish a new audited compaction generation.
- Add `AgentRequest.CanonicalTurnInput` and
  `WithAgentTurnCompletionPolicy`. Explicit-signal Agents retain natural-stop
  output, then continue until `ask_user` or `task_complete` declares the
  lifecycle result.
- Reset provider-attempt and live-segment ownership atomically for every new
  Run. Ask User continuation now publishes `preparing` before provider work,
  preserves exact attempt identity, and streams reasoning and assistant deltas
  before terminal settlement.

## v6.0.1 - 2026-08-30

- Migrate the historical v6 fork shape whose copied Effect Attempt still names
  its verified source-thread ancestor. Terminal copied attempts are removed as
  obsolete execution authority while unrelated or active identity mismatches
  continue to fail closed.
- Stop copying Effect Attempt entries during new forks. Forked journals now
  retain conversation history only, reconnect parents, recompute depth and
  leaf identity, and regenerate canonical entry bytes.

## v6.0.0 - 2026-08-30

- Advance the module to semantic import path `/v6` and freeze the generated v6
  public API baseline.
- Fail a turn atomically with `effect_outcome_unknown` whenever a dispatched
  tool effect cannot be confirmed. The same terminal path is used during live
  execution and before `runtime.Open` makes a recovered Host available.
- Add domain schema v7. Its strict v6 migration permanently fills exact
  historical `RunID`, removes executable effect-retry authority, closes pending
  tools and interactions, clears provider continuation, and converges active
  unknown effects exactly once.
- Remove `ThreadService.RetryEffect`, effect-retry interactions, retry claims,
  and retry dispatch state. Unknown irreversible effects can no longer be
  replayed manually or automatically.
- Remove the deprecated `ThreadView.Error`, `ThreadSummary.Error`,
  `AssistantDraft`, and `ThinkingDraft` mirrors. Hosts consume typed `Failure`
  and ordered `Items` only.

## v5.0.16 - 2026-08-30

- Expose the exact `RunID` on every current-view item and interaction. Canonical
  reload now preserves each journal entry's `TurnID` and `RunID`, including
  multi-turn history and same-turn continuation, and rejects incomplete or
  conflicting execution identity instead of projecting an ambiguous item.
- Persist run identity on all new message and effect-result entries. Message
  rows written by v5.0.0-v5.0.15 are repaired only when canonical lifecycle
  markers identify the run exactly; ambiguous active continuation still fails
  closed.

## v5.0.15 - 2026-08-29

- Expose the exact `RunID` and one transient, provider-neutral run-progress
  phase on typed thread views and summaries. The thread actor now owns phase
  changes, rejects stale run and provider-attempt events, and clears progress
  while waiting for interaction or after terminal settlement.

## v5.0.14 - 2026-08-29

- Replace the monolithic session-tree checkpoint, JSON diff journal, and
  full-path root inventory writes with schema-v6 thread, entry, artifact, and
  compact index records. Mutations now write only affected records, independent
  of unrelated conversation history size.
- Atomically migrate v5 checkpoints and pending recovery frames to v6, then
  remove legacy records without retaining a dual-read runtime path.
- Initialize SubAgent titles from their task names and let the caller context
  control turn admission instead of imposing an internal five-second timeout.

## v5.0.13 - 2026-08-29

- Add typed SubAgent operation Activity payloads that preserve management
  actions, ordered targets, completion counts, missing targets, and wait
  timeouts without overloading the single child-thread fact renderer.

## v5.0.12 - 2026-08-28

- Publish cumulative canonical thread token totals on each successfully
  committed final provider-usage runtime event, so first-turn live consumers
  can show cache usage without polling or maintaining a second ledger.
- Reject stale provider-attempt usage before journal admission, and never attach
  totals to stream-only usage or failed journal writes.

## v5.0.11 - 2026-08-28

- Preserve bounded file read content, line ranges, mutation counts, unified
  diffs, and unavailable-diff details in typed File Activity payloads.
- Add typed multi-file Patch mutations and aggregate change metadata so hosts
  do not need to parse tool-result text or maintain a second projection.

## v5.0.10 - 2026-08-28

- Stop discovering, requesting, and emitting page icons from `web_fetch`.
  Activity remains limited to response metadata and a bounded content preview;
  hosts own static tool iconography.
- Retain the previously exported site-icon Activity fields as deprecated v5
  compatibility data for records written by v5.0.9.

## v5.0.9 - 2026-08-27

- Preserve typed Web Fetch Activity payloads through event sanitization,
  canonical journal storage, restart recovery, and current thread views.
- Add a 2,000-character content preview and one best-effort passive same-origin
  site icon under the existing SSRF-safe fetch path. Complete page content
  remains in the tool result and artifact.
- Include the bounded requested URL in the default Web Fetch Activity label so
  hosts have a useful fallback title.

## v5.0.8 - 2026-08-27

- Project final provider usage through the current attempt-scoped event
  envelope so canonical thread totals and live context status retain input,
  output, cache-read, and cache-write counts.
- Use one strict attempt-envelope decoder for runtime and harness projections;
  incomplete or mismatched attempt identity remains rejected.

## v5.0.7 - 2026-08-27

- Add the public `tools/webfetch` tool with fixed GET-only limits, redirect and
  dial-time SSRF validation, bounded text decoding, and HTML-to-Markdown
  conversion.
- Add typed web-fetch Activity metadata and an output-policy truncation notice
  while preserving the complete tool result in the canonical artifact path.

## v5.0.6 - 2026-08-27

- Derive one dense public tool-batch plan after argument validation so internal
  schema-correction results cannot leave canonical tool projection waiting for
  calls that were intentionally hidden.
- Expose canonical typed terminal failure details on `ThreadView` and
  `ThreadSummary`. The existing `Error` field remains a deprecated text mirror
  for v5 wire compatibility.
- Classify provider-attempt and tool-batch projection invariant failures as
  engine-contract failures while preserving storage failure classification for
  repository writes.

## v5.0.5 - 2026-08-27

- Expose disjoint cumulative input, output, cache-read, and cache-write token
  totals from canonical final provider usage records through
  `ThreadContextReader`, while preserving the latest context-pressure status.

## v5.0.4 - 2026-08-27

- Repair the exact legacy tool-result Raw projection written when invalid UTF-8
  was JSON-escaped before normalization. Startup verifies the original hash and
  root inventory, checkpoints the canonical representation atomically, and
  continues to reject every other authority mismatch.
- Preserve the public `runtime.ErrAuthorityCorrupt` classification for
  session-tree failures returned by `runtime.Open`.

## v5.0.3 - 2026-08-26

- Normalize invalid UTF-8 in tool-result text and typed Activity presentation
  before output projection, observation, and canonical persistence.
- Treat the committed Effect result entry as the single result fact while
  retaining strict Effect identity, entry-integrity, and artifact checks. JSON
  normalization can no longer create a false authority-corruption failure.

## v5.0.2 - 2026-08-25

- Bound recovery storage by checkpointing terminal turns atomically and folding
  unusually long active journals after 128 frames or 32 MiB.
- Add validated pre-open SQLite space maintenance. Fresh stores use incremental
  auto-vacuum; legacy stores can be rebuilt with native SQLite `VACUUM` without
  exposing or copying opaque Floret records.
- Project root thread lists from one canonical inventory snapshot and expose
  bounded lifecycle, queue, pending-input, error, and last-item summary fields.

## v5.0.0 - 2026-08-23

- Advance the module to semantic import path `/v5`. The v5 API baseline now
  includes bounded structured Activity rows without weakening the frozen v4
  compatibility contract.

- Merge each tool result presentation with the matching tool call presentation
  before persistence and canonical current-view projection. Terminal results
  can add output and status without erasing host-authored descriptions,
  commands, or other safe display facts from the original invocation.

## v4.0.19 - 2026-08-23

- Make `ThreadService.Cancel` atomically commit the cancellation request,
  pending interaction and tool settlement, effect fence, and aborted turn.
  Stop no longer waits for provider or tool goroutines, unknown effects cannot
  be retried after cancellation, and late writes cannot reactivate the turn.
- Project canceled tool results consistently at both the activity and payload
  levels, and settle previously recorded incomplete cancellations on restart.

## v4.0.18 - 2026-08-23

- Add bounded, ordered structured activity rows for host-sanitized text,
  Markdown, and code details. Rows remain product-neutral typed display data
  and survive lifecycle merges and terminal settlement.

## v4.0.17 - 2026-08-22

- Carry validated turn-scoped supplemental context through typed thread sends,
  queued input promotion, and provider execution without adding it to
  canonical conversation history.
- Keep cancellation idempotent and expose the current view returned by the
  single runtime cancel boundary so hosts can settle stop actions without a
  second, racy view read.

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

## v5.0.1 - 2026-08-24

- Publish terminal thread views only after canonical journal projection. A
  completed turn with no visible assistant or terminal tool presentation now
  fails explicitly instead of leaving a user-only transcript, and canonical
  refresh failures are returned to the terminal settlement path.

- Keep accepted thread turns independent from transport request contexts. Add
  internal cancellation-source diagnostics so user stop, thread deletion,
  runtime shutdown, and execution-context cancellation are distinguishable.

- Fence canonical refreshes against a view change that occurs while the
  journal snapshot is being read, so an older snapshot cannot replace a
  terminal current view.

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
