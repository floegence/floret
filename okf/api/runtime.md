---
type: Public API
title: Runtime Package
description: Immutable Agents, durable Host ownership, typed thread operations, and observation.
resource: /runtime
tags: [api, runtime, agent, host]
timestamp: 2026-08-18T00:00:00Z
---

# Runtime

`runtime.Open` owns one opaque `storage.Source` and returns a composition-root
`*runtime.Host`. Hosts create an immutable `Agent`, bind an
`AgentFactory` through `Host.ThreadService`, and expose that typed service to
the product boundary. The service owns one durable thread journal and one
in-process actor for each active thread.

## Thread goals

Opt in with `runtime.WithAgentGoals()`. The optional
`runtime.ThreadGoalController` exposes `SetGoal` and `DeleteGoal` without
starting execution. `ThreadView.Goal` and `ThreadItemGoal` expose canonical
current state and changes; `Reviewing` is a transient presentation fact.
`ThreadGoalSubmitter.SubmitGoal` adds explicit save-and-pursue intent without
changing the save-only controller. It starts only from canonical idle state;
running or waiting execution is updated without another queued Turn.
See [Canonical Thread Goals](../decisions/thread-goals.md) for request timing,
review validity, deletion suppression and recovery boundaries.

## Thread service

`Host.MediaInventory` is the read-only canonical retention boundary for
host-owned media byte stores. It returns `MediaReference` values with explicit
`ThreadID` and attachment descriptors from all retained journals: archived
threads, inactive paths, and queue facts remain covered. Deleted threads are
excluded. Descriptors stay opaque and are never authorization evidence. Hosts
must hold the tool-media write gate before reading the snapshot and collect only
when `Idle` is true, or before deferred execution is activated. `Idle` remains
false for unfinished executions, including waits and archived threads. Upload
owners retain their existing protection before admission. Current `ThreadView`
and root-list results are insufficient
for garbage collection. This additive v7.28 API does not change image-tool
contracts, storage bytes, or the domain migration lineage.

`ThreadService` is the current host-facing lifecycle contract. Its commands
include `Create`, `Fork`, `Delete`, `SetTitle`, `Send`, `Respond`, `Cancel`,
`Retry`, queue operations, and `Subscribe`; queries include
`View`, `List`, `History`, and `Context`. Every mutation carries a stable
request key. Replaying the same key and fingerprint returns the committed
result; changing the input returns `runtime.ErrRequestConflict`.

The optional `ThreadToolResultReader.LookupToolResult` reads a confirmed local
tool's original arguments, visible output, tool name, and status from the active
canonical path. The query requires exact thread, turn, run, and tool-call
identity. An unfinished, absent, or different execution returns `found=false`;
deleted threads return the normal thread error. Hosts authorize the request and
interpret their own output without reconstructing provider history, dispatching
a tool, or creating another result store. These private values must not be
included in public presentation by default.

The built-in backend locates the exact call/result pair through the transactional
canonical query index. It checks source identity, chronology, current active-path
membership and integrity before returning detached entries. Unrelated history is
not scanned or materialized. Released custom journal repositories lacking this
internal capability retain their complete-read adapter; it can be removed only
with the next major API boundary.

Live text items use their first delta's occurrence time, and tool items use the
Activity start time. Updates preserve that creation time. Historical items use
the journal's original occurrence time, including copied entries in forks;
timestamps do not define item ordering or replace the canonical ordinal.
Local-tool Activity uses the original canonical dispatch occurrence for
`StartedAtUnixMS` and the result occurrence for `EndedAtUnixMS`, keyed by exact
turn, run and tool-call identity. Current terminal views, History and fork/restart
reads preserve that interval. Admission and approval waits are excluded; a
missing dispatch leaves the execution start unavailable. An observation can
supply timing only for a matching canonical invocation and never owns Run
identity. Hosts display management-tool duration independently of the current
lifecycle of a referenced child thread.

The optional ThreadContextCompactor.CompactContext takes ThreadID, RequestKey
and optional ContinueGoal. Admission requires an idle thread without queued input
or unresolved interactions and returns before provider work. Omitted/false keeps
TurnKindContextCompaction, whose installation and terminal are atomic and which
contains no ordinary answer or tools. True atomically selects
TurnKindGoalContinuation only for an active canonical Goal. Both kinds reach the
factory and current/history projections without fabricated user input.

Continuation performs mandatory compaction or explicit noop, then the existing
Engine and Goal review loop in the same admitted execution. A preparation failure
prevents work. Summary installation does not complete the Goal execution. Stop
covers preparation, tools, review and waits while retaining any installed summary.
Deletion before business admission prevents work; later deletion suppresses review
without stopping ongoing work. Current Goal versions and atomic terminal guards
retain completion authority.

Same-key replay preserves the original intent and admission decision after Goal
changes or restart. Pure failures use a new compact key and reject ordinary Retry.
Continuation uses ordinary Retry, interactions and restart, retaining canonical
preparation and confirmed tools without automatic uncertain-effect replay. Domain
schema 28 -> 29 preserves historical pure compactions and rejects old writers.
`ThreadContextCompaction.AfterItemID` is derived from Floret's canonical item
projection at operation start, including after pagination, fork, and restart.
Hosts use that anchor and existing live updates without another lifecycle store.
ThreadView.Context is the complete actor-owned snapshot shared with
ThreadContextReader.Context. Committed context changes install before a version
is published, including during active execution; text deltas reuse it. Failed
assembly returns an error and explicit reads retry without blocking cancellation
or execution settlement. CanonicalOrdinal preserves the operation's first lifecycle
position on the queried Path. Insertion anchors do not substitute for Turn/Run
ownership. Presentation anchors do not own execution or admission intent.
See [domain schema v14](storage.md#domain-migration) and the
[README behavior contract](../../README.md#independent-context-compaction).

The shared generator reads complete eligible canonical text, formatting, tool
arguments, references, and runtime facts. A single ordered loop batches by request
budget, carries the latest candidate summary, and fragments oversized message
bodies without dropping bytes. Rendered-request checks and overflow retries are
bounded; every prepared handle is closed. Candidates are never durable and only
the fully consumed, validated result reaches installation. Failed, incomplete,
or over-budget output preserves the original effective context. The 20,000-token
summary ceiling is separate from the 50,000-token compacted-context target.


The optional `ThreadSendReader.LookupSend` reads the original admitted
`UserInput` by thread and request key. Queue edits and promotion do not change
that original transport identity; a removed queue item also retains its send
fact. Hosts authorize the query and compare the submitted content before
returning a replay, without resolving uploads again or replacing frozen runtime
context. Missing keys return `found=false`; deleted threads return the normal
thread error. This query reads existing canonical facts and adds no receipt
store or lifecycle owner.

Floret allocates `ThreadID`, `TurnID`, and `RunID`. A `LogicalRequestID` is a
user-visible association only and never replaces those execution identities.
Child threads are ordinary durable threads with explicit parent metadata.

`Send` validates, fingerprints, and atomically commits canonical text,
attachments, references, and `UserInput.Context` before publishing a live view.
Input slices are copied before admission or asynchronous Agent preparation;
callers may reuse their input after `Send` returns without changing queued or
executing work.
`MessageContextItem` carries Kind, Title, and Text snapshots effective from that
user message. Reference kind, label, text, and truncation status enter model
history in order; `ResourceRef` remains opaque and excluded. The same projection
feeds continuation, retry, fork, restart, and compaction. Context-only and
reference-only inputs remain eligible for retry.

`SendInput.SupplementalContext` is only for explicitly ephemeral material,
including secrets. It never becomes journal history or provider continuation
state. Ordinary selected resources and runtime facts use durable input. Queue
admission, interaction resolution, and cancellation remain canonical-first;
hosts must not persist another transcript or reconstruct lifecycle from audit.

Schema-invalid ordinary and control calls retain complete call/result pairs.
Mixed batches retain original call order: valid ordinary tools execute once,
while control calls receive correction to submit separately after seeing those
results. Rejected calls never acquire effect authority or open interactions.
Correction feedback has one identical representation in live projection and
final persistence. Input continuation reuses provider state from its exact
waiting run, retaining native reasoning/search data and strict history checks.
At most two regeneration opportunities run before terminal failure. Only a valid
control projection opens an interaction; programmer, storage, and effect failures
are not model-correction opportunities. Activity filtering changes presentation,
never model history. See [provider history](../../internal/session/provider_history.go)
and [validation feedback](../../internal/engine/validation_feedback.go).

`AgentRequest.Input` is the input for the execution being created. For an
input continuation it is the accepted, secret-free interaction resolution;
`AgentRequest.CanonicalTurnInput` remains the original canonical user input of
that Turn. Agent factories use the latter when resolving stable task identity
and must not reinterpret an interaction answer as a replacement objective.
Public answers are also projected once as a canonical user message, so the
continuation and later Turns see the same durable fact. Secret values are
ephemeral; only a redacted marker remains in the journal and provider history.

After `Send` returns an accepted turn, provider execution uses a runtime-owned
background context. The request context only controls admission and response
waiting; closing an HTTP request, changing the selected thread, or reconnecting
a view cannot cancel the accepted turn. Only `Cancel`, thread deletion, and
Host shutdown are execution cancellation owners.

`Cancel` is the authoritative user Stop boundary. Default `CancelModeImmediate`
keeps synchronous settlement for existing v7 consumers. `CancelModeGraceful`
commits a stop fact and fences new execution before cancelling the execution
context, then returns acceptance. One absolute five-second window lets in-flight
tools finish output, artifacts, and canonical results through the existing effect
finalizer. The runtime owner seals cancellation when execution drains or the
window expires. Confirmed results and real errors remain unchanged; dispatched
unknown effects keep `effect_outcome_unknown` and cannot be retried.

`ThreadView.Cancellation` and `ThreadSummary.Cancellation` retain source, mode,
Thread/Turn/Run identity, and request time. Active plus Cancellation means stopping;
an idle cancelled turn means stopped. Hosts must derive progress from these views
and allow editing/navigation while waiting. Accepted cancellation survives request
disconnection and restart; it never starts queued input automatically. Historic
records without provenance do not acquire an inferred source. Stopping does not
undo effects that already occurred.

## Queued message controls

The service returned by `Host.ThreadService` also implements the optional
`ThreadQueueController`; the existing `ThreadService` interface is unchanged.
`EditQueued` requires `ExpectedText` and an edit request key. It changes only
pending text, preserving queue ID, order, attachments, references, context and
original request identity. For complete content editing, supply paired
`ExpectedInput` and `Input`, leaving both text-edit fields empty. The complete
input precondition detects attachment, reference and context changes even when
text is unchanged; replacement validates the same input contract as admission.
Supplemental context, queue identity and position remain intact.
An admitted, deleted or stale item returns
`ErrRequestConflict`; an empty invalid input is rejected. Replacement uses one
atomic transaction of existing queue facts, so schema v15 and existing readers
remain valid. Promotion resolves the latest input under the admission lock.

The optional `ThreadQueueEditReader.LookupQueuedEdit` returns the immutable
before/after content for one edit key, including after later edits, promotion,
deletion and restart. It reads existing queue facts, persists no second receipt
and grants no authorization. Hosts must authorize the thread before looking up
a key and compare the submitted content before acknowledging a replay.

`SendQueuedNow` preserves completed work and starts the selected input in a new
turn after graceful cancellation settles. This is stop-and-send, not an injected
message in the current provider request. It waits within the existing bounded
stop window without holding the actor lock; other input and navigation remain
available. After Stop is accepted, the transition runs independently of HTTP
cancellation. Unknown effects remain terminal errors and the chosen input stays
queued. If the Host shuts down before admission, queued input remains available
for an explicit retry; restart never infers or executes an unfinished command.
Replaying an admitted send returns its current view without stopping a newer turn.

See [queue editing and sending](../../runtime/thread_queue_edit_test.go).

## Input presentation

Validated Ask User fields have one projection into `InputPresentation` for
subscriptions, current views, workspace summaries, restart, and historical
interactions. Optional `ReasonCode`, `RequiredFromUser`, and `EvidenceRefs`
preserve validated reason, requested input and evidence associations. Hosts must
validate associations against authorized canonical facts; references never grant
access or permission. Absent historical fields remain absent.
Optional `InputQuestion.Header`, `WritePlaceholder`, and
`ChoicesExhaustive` preserve source intent; absent metadata remains absent.
`Choices` contains `InputChoice` entries with original `ChoiceID`, display
`Label` and `Description`, and the established submitted `Value`.

This additive v7 API preserves `Options` as ordered answer strings for existing
consumers throughout v7; removal requires a future major version. Both option
forms come from the same projection. Hosts submit `Value`, not `ChoiceID`.
Tool-origin `tools.InputRequest` remains valid with its existing string options.
No domain schema change, new durable state, history rewrite, or host journal
access is needed. Public views detach nested input data from the runtime owner.
See the [runtime presentation regression](../../runtime/thread_input_presentation_test.go).

## Tool-requested input

A completed `tools.Result.InputRequired` produces an ordinary input interaction
linked to the original tool, Turn, and Run. Its identity is derived from the
canonical result entry. The Activity remains a completed tool; no fake `ask_user`
call or duplicate result enters provider history. The response is a user-role
control message. Input becomes visible only after the batch reaches its canonical
waiting marker. The actor resumes only after every input has resolved, including
across restart; cancellation resolves outstanding input without another dispatch.
A failed batch exposes no orphan input request. Hosts must not build a second
wait owner. See the [tool input contract](tools.md#tool-requested-user-input).

## Title snapshots

`ThreadSummary.Title`, `TitleStatus`, and `TitleGeneration` form one canonical
snapshot. Hosts order titles independently of runtime view versions, activity,
and product settings. An unset title has generation zero. Each new automatic
attempt or manual title advances the generation. Within one generation,
`pending` may settle to `ready` or `failed`; terminal snapshots cannot regress
to pending, and conflicting terminal snapshots are invalid. Duplicate snapshots
are idempotent. A higher generation takes precedence even when it is pending.
Generation is existing durable authority, not another journal revision. Claim
tokens remain private. Hosts must not infer titles from runtime current views.

Automatic title generation uses the admitted user text with a separate title
prompt that instructs the provider to preserve the request language, explicitly
including English. Host context and the main agent system prompt do not select
the title language. Language preservation takes priority over compression;
automatic titles allow up to 48 Unicode characters so English phrases remain
readable. Generation disables reasoning when supported, otherwise chooses the
lowest declared short-request effort. Since provider output limits include
hidden reasoning, automatic titles allow up to 1,024 output tokens when thinking
cannot be disabled or its controls are unknown; explicit non-reasoning and
disable-capable models retain the 64-token limit. The ceiling is not a token
reservation or a change to visible title length. Truncation still fails without
publishing a partial title, and the existing timeout and title lifecycle remain
authoritative. See the [title prompt](../../internal/agentharness/title.go)
and [automatic title input](../../internal/agentharness/automatic_title.go).
The opt-in [live DeepSeek test](../../runtime/thread_title_live_test.go) verifies
real provider output; deterministic prompt checks alone cannot establish model
language compliance.

## Active-history forks

A fork copies conversation history, never active tool execution. Before sealing
a copied unfinished turn, it pairs each pending ordinary tool call with a
canceled result explaining that the source thread still owns the execution and
outcome. This lets the child accept its first mission with valid tool history;
the original call and result continue normally in the source. Existing completed
results are preserved. This uses existing v12 facts and changes no schema. See
the [active-tool fork test](../../runtime/thread_active_fork_test.go).

## Failed-turn retry and queued interactions

`Retry` resumes a failed turn from its latest existing save point. Canonical tool
results remain in the next provider request, after repeated failures and restart;
no duplicate user input is appended. A completed-turn retry still regenerates
from its original admitted input. Unknown effects remain non-retryable. The
provider prefix boundary is the last model message projected at the checkpoint,
which can itself follow non-message context observations.

Queue operations append thread-level facts. Canonical turn readers verify their
ancestry while permitting those facts between entries of a waiting turn. An
answer after restart continues that turn, then normal completion drains its queue.
These fixes use existing journal facts and preserve schema v12.

See the [checkpoint retry test](../../runtime/thread_retry_checkpoint_test.go)
and [queued question restart test](../../runtime/thread_ask_restart_test.go).

## Effects and shutdown

Tool effects cross a durable one-shot authorization boundary. If the outcome
of a dispatched effect cannot be confirmed, Floret atomically closes every
unfinished tool and interaction, clears provider continuation, and fails the
turn with `effect_outcome_unknown`. The effect is never replayed and there is
no retry command. A terminal turn rejects late results.

`CancelInput.IncludeDescendants` opts into stopping the existing selected subtree,
including active children of an idle parent. Creation and stop admission share
the Host mutation lock, without waiting for provider or tool completion. Canceled
Create/Fork calls reject after acquiring that lock. Every descendant keeps its
normal immediate/graceful effect settlement and queue; unrelated roots are not
affected. The default remains selected-thread cancellation. See the
[subtree cancellation tests](../../runtime/thread_subtree_cancel_test.go).

`Delete` marks the whole active subtree as deleting, cancels provider and tool
work, waits for execution/effect drains, then commits the canonical tombstone.
After that commit, the actor releases its view, draft buffers, item indexes,
request cache, and execution references and leaves the service registry. Cold
lookups consult canonical thread/tombstone authority before installing an actor.
The small identity fence on any previously captured actor remains to
reject late mutations and effect dispatch through previously captured actors;
a failed deletion retains recoverable presentation. Late provider output and
late effect dispatch are rejected. `Host.Shutdown`
stops admission, canonically cancels active in-memory executions before closing
their actors, joins Host-managed work, and closes storage only after the join.
Every settlement path reuses an existing cancel request for the exact Turn/Run
under the actor lock, preserving its identity, source, mode, and timestamp.
Shutdown or an immediate stop during graceful cancellation cannot create a
second cancellation fact. A journal read failure stops settlement without
substituting a new request.
Pending tool approvals are resolved as cancelled and reject stale answers after
reopening. Already durable input waits and queued input remain available; an
uncertain dispatched effect retains `effect_outcome_unknown`. Settlement errors
are returned alongside storage close errors. A cancelled shutdown remains in
progress for a later caller. See the
[shutdown settlement regressions](../../runtime/thread_shutdown_approval_test.go).

## Views and subscriptions

`ThreadService.View` returns a complete replaceable `ThreadView` containing
ordered user, thinking, assistant, tool, and interaction items. `ViewVersion`
is process-local notification ordering. Publishers reject per-thread version
regressions. `Subscribe` is observation, not durable replay; a stale or closed
subscriber must reconnect and obtain a fresh baseline.
Streaming consumers use the optional [native update contract](thread-updates.md)
to avoid materializing complete history on ordinary text growth. Both APIs share
the actor, version and publication owner.

Validated tool calls enter the current view with their Activity presentation
before dispatch. Provider argument streams do not create running tool items.
The existing actor applies the Engine's changed Activity facts for pending,
approval, dispatch, output, and result changes, and publishes each changed
view without waiting for a journal refresh or the end of the Turn. Parallel
tools publish these mutations in reduction order. Canonical refresh retains
newer observed tool progress when it has only the older call record; terminal
turn settlement still uses the complete canonical journal. Hosts consume this
view rather than maintaining another tool lifecycle or argument cache.

Every `ThreadItem` and `ThreadInteraction` carries the exact `TurnID` and
`RunID` of its canonical journal fact. Historical items retain their original
run across later turns and same-turn input continuation. Missing or conflicting
execution identity fails the view read instead of producing an ambiguous item.
Domain migration permanently fills released v5 message rows that predate
durable message `RunID` when canonical lifecycle markers identify one exact
run segment. Runtime projection never infers a missing identity.

`ThreadView.Turns` and `HistoryPage.Turns` retain each admitted turn's exact
identity, kind, historical outcome, original start/end timestamps, elapsed time,
unioned user/approval waiting time, and work duration in milliseconds. Only
terminal turns settle durations; queue time is excluded and parallel waits are
deducted once. Model latency, tools, and retries count as work.

Accepted natural-stop response segments are recorded as canonical entry
references in terminal metadata and mapped to stable `FinalItemIDs`.
`FinalBoundaryComplete` means the full boundary was recorded, including a known
empty boundary. Length continuations remain one final response; tools and hook
continuation discard the preceding candidate. Legacy boundaries are unavailable,
never inferred from text, timestamp, or the last assistant message. Summary notifications cannot author response classifications.
Nested forks remap references and preserve original occurrence timestamps.

`ThreadItem.CanonicalOrdinal` and `ParentEvent.CanonicalOrdinal` (JSON
`canonical_ordinal`) are one-based source positions on the queried canonical
Path. Merge public items and parent events by this position, preserving item
order at ties; occurrence timestamps are display facts only. Tool updates retain
their first position. Zero means an uncommitted live item, whose tail order stays
unchanged. These positions are neither durable cursors nor execution identities.
Forks derive positions from their own Path and preserve all copied occurrence
timestamps; inherited event recipient identities remain unchanged.

`ThreadTurn.IntermediateItemIDs` (JSON `intermediate_item_ids`) contains ordered,
unique segmented assistant items superseded by a later local tool call, hosted
activity or semantic continuation in the same Turn/Run. Accepted final references
are disjoint. Length continuation alone does not supersede text. Trailing partial
answers and unsegmented legacy text remain unclassified. Hosts may fold these
confirmed intermediate items independently of terminal outcome; unknown prose
must remain visible. These are read projections, with no journal rewrite or
schema change (current domain schema 31).

Optional journal metadata was introduced without rewriting existing stores in
v7.29.0. The v7.29.1 migration advances the current domain schema to v14 and
retains those records and their canonical bytes after explicit marker identities
are validated.

`ThreadView.RunID` is the exact current execution identity and never aliases
`TurnID`. `RunProgress` is the actor-owned, process-local phase for active
provider and tool work. An active view with no unresolved interaction always
carries progress; the transition into or out of approval/user-input waiting
publishes the progress and interaction state together. Progress is cleared
while waiting for approval or user input and after terminal settlement; hosts
must not reconstruct it from items or persist a second progress lifecycle.
`ThreadSummary` carries the same bounded identity and progress projection for
thread lists.

Every new Run enters the actor through one atomic transition. It closes the
old live segment, installs the new Turn, Run, and logical-request identities,
resets the attempt epoch, and publishes `preparing` before provider work. The
first attempt is epoch 1. Later events must match the active Run and logical
request; only a higher epoch of that same request may replace the active
attempt. Late events from a waiting or completed Run remain rejected.

Terminal presentation settles from the canonical ordered journal. The runtime
publishes a terminal current only after that projection succeeds; a completed
turn without a visible assistant or terminal tool item is reported as a
failure rather than exposed as a successful user-only transcript.
`ThreadView.Failure` exposes the canonical terminal failure code and message.
Hosts classify terminal failures from `Failure.Code`; there is no parallel
text-error field.
`TurnResult.Output` remains the run-level aggregate result and is not appended
as a new assistant item. Different stable assistant IDs remain distinct even
when their text is equal. A canonical snapshot is applied only if the
in-memory view version is unchanged since the snapshot read began; this keeps
an older read from replacing a newer terminal view.

`ThreadService.List` reads root lifecycle facts from one canonical inventory
snapshot. `ThreadSummary` carries the current turn and run identity, attention, queue
count, pending input, typed terminal failure, and bounded last-item preview
needed by a host list. It does not hydrate complete timeline, attachment,
context, or SubAgent detail for every thread; those remain selected-thread
queries.

`ThreadContextReader.Context` keeps two scopes explicit: `Usage` is the latest
context-pressure status, while `UsageTotals` folds disjoint input, output,
cache-read, and cache-write counts from canonical final provider-usage entries
across the thread. Final usage reaches both projections through the same
attempt-scoped event decoder. Projected request estimates are never included in
totals.

After a final provider-usage context status is appended successfully,
`runtime.Event.ThreadUsageTotals` publishes the updated value from that same
canonical fold. It is absent from projected requests, stream usage, rejected
attempts, and uncommitted events. Hosts may use it for immediate display, then
reload `ThreadContextReader.Context` after reconnect; they must not accumulate
or persist a parallel total.

## Agent and provider boundary

`runtime.NewAgent` snapshots the configured profile, system prompt, gateway,
tools, effect policy, capabilities, context policy, and execution limits. The
provider gateway is the only model transport boundary. Provider credentials,
editable profiles, endpoint authorization, uploads before admission, and UI
rendering remain host-owned.

The host may return an Agent with a different provider, model, system prompt,
tool surface, or reasoning policy for a new Turn. The first provider checkpoint
freezes that complete surface for the Turn. Ask User, tool loops, retries, and
restart recovery reuse it exactly. Floret keeps the canonical path intact,
starts or resumes the surface's independent render lineage, and clears
provider-native continuation state across a surface switch.

Hosts that need live capability changes can explicitly return
`ToolSurface.RefreshProviderSurface: true` from `WithAgentDynamicToolSurface`.
Each subsequent provider request then uses current system text and local/hosted
tool definitions. Provider, model, reasoning, context policy and compatibility
identity remain fixed by the first Turn checkpoint. Changed envelopes establish
a new render lineage and clear provider continuation state without compaction
or rewriting earlier checkpoints. The default remains the frozen Turn surface.
Dispatch-time permission resolution and existing approvals remain independent:
refreshing model-visible tools does not authorize an invocation or settle a wait.
`ToolSurfaceRequest.InitialProviderSurface` supplies a detached copy of the first
checkpoint's system text and tool definitions, including after restart; it is nil
before the first request. Hosts can preserve unrelated capabilities from this
snapshot while refreshing only their live policy. It contains no executable
handlers and does not authorize tools or create a second persistence contract.

The provider-context v6 projection establishes one explicit render boundary
for earlier projectors, including v5. Canonical lineage hashes durable typed
conversation facts rather than journal entry and parent identities, which may
be assigned differently during an active tool loop and later canonical
reconstruction. The first request after a projector change uses a new render
lineage and the complete typed canonical history. It does not create a
compaction, rewrite the journal, or reuse old provider continuation state.

`ThreadContextReader` treats each committed Turn policy as the boundary for the
legacy `Usage` sample. Until that Turn records its first request status,
the snapshot exposes the new provider, model, and policy without carrying the
previous Turn's usage. Canonical whole-thread token totals remain cumulative
across the boundary.

A provider natural stop completes the Turn. Ordinary tool results continue the
provider loop, while `ask_user` waits and resumes the same frozen Turn. A tool
call whose definition is unavailable returns a safe ordinary error result and
continues. Historical tool calls and results remain typed and paired even when
their definition was removed; only current definitions are exposed for new
calls. Public Ask User answers appear once in the paired tool result. Secret
answers leave a redacted result and reach only the current resume request as an
ephemeral overlay.
Use Ask User for required execution input and requested guided questions. Ordinary
discussion and optional feedback may use prose without opening an interaction.

Pending Ask User input requires a validated, non-error control signal with at
least one complete question. A failed projection terminates the Turn and never
creates an interaction. Current views, summaries, live events, and restart
recovery apply the same classification; historical `waiting + control_error`
facts are read as failures and receive a safe paired provider error result.

## Durable schema

The internal session-tree domain has a permanent contiguous v2 -> v3 -> v4 ->
v5 -> v6 -> v7 -> v8 -> v9 -> v10 -> v11 migration lineage. `runtime.Open` runs domain migration, logical schema
update, and final invariant verification in one backend transaction. Unknown,
future, corrupt, or drifted state fails closed without changing canonical
records. `runtime.Options.StartupProgress` reports only the product-neutral
`migrating` and `verifying` phases; it exposes no record counts or content.
Startup selects one exact source format, rejects mixed authority, decodes an
unchanged current-v11 store once, and performs a persisted final verification
after every startup write. Current-schema startup is byte-preserving and
idempotent. The v5 -> v6
edge first replays pending v5 recovery frames and accepts only the exact v5
tool-result Raw repair produced before UTF-8 normalization was enforced. It then
writes segmented v6 authority and removes the legacy checkpoint, full-path root
inventory, and diff journal in the same transaction. The v6 -> v7 edge
permanently fills exact run identity, removes retry authority, and terminates
active unknown effects before the Host becomes available. The v7 -> v8 edge
classifies only the exact Engine continuation user message paired with its
`context_continue` save point as a control signal, restoring one canonical user
input per Turn. Every other mismatch
still fails closed. The v8 -> v9 edge moves context execution identity to the
canonical entry and renames current context kinds. A Turn resumed after an
interaction follows the latest canonical entry Run instead of the Turn's
initial Run. A historical mismatch is repaired only when the payload Thread is
a verified fork ancestor and the entry exactly matches the canonical fork
source copy; other drift fails closed.
This automatic domain convergence is separate from the explicit
legacy physical conversion
surface; normal startup never dual-reads or converts that external schema.

Startup errors from internal session-tree authority validation are exposed
through `runtime.ErrAuthorityCorrupt`; hosts do not inspect internal errors or
physical records to classify them.

## Destructive storage reset

`PreflightStorageReset` and `ResetStorage` are the only public destructive
maintenance boundary for Floret-owned records. The caller must first stop the
`Host`, validate the exact environment and target through its own ownership
manifest, and pass that environment ID, operation ID, and manifest digest to
Floret. A backend without `storage/spi.MaintenanceResetter` fails closed.

Reset never reads, exports, or migrates prior domain records. A supported
backend removes all opaque Floret records through one backend-owned
transaction; Floret then initializes and verifies the current schema. The
operation is idempotent. Failure after old records are cleared leaves no old
authority to reopen, so the host must keep readiness closed and retry reset or
initialize a new store instead of falling back to normal startup.

## Packages

Use `identity`, `config`, `runtime`, `observation`, `tools`, `provider`, and
opaque `storage.Source` values. Downstream applications must not import
`internal/*` or infer lifecycle state from physical storage records.

The v9 -> v10 edge admits optional durable model context and typed validation
feedback without modifying old canonical bytes. A v9 source containing v10
fields is rejected. Provider projection v9 explicitly resets prior projection
lineages; an admitted retry uses its canonical source entry to establish a new
boundary once, then resumes normal prefix checks. Missing ephemeral history is
not fabricated.

The v10 -> v11 edge validates the exact old authority and preserves every journal
byte. New stop facts require source, mode, and exact execution identity; a canceled
effect may reference a confirmed canonical canceled result. These shapes are
rejected under v10. Restart seals pending cancellation without re-executing tools.

`ThreadContextSnapshot.ContextUsage` and `runtime.Event.ContextUsage` add a
shared projection of committed context records without changing schema v12.
`Confirmed` is the most recent native measurement; `Estimate` is the current
prediction, including unavailable-native final observations. Same-model,
same-budget Turns retain confirmed usage. Model or budget changes and successful
compaction clear samples; failed, cancelled, and noop compactions preserve them.
Live publication follows canonical commit. Reconnect and restart use the same
fold. A present empty snapshot clears prior samples; absent samples are unknown.
The existing `Usage` and `ContextStatus` remain latest-status observations for
v7 consumers. Hosts must not use a prior confirmation to hide current pressure.

## Context budget failures

`ThreadTurnFailure` distinguishes `context_budget_invalid`,
`context_fixed_overhead`, and `context_compaction_limit` from engine-contract
failures. Output headroom must leave positive input capacity. Known fixed input
that cannot fit must stop before automatic compression; compressible history
continues through the existing compaction path. Diagnostics contain numeric
budgets, never request bodies. Live views, summaries, and restart reads preserve
the canonical failure. Hosts may offer configuration changes and explicit retry;
no automatic retry or secondary lifecycle is introduced. Schema v12 is unchanged.
See [budget guards](../../internal/engine/context_budget.go) and
[restart and retry coverage](../../runtime/context_budget_test.go).

Conditional queue removal uses `DeleteQueuedInput.ExpectedInputSHA256` to compare the complete pending input inside the runtime owner. Missing, started or changed input returns `ErrRequestConflict`; exact committed retries succeed after restart. This additive public API supports host take-back-to-draft flows without introducing another durable owner or changing schema 15.

### Immediate steering

The optional `ThreadSteeringController` accepts full `UserInput` through `Steer`, or atomically consumes exact queued content through `SteerQueued`. Active executions keep their TurnID and RunID; suspended input uses the existing continuation boundary and a new RunID. One thread actor owns admission, fragment cancellation and completion sealing. Requests are durable typed journal facts, with one reference-only applied or cancelled resolution. Pending instructions are excluded from model history. Provider attempts remain fenced; confirmed tool results keep their existing finalizer, while unknown side effects terminate without replay. Stop always wins over deferred continuation. Standalone compaction returns `ErrThreadBusy`.

## Provider recovery progress

`WithAgentProviderRetryPolicy` validates an explicit `ProviderRetryPolicy`.
`DefaultProviderRetryPolicy` describes the shared defaults. `Event.Retry` and
`ThreadRunProgress.Retry` contain detached `ProviderRetryProgress` snapshots,
including absolute `NextAttemptAt` for reopening views and local countdowns.
The thread stays active and existing cancellation/steering remains authoritative.
See [provider recovery ownership](../decisions/provider-retry.md).

## Managed runtime facts

`TurnInput.ModelContextSnapshot` and `ToolSurface.ModelContextSnapshot` admit
complete keyed facts. Collect at logical provider preparation or manual compaction
using the same resolved permission state. `ThreadContextSnapshot.ModelContext`
reports admitted facts and latest request usage. Required failure wraps
`runtime.ErrModelContextUnavailable`; safe Retry follows normal effect rules.
See [ownership and compaction](../decisions/managed-model-context.md).

## Answer Fork Anchors

Hosts pass an optional SourceItemID to ForkThreadInput to fork after a committed assistant item on the source active path. Floret resolves the presentation ID internally; hosts never decode or copy journal records. SourceEntryID and SourceItemID are mutually exclusive. Unknown, foreign and non-assistant items fail without creating a destination; request keys retain exact-anchor idempotency. The last final assistant item includes its canonical completion marker; earlier items keep their exact cut. The domain schema remains 17.

## Passive parent events

The additive `ThreadParentEventService` is optional, explicitly enabled by
`Options.EnableParentEvents`, and leaves required `ThreadService` unchanged.
Flower owns model tool definitions and host authorization; Floret owns direct
parent identity, canonical source receipts, admission, and detached event reads.
Reports verify active authorized invocation identity, accept nonempty UTF-8 up
to 8 KiB, and deduplicate exact replay. Changed content or forged authority fails.

Only terminal outcomes, typed execution timeouts, unresolved input/approval and
explicit reports produce events. Receipts do not run, steer, cancel, respond or
recover a parent. The next natural provider boundary admits one frozen bounded
message after tool pairing; natural completion never forces a drain. Per-event
and per-batch limits include framing, prioritized unresolved interaction and
unknown-effect references, and a closed overflow range readable with explicit
pagination. Full reports are stored once; full handoffs remain on existing reads.
Use sufficient untruncated reports directly and read report events only when
needed. Completion notifications do not contain final reports; hosts expose
result reads and should reuse results already available for the same turn.
`tools.SubAgentOperationStatus` distinguishes metadata-only checks from
inspection and waiting without adding durable read tracking or changing
existing actions. Event reads remain available for explicit audit.

`ThreadView.ParentEvents` supplies bounded metadata and explicit pagination
separately from streamed item ordinals. Successful pages preserve an allocated
empty event array through public clones and JSON; disabled parent events remain absent. `Admitted` identifies canonical context
placement, not proof of model reading. Existing subscriptions are observation
only; hosts subscribe before checking pending events. List/read never wake an
unhydrated thread. See [storage lineage](storage.md) and the
[public integration contract](../../README.md#passive-child-events).
`ParentEventPage.ThreadID` (`thread_id`) identifies the queried canonical thread
path on every successful list, read and current view, including empty or filtered
pages. A fork inherits events without rewriting their original `ParentThreadID`;
that recipient can differ from the page identity. Hosts validate page scope
against the requested thread and authorize source navigation independently.

ParentEvent.ParentTurnID (JSON parent_turn_id) identifies the most recently
started ordinary parent turn preceding receipt in canonical Path order. It
remains that turn after termination until another ordinary turn starts; context
compaction does not advance it. No preceding ordinary turn means the field is
omitted. Forks retain inherited ownership and original recipient identity.
List, full read, pagination and restart derive the same field without changing
historical bytes or admission. Domain schema remains 22. Hosts own presentation.

## Bounded thread observation and failure reasons

The optional public `ThreadWaiter` freezes scoped targets at the first read,
subscribes before reading, and re-reads canonical current state on wakeup.
Missing targets take priority over pending interactions, then terminal state;
a single elapsed deadline returns the latest successful snapshot. Cancellation
and Host closure remain distinct errors. Hosts map attention to completed-tool
input requests and resume only after explicit user input.

`ThreadTurnFailure.Reason` is derived from canonical code and exact Engine
messages for both live and historical reads. It is not another persisted ledger
or a migration. ThreadTurnFailureToolArgumentCorrectionExhausted identifies
both tool_dispatch and control_error correction exhaustion, including exact
v7.69.1 historical Engine messages. Raw validation details are not safe UI copy.
Explicit Retry starts a new Run and retains confirmed results; unknown effects
keep their existing no-replay boundary. The subsequent domain 29 -> 30 edge segments canonical body storage.
See [runtime source](../../runtime/thread_wait.go) and
[failure classification](../../runtime/thread_failure_reason.go).

## Visual Compaction And Media Locators

`MessageAttachment.Locator()` hashes the complete descriptor using versioned,
fixed field encoding and returns `media:v1:<sha256>`. This additive public method
supports authorized host rereads without exposing resource refs in model text.
It does not change schema 19 or attachment persistence. Forks preserve locators.
`ThreadItem.Attachments` projects user and confirmed tool-result attachments;
Activity target refs are presentation, never canonical membership.

The [summary generator](../../internal/engine/compaction/provider_summary.go)
uses one ordered text/image sequence, UTF-8 fragments, atomic image source labels,
complete rendered budgets and frozen retry/refit handles. Summary requests have
no tools or provider continuation. All input must be covered before checkpoint
installation. Summaries retain relevant visual facts with locators and uncertainty;
checkpoints direct exact-detail questions back to original snapshots. Old
checkpoints are not rewritten or retroactively supplied with visual facts.

[Scoped history queries](thread-query.md) discover older images without a
second attachment index or an unbounded checkpoint catalog. Hosts own bytes and
current authorization. Floret owns descriptors, canonical membership and summaries.

Compaction records optionally carry CompactionDecision with the frozen trigger and
validated replacement request fingerprints, estimator diagnostics and pressure.
Only Engine produces these facts; hosts do not recompute historical requests.
Missing fields remain unavailable. Manual preflight has no trigger candidate.
The canonical terminal turn settles an interrupted running observation so restart
cannot strand a completed operation in a running state.

Completed ordinary tool activity retains identity, status and exact canonical
source locators, rather than a full typed payload in every actor/reducer.
Explicit full API and legacy observers reconstruct before freezing publication.
Bounded windows return ordered ActivityBodies and use ThreadBodyActivityJSON
when the combined source exceeds their preview budget. A consumer reads each
complete field, verifies SHA256, then applies tools.MergeActivityPresentations
in source order. Active and derived Goal activity retain existing ownership.

List and Summary read existing actors through the same accepted-state owner
before any canonical path. Preview/time positions track explicit item changes;
only reorder/removal boundaries rebuild them. Summary reads visit pending input
controls rather than resolved historical interactions. Unloaded summaries remain
canonical path reads and do not create actors; cold summary indexing is not yet
complete. The fixed-history summary benchmark isolates this actor kernel and
must not be presented as storage or UI qualification.
