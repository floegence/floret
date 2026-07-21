---
type: Public API
title: runtime Package
description: The runtime package is the public capability surface for hosted threads, child threads, stores, events, control signals, and Floret-owned context lifecycle.
resource: /runtime/runtime.go
tags: [api, runtime]
timestamp: 2026-07-20T00:00:00Z
---

# Summary

`runtime` is the primary downstream integration package. Provider-backed work
uses a thread-bound `TurnExecutionHost`, a thread-bound
`ThreadCompactionHost`, or a parent-bound `SubAgentHost`. Provider-free
lifecycle transitions use `ThreadCreateHost`, `ThreadTitleHost`,
`ThreadForkHost`, `ThreadDeleteHost`, `ThreadReadHost`,
`SubAgentReadHost`, `PendingToolRecoveryHost`, and
`InterruptedTurnRecoveryHostFactory` / `InterruptedTurnRecoveryHost`. Active pending settlement stays on the exact
`TurnExecutionHost` or `SubAgentHost` owner. Hosts provide product input, tools,
permissions, and optional model transport; Floret owns
provider loop execution, provider-visible context assembly, trimming, summary
generation, compaction checkpoints, continuation state, and lifecycle
observations.

Downstream packages that need substitution define local interfaces containing
only their actual capability methods. Floret does not publish a repository-wide
host interface for every runtime operation.

# Main Entry Points

* `ConfigureHostCapabilities` opens the Store's only bootstrap callback and
  seals its `HostBootstrap` before returning. The Store rejects reconfiguration
  and value copies, so no reusable cross-family issuer or bootstrap survives
  configuration. Responsibility-specific binders may remain at the composition
  root and become active only after a successful callback; error and panic paths
  revoke every binder created by that attempt.
* `NewTurnExecutionHostBinder`, `NewThreadCompactionHostBinder`, and
  `NewSubAgentHostBinder` create narrow issuers. Their `Bind` method fixes one
  root `ThreadID` or parent `ThreadID` and returns a factory whose
  `NewHost(ctx, options)` first verifies that exact canonical authority, before
  provider configuration, skill discovery, event emission, or tool registry
  mutation. Its options contain provider/runtime configuration only.
* `NewThreadCreateHostBinder`, `NewThreadTitleHostBinder`,
  `NewThreadForkHostBinder`, `NewThreadDeleteHostBinder`, and
  `NewThreadReadHostBinder` create provider-free issuers for one named
  responsibility. Except for create, their `NewHost(ctx, ...)` methods validate
  the exact canonical root or parent before returning a durable handle.
  SubAgent reads use their parent-bound binder.
  `PendingToolRecoveryHostBinder` exposes separate root-thread and SubAgent
  parent methods. `InterruptedTurnRecoveryHostBinder.BindThread` and
  `BindSubAgent` snapshot one exact root or parent-child turn owner and
  generation into `InterruptedTurnRecoveryHostFactory`; its `NewHost` may
  refresh only heartbeat and expiry for that stable target. Before permanently
  resolving a disappeared or replaced target, Floret atomically validates its
  canonical admission and finish ledgers. Recovery authority has no mixed or
  empty identity shape and cannot follow a later turn.
* `NewMemoryStore` creates an in-memory runtime store for tests or ephemeral use.
* `OpenSQLiteStore` creates Floret-managed durable runtime storage. Bootstrap
  code invokes `ConfigureHostCapabilities`, retains only selected narrow
  binders at the composition root, distributes only exact bound factories or
  handles, and keeps Store lifetime ownership. Store close rejects new
  operations, cancels Store-owned executions and automatic-title workers, waits
  for terminal finalization, and then closes the backend. SQLite opens the exact
  v16 schema, upgrades exact v14 stores through v15 to v16, upgrades exact v15
  stores to v16, and upgrades only an exact empty v13 predecessor under one
  early write lock. Older, unknown, corrupt, or fingerprint-mismatched stores
  are rejected without mutation through `UnsupportedStoreSchemaError`.
* `ThreadCreateHost.CreateThread` is the only top-level public operation that
  creates a missing canonical journal. Its binder fixes the exact `ThreadID`
  and `CreateIntentID` before the handle reaches the create coordinator.
  Creation is idempotent only for that immutable identity and fingerprint and
  returns transcript-free `ThreadSummary` lifecycle metadata. There is no
  second start-or-create alias.
* `ThreadReadHost.ReadThread` returns a transcript-free `ThreadSnapshot`,
  including canonical status, latest turn and run identity, and the journal
  `ThroughOrdinal`. The snapshot intentionally has no message shortcut.
* `ThreadReadHost.ReadArtifact` reads the immutable content for one
  `ArtifactID` owned by its exact bound root thread. `SubAgentReadHost.ReadArtifact`
  performs the same read for any complete descendant of its bound parent. Each
  operation validates thread authority and reads the artifact atomically from
  the Store; missing artifacts return `ErrArtifactNotFound` with a zero
  `ArtifactContent` result.
  `ArtifactRef` contains only Floret-owned identity and safe metadata. It has no
  URL or filesystem path: downstream hosts construct authenticated routes from
  their already-bound root or SubAgent read capability instead of persisting a
  second artifact mapping.
* `ReadThreadOverview` returns that `ThreadSnapshot` together with the optional
  latest admitted `ThreadTurnSnapshot`, both projected from one active-path read.
  An unadmitted started marker does not fabricate a latest turn.
* `ThreadReadHost.ListThreadTurns` returns canonical turn snapshots in ascending
  journal ordinal order. Initial `Tail`, historical `BeforeCursor`, and
  incremental `SinceCursor` pagination are mutually exclusive. Cursors carry a
  canonical entry identity rather than a host-derived ordinal; a cursor that is
  no longer on the active path returns `ErrStaleThreadTurnCursor`. Started,
  waiting, or terminal
  markers do not make a turn public by themselves; the turn enters the page only
  after its canonical user entry is committed. The corresponding public
  `thread_entry_committed` event is emitted only after this read returns the
  same running turn, so a host may synchronize presentation from the public
  read capability before handling provider or assistant events. Each returned
  turn includes its explicit run identity, canonical user entry, input,
  attachments, ordered references, retry-source identity, typed failure,
  verified control signals, complete `ThreadTurnProjection`, and
  projection-through ordinal.
* `ThreadReadHost.ReadThreadAgentTodos` reads Floret-owned typed Agent todo
  state. `TurnExecutionHost.UpdateThreadAgentTodos` updates that state with
  compare-and-swap versioning and must reference a real journal turn, run, and
  tool call. Fork rewrites the stored turn/run authorship with the journal, and
  thread deletion removes the state.
* `TurnExecutionHost.RunTurn` executes one hosted user-facing turn and rejects a
  request whose `ThreadID` differs from the handle binding.
* `RunTurnRequest.Input` is `TurnInput{Text, Attachments, References}`. At least
  one field must be present. Each `MessageAttachment` carries an opaque durable
  `ResourceRef`, name, MIME type, and non-negative size; Floret persists and
  projects the attachment but never resolves or reads the host resource.
  `MessageReference` is an ordered, durable, user-visible closed union of
  `text`, `file`, `directory`, `terminal`, and `process`. It carries a stable
  message-local `ReferenceID`, label, optional display text, truncation fact,
  and an opaque host resource identity for file/directory references. Floret
  validates public limits before admission and never interprets the opaque
  resource identity as authorization.
* `RunTurnRequest.Limits.MaxInputTokens` caps cumulative provider input tokens
  across every model request in one run. `MaxTotalTokens` independently caps
  cumulative total tokens; provider/context output limits remain per request.
* `RunTurnRequest.RunID`, `ThreadID`, and `TurnID` are required. `RunID`
  identifies the concrete engine/provider execution and must be supplied
  explicitly rather than inferred from the turn identity.
* `RunTurnRequest.ManualCompactions` lets a host request manual context
  compaction for an active hosted run. Floret polls it at provider-loop safe
  points, emits compaction lifecycle events, and continues the same run.
* `RunTurnRequest.SupplementalContext` carries host-provided current-turn
  context items. Floret normalizes and validates the complete rendered payload
  before a new admission, then renders it into provider requests for that turn
  only without modifying `Input`, durable thread history, working directory,
  permission state, tool approval state, cache/raw plans, request-response
  ledgers, or opaque provider continuation state. A reference-only root input
  requires renderable supplemental context for its current provider turn;
  entry points without a supplemental-context contract reject reference-only
  input. Exact replay is read-only and ignores changed or missing supplemental
  context. Hosts own source policy, redaction, and intentional truncation before
  passing those items to Floret.
* `ThreadCompactionHost.CompactThread` runs a compaction-only maintenance
  operation for its bound idle thread through `CompactThreadRequest` without creating natural
  assistant output. `RequestID` and `Source` are required, and the result carries
  one validated terminal `observation.CompactionEvent` with exact thread, run,
  request, operation, and source identity.
* `ThreadReadHost.ReadThreadContext` returns the canonical
  `ThreadContextSnapshot` projected from the active journal path for a top-level
  thread. Child context is available only inside parent-scoped SubAgent detail.
  The snapshot contains resolved provider/model,
  `config.ContextPolicy`, latest `ContextStatus`, typed compaction events, and
  `UpdatedAt`; malformed or identity-conflicting journal data returns an error.
* `SubAgentHost` owns `SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, and
  `CloseSubAgent`; `SubAgentReadHost` owns list/detail/activity reads. Every
  capability is bound to one parent and first requires that the
  canonical parent journal still exists; retained child metadata cannot keep an
  orphaned SubAgent operational after its parent is missing.
* `ListSubAgentActivityTimeline` returns a parent-scoped,
  product-neutral `observation.ActivityTimeline` for hosted child lifecycle
  status without exposing child transcripts. Its payload includes durable
  child-thread identity as `thread_id`, but not
  downstream product actions, routing targets, or UI runtime labels.
* `ReadSubAgentDetail` lets a host read a parent-scoped, paginated child-thread
  execution timeline for human UI or audit surfaces without expanding
  `WaitSubAgents` payloads. Its top-level
  `activity_timeline` is the canonical current activity projection rebuilt from
  retained child detail events; paginated rows are ordered journal facts rather
  than live tool-state snapshots.
  Child and root detail rows use the same `ThreadDetailEvent` contract; parent
  scoping remains enforced by the child-detail request.
  Its top-level `context` block exposes neutral model-bound facts: provider
  and model identity, model-derived context policy, current context
  pressure/usage status, and public compaction lifecycle operations. Context
  window size comes from the resolved model capability and policy, not from
  parent thread, child thread, subagent, or fork mode.
* `ListThreadDetailEvents` lets a host read the Floret-owned ordered execution
  transcript for a hosted thread without reading Floret storage internals.
* `ReadLatestThreadTurn` returns the latest admitted turn from the active path
  without requiring hosts to cache or replay the complete journal.
* `SetThreadTitle` is the only host title write contract. It stores a non-empty,
  single-line title of at most 200 Unicode characters as a canonical host title,
  is idempotent for the same value, and emits `thread_title_updated` when the
  value changes. In provider title mode, Floret begins a durable automatic-title
  generation immediately after first user admission and runs it concurrently
  with the main provider turn. `ThreadSnapshot.TitleStatus` exposes
  `pending`, `ready`, or `failed`; a manual host title wins the generation race.
  Reference-only title input uses only canonical labels, never reference text or
  opaque resource identity.
* `ProjectThreadTurn`, `ReadTurnProjection`, and `TurnResult.Projection` expose
  the product-neutral ordered assistant text, activity timeline, and
  control-signal segments for a hosted turn.
* `ReadApprovalQueue` returns the one durable product-neutral approval queue for
  a root and its canonical descendants. Only `Current` is decisionable;
  `ResolveApproval` requires its exact generation, revision, approval identity,
  and a stable `DecisionID`. The same decision replay is idempotent, while stale
  or mismatched decisions return a conflict without calling the host gate.
* `CompletePendingTool` requires the public completion `RunID` and uses it as
  the follow-up execution identity.
* `SettlePendingTool` records a host-owned pending tool outcome as a
  detail/activity event for the original turn without creating provider-visible
  context or running another model turn. `PendingToolSettlementTarget` carries
  the complete thread, turn, run, tool call, tool name, and handle identity; the
  result echoes that target and validates it against the settlement event and
  projection. Settlement is idempotent for the same target and status, rejects
  conflicting status for an already settled target, and may arrive before the
  original pending tool result row when host-owned work finishes immediately
  after the tool call is exposed.
* `tools.PendingToolResult.Handle` is the provider-visible continuation token.
  Pending metadata remains observation-only and is not rendered as a model-facing
  tool result field.
* `DeleteThread` removes a Floret-owned thread tree from the engine store,
  including child threads, prompt cache scopes, and artifacts.
* `ErrThreadNotFound`, `ErrTurnNotFound`, `ErrRunNotFound`,
  `ErrArtifactNotFound`, and `ErrSubAgentNotFound` are public sentinel errors
  for `errors.Is` checks on capability not-found responses.
* `ErrInterruptedTurnNotFound` means a validated live exact root or
  parent-child target has no active turn lease, so bind creates no recovery
  target. `ErrRecoveryTargetResolved` means an already-bound exact target's
  lease disappeared or moved to a valid higher generation; the coordinator
  permanently finishes that target and never follows replacement authority.
* `ErrThreadNotActive` reports that an active-derived pending settlement handle
  no longer owns an active thread. It never falls back to recovery admission.
* `ErrThreadBusy` reports that an active turn or another canonical mutation owns
  the thread. `ErrNoRetryTarget` reports that no canonical turn is eligible for
  retry. Hosts branch on these sentinels rather than importing harness or journal
  lease errors.
  `AuthorityBusyError`, inspected with `errors.As`, classifies turn versus
  structural/mutation authority without exposing owner identity.
* `ErrPendingToolNotFound`, `ErrPendingToolNotActive`, and
  `ErrPendingToolSettlementConflict` distinguish an unknown tool-call target, a
  target that is not an active pending result, and a target that was already
  settled differently. `ErrSubAgentClosed` reports a child mutation attempted
  after canonical close. All support `errors.Is`.
* `ErrSubAgentParentRequired` reports that a root capability was used for a
  parent-owned child thread. `ErrThreadAuthorityInvariant` reports malformed,
  missing, cyclic, or conflicting durable root/SubAgent authority metadata.
  Both support `errors.Is`.
* `ErrJournalInvariant` reports that resume found ambiguous active-path state
  and refused heuristic repair. `ErrAgentTodoVersionConflict` reports a stale
  todo CAS update.
* `ErrRequestConflict` plus `RequestConflictError` reports immutable request-ID
  reuse with changed input without exposing stored payloads. Store open errors
  use `UnsupportedStoreSchemaError` and `StoreLeasePolicyMismatchError` for
  typed, non-destructive compatibility handling.
* `TurnResult.ProjectionAvailability` reports `ready` or `unavailable`
  independently from execution error. An unavailable projection preserves
  terminal engine facts and carries diagnostic text in `ProjectionError`.
* `ModelGateway` lets a host supply model transport through provider-backed
  capability options while Floret owns loop control, tool dispatch, context
  lifecycle, and ledgers.
* `ModelGatewayIdentity` supplies provider/model names and a required,
  non-sensitive `StateCompatibilityKey` for `ModelGateway` requests, ledgers,
  and opaque continuation compatibility. Gateway-backed hosts must not set
  provider transport fields such as provider, model, base URL, API key, or fake
  response in the capability `Config`; pass the raw non-transport runtime
  config to the selected constructor and put transport identity only in its
  `ModelGatewayIdentity`.
* `ToolSurfaceProvider` lets a host refresh the provider-visible local tools,
  hosted tools, system prompt, and host context at provider-loop safe points
  without adding product-specific policy concepts to Floret.
* `ReasoningSelection` carries provider-neutral reasoning intent for a run.
  `RunTurnRequest` carries it as turn intent, and `ModelRequest` forwards the
  effective selection to host-owned model gateways.
* `ModelEventToolCallStart`, `ModelEventToolCallDelta`, and
  `ModelEventToolCallEnd` expose model tool-call streaming as provider-neutral
  public runtime events. They identify the call being generated without carrying
  argument text; `ModelEventToolCalls` remains the final executable batch.
* `ModelRequest.Messages` uses typed roles, assistant text/reasoning, grouped
  `[]tools.ToolCall`, and one typed tool result per tool message. Floret groups
  parallel calls and validates call IDs, JSON arguments, result order, names,
  and adjacency before invoking the gateway. Gateway adapters map this contract
  directly to provider wire format and must not regroup, deduplicate, reorder,
  drop, or repair messages.

# Boundaries

The host should treat `runtime.Store` as opaque. Product data such as owners,
workspace metadata, pinned state, billing, and read watermarks belongs in the
host database keyed by `runtime.ThreadID`.

Floret Store data includes the admitted conversation journal, turn/run
lifecycle, canonical titles and typed failures, ordered user references,
projections, verified control payloads, approvals, Agent todos, prompt-cache and
provider ledgers, tool artifacts, context lifecycle, and opaque provider
continuation. Hosts must not query Store tables, copy these facts into product
storage, or persist a second conversation, run, title, reference, todo,
approval, or model-visible message projection. A host may map public snapshots
to a response or in-memory UI DTO, but that mapping is not another durable
engine fact source.

Subagents are parent-managed child threads, not a graph workflow framework and
not host-owned pending tool work. Each child uses its own durable `ThreadID` and
prompt scope; host products own agent profiles, permission policy, UI, and
orchestration prompts.

When `TurnExecutionHostOptions.ModelGateway` or
`SubAgentHostOptions.ModelGateway` is set, hosted parent and child turns use
the supplied model transport. Title selection remains host-owned unless the host
explicitly selects `ThreadTitleModeProvider`; only that mode sends title requests
through the same gateway, while both modes persist through Floret. The gateway is invoked with the concrete runtime
identity for each request, so a child turn uses the child `ThreadID` and prompt
scope. Provider/model identity comes from the selected capability's
`ModelGatewayIdentity`; gateway-backed hosts pass raw non-transport config to
the selected constructor and keep provider transport configuration out of its
`Config`.
Provider-visible user messages carry attachment resource references through
`ModelMessage` so the host adapter can resolve them into provider-native image or
file content. Opaque attachments are rejected before admission when no
`ModelGateway` exists; they never degrade into filename text.
Canonical `MessageReference` values are public conversation facts, not automatic
provider-history content. The current turn receives any richer model material
only through validated `SupplementalContext`. Floret strips references from
provider history, cache input, raw plans, ledgers, summaries, checkpoints, and
opaque continuation state. Downstream hosts render public references and resolve
file/directory `ResourceRef` values only after rereading the exact canonical
thread, turn, user entry, and reference under current product authorization; the
display label or text is never access authority.

Child `ThreadID` is the lifecycle target for spawn, send, wait, list, and close.
Task names, task descriptions, and agent paths are reference metadata and may
repeat. `SubAgentSnapshot.TaskDescription` records the parent-authored
responsibility or objective for the child as neutral lifecycle metadata; product
UI actions, routing ids, and display copy remain host-owned. Queued child inputs
are journal entries in the child thread, so host restart and storage backends
preserve pending work, cancellation, and consumption state.
After a provider-backed host process restarts, `SubAgentReadHost.ListSubAgents`
is the canonical public reload source for a parent thread's child-thread list;
hosts must not inspect Floret storage tables or rebuild child identity from
transcript display projections.

`SubAgentSnapshot.ForkMode` is the engine-owned fork contract for a child
thread. `none` starts the child with only the delegated mission, while
`full_path` forks the parent thread path into the child thread. Hosts may map
this to product UI terms, but should not persist a second engine fork state.

`WaitSubAgents` is a bounded lifecycle wait. Its default wait is five minutes
and requests are capped at twenty minutes. Timeout returns snapshots and
`TimedOut=true`; it does not close, delete, or hide child detail. Child run
execution also has a configurable host timeout, defaulting to twenty minutes, so
a stuck child turn can be stopped while preserving its thread and journal.

Stopping a parent run is an execution lifecycle decision, not a data retention
decision. Hosts that stop parent work should call `CloseSubAgents` for the
parent thread. Floret cancels unfinished child turns, cancels queued child
inputs, marks affected children closed, writes lifecycle detail, and keeps the
child histories readable. Completed, failed, cancelled, or already closed
children remain historical records.

Subagent detail reads are scoped by both parent and child `ThreadID`; a host
must prove parent ownership before a child timeline can be read. Detail events
are projected from the child thread's current journal path in ordinal order and
cover delegated input, messages, tool calls, tool results, approvals, turn
markers, lifecycle stops, compaction checkpoints, and run failures. This API is
for host UI and audit inspection. Hosts should keep model-facing subagent tool
results bounded and should not inject the full detail timeline into a parent
model context by default.

Detail pagination is bounded by a default and maximum limit and returns
`HasMore` plus `NextOrdinal`. By default, detail events expose bounded,
sanitized previews plus hashes, metadata, truncation facts, and artifact
references. Raw message content, reasoning, tool arguments, and full tool result
content are omitted unless a host sets `IncludeRaw` for an explicitly authorized
human/debug inspection surface, and hosts should not reuse that raw response as
model-facing tool output. Each projected row may also carry a canonical
`observation.ActivityTimeline` generated by Floret. Tool result details expose a
structured `status`; hosts should use that status and the row timeline instead
of inferring execution state from preview or result text.

Thread detail reads expose the current hosted thread journal path in entry
ordinal order. They cover user messages, assistant messages, tool calls, tool
results, turn markers, compaction checkpoints, approvals, custom entries, and
run failures. This is Floret's public read model for durable execution
transcript facts. `ThreadTurnProjection` is the public display projection over
those facts: `RunTurn`, `RetryTurn`, and `CompletePendingTool` return it on
`TurnResult`, `SettlePendingTool` returns it on `PendingToolSettlementResult`,
`runtime.Event.Projection` carries the current live projection on committed
thread-entry events, and hosts with a known `ThreadID`, `TurnID`, and `RunID`
may call `ReadTurnProjection` to rebuild the turn display projection from
durable Floret detail after reload. Runtime live, turn-result,
pending-settlement, and read-back projections are canonical current-turn
display projections built by the Floret host from raw-capable journal facts;
the default preview-only detail read model is for listing and inspection, not
for authoritative assistant markdown.
`ThreadTurnProjection.ThroughOrdinal` is the maximum durable detail-event
ordinal included in the projection. Hosts compare it only within the explicit
`ThreadID`, `TurnID`, and `RunID` identity tuple to reject duplicate or stale
updates. `ProjectedAt` records projection time but does not define ordering. A
projection whose turn-start marker is durable reports `Status=running`; the
status changes only when a completed, waiting, failed, or cancelled marker (or a
terminal error fact) is included.
`ListThreadTurns` is the normal conversation-history read model. It groups the
same journal facts by started-turn ordinal and always returns ascending turns,
so hosts do not merge local transcript rows, timestamps, previews, or live
draft IDs into history. `ThreadTurnsPage.ThroughOrdinal` still reports the full
journal read boundary while a marker-only turn is hidden, allowing hosts to
observe revision progress without receiving an unadmitted turn. A live
projection is accepted only for its exact
`ThreadID`/`TurnID`/`RunID`; once the durable projection reaches the turn, a host
removes its in-memory draft or resynchronizes instead of guessing an order.
An initial reader uses bounded `Tail`, historical navigation uses only the
returned `BeforeCursor`, and live polling uses only the returned non-empty
`SinceCursor`. A retry turn carries `RetrySource{TurnID, EntryID}` and does not
duplicate the original user message. Hosts must not synthesize cursor ordinals,
copy retry input, or fall back to scanning the active path when an entry cursor
is stale.
A canonical user input containing only references is not retry-eligible because
its provider material was ephemeral and cannot be reconstructed from durable
authority. Its overview/page reports `CanRetry=false`, and `RetryTurn` returns
`ErrNoRetryTarget` before lease or journal mutation. A host that resolves fresh
resource context submits a new turn rather than relabeling it as a retry.
`ReadTurnProjection` requires explicit `RunID` input instead of inferring it
from stored rows, because `RunID` is execution identity and not the thread or
turn storage identity. A missing thread is reported with `ErrThreadNotFound`; a
known thread with no matching turn detail is reported with `ErrTurnNotFound`;
and a turn whose durable detail does not record the requested run is reported
with `ErrRunNotFound`.
If final detail reading or projection attachment fails, `RunTurn`, `RetryTurn`,
and `CompletePendingTool` preserve their engine result, return
`ProjectionAvailability=unavailable`, and do not convert the projection failure
into an execution error. `SettlePendingTool` likewise preserves its committed
detail event when the subsequent projection read is unavailable.
`ForkThread` is the public runtime contract for host-visible conversation forks.
Its request requires a host-supplied `ForkOperationID`. Before creating any
target, Floret durably fixes the source leaf, destination thread IDs, complete
turn/run mappings, and terminal child-thread clone plan. Each target carries an
exact operation/node marker. Commit publishes every planned target and the
terminal operation result in one transaction; replay never recreates a missing
node, rereads a newer source leaf, or regenerates identities. Reusing an
operation for a different request returns `ErrForkOperationConflict`; an
occupied destination before preparation returns `ErrForkDestinationConflict`;
and a missing, partial, or mismarked completed target set returns
`ErrAuthorityCorrupt`. An exact matching tombstoned target tree returns
`ErrThreadDeleted`.

The result returns only the operation ID and destination thread summary. It does
not expose source-to-destination `TurnID` or `RunID` mappings. A host that needs
destination turn details reads them through `ThreadReadHost.ListThreadTurns` and
must not persist its own fork identity map, clone Floret storage tables, or
materialize Floret display projections into a host shadow transcript.
`ThreadForkHost` exposes the fork transition without provider configuration;
source validation uses a separately injected `ThreadReadHost`.
`ProjectThreadTurn` derives assistant text, control-signal segments, and turn
activity only from the ordered `ThreadDetailEvent` stream for the target turn.
It does not accept or merge an older aggregate activity timeline as an input.
Row-level `ActivityTimeline` values are treated only as already-sanitized
presentation metadata for their corresponding detail event; the final activity
state is re-reduced from ordered tool call, tool result, approval, turn marker,
and run-failure facts. Terminal turn markers apply across all activity timeline
segments in the turn: cancelled turns settle unresolved pending/running items to
`canceled`, and failed turns settle them to `error`. Successful or completed
turns do not imply completion of host-owned pending work. Save-point markers
and other non-terminal turn markers such as `started`, `waiting`, or unknown
queued states remain lifecycle facts and do not settle activity. The
`tool_result_batch` save-point reason is a projection boundary only: it closes
the current activity segment so the next tool batch can become a separate
segment, while later facts for the same tool invocation still merge back into
one activity item. Downstream hosts may map those product-neutral segments to
their own UI blocks, but must not read Floret's store schema, rebuild execution
ordering from separate audit tables, or call `observation.BuildActivityTimeline`
to create the main thread activity surface. Host live protocols, cursors,
replacement snapshots, and product timeline reducers remain downstream
concerns; they are not Floret runtime contracts. Pagination uses `AfterOrdinal`,
`Limit`, `HasMore`, and
`NextOrdinal`; raw content follows the same explicit `IncludeRaw` opt-in rule as
subagent detail reads. Thread detail events share the same row-level
`ActivityTimeline` projection and structured tool result `status` contract as
subagent detail events. Subagent detail context facts are read from Floret's
public journal projection and sanitized observation DTOs; raw provider requests,
prompt-cache internals, transcript windows, and trimming strategy fields remain
internal. Host-owned pending tool outcomes after a provider turn must be
reported through `SettlePendingTool`; that settlement is stored as a custom
journal entry projected as a `tool_result` detail event, updates the original
activity item for the same tool id, removes running-only metadata, and does not
enter provider-visible history. Turn projection treats that settlement as the
authoritative activity lifecycle fact even when run-end markers or the
original pending tool result are ordered before or after it.

`runtime.Event.ActivityTimeline` remains Floret's live lifecycle observation
for tool, approval, control, budget, and run-end facts. It is generated from the
same sanitized observation events used for terminal `TurnResult` activity, but
it is not the display segment ordering contract for a hosted turn. Hosts should
use `runtime.Event.Projection`, terminal `TurnResult.Projection`, settlement
projections, or `ReadTurnProjection` for the main thread display and treat the
aggregate live timeline as observation/diagnostic state. Tool call and tool
result detail entries preserve the original lifecycle event time rather than
the later batch append time; `ProjectThreadTurn` uses those event times plus
result duration facts to keep final item intervals consistent. Invocation
presentation stored on a tool call message is merged with result presentation
for the same tool id, so final rows keep command labels and payload while
adding terminal result chips and payload fields.

Terminal turn results, including cancelled turns, still return a bounded
`ThreadTurnProjection`. Failed and cancelled terminal markers settle unresolved
tool and approval activity in Floret's projection before the result is returned,
so impossible-to-continue rows do not remain decisionable. Caller cancellation
during live projection or turn finalization is still recorded as a cancelled
terminal fact rather than as a generic failure. Successful turns keep
host-owned pending work running until the host reports a terminal outcome
through `SettlePendingTool`. That settlement remains authoritative for the tool
id and updates the same projected activity item rather than adding a duplicate
row. Active settlement is a method only on the exact `TurnExecutionHost` or
`SubAgentHost` owner and requires its locally held durable lease proof; it never
reacquires admission. A restart coordinator receives a provider-free
`PendingToolRecoveryHost` bound to exactly one root or parent. Interrupted-turn
recovery uses a separate factory bound to one exact root or parent-child plus
`TurnID`, owner, and generation. `NewHost` refreshes only a newer heartbeat for
that same target; disappearance or valid generation replacement ends the
target, while proof rollback or same-generation identity drift is authority
corruption. Durable takeover and finalization occur in one transaction. Recovery never scans
branches, rewinds the leaf, or treats missing authority as success, and every
control-signal message is excluded from ordinary unresolved tool-call
settlement. Downstream hosts should consume Floret projections and settlement results
to replace their product UI for the turn instead of synthesizing final tool
status from local audit records or live stream leftovers.

`ThreadTurnFailure{Code, Message}` is the canonical public failure contract.
Stable codes distinguish cancellation, interruption, provider failure, tool
dispatch failure, unknown effect outcome, authorization availability/contract,
storage, and engine contract failures. `legacy_unclassified` is reserved for a
predecessor terminal failure whose origin was not durably recorded; migration
never guesses it from error text. Interrupted recovery first terminalizes every
prepared or dispatching effect; any unknown effect outcome takes
precedence over a generic interruption. Hosts preserve `interrupted`,
`Recoverable`, and `CanRetry` as canonical facts and must not rewrite them to a
locally inferred running or recovering lifecycle.

The durable aggregate approval queue is the current-state companion to the
approval audit trail. `ReadApprovalQueue` covers the root and its canonical
descendants, orders ready items deterministically, and exposes exactly one
current item with queue generation and item revision. `ResolveApproval` records
the stable decision before invoking the host authorization gate and then
atomically settles approval, effect, proof hash, and queue promotion. Hosts own
product modes, approval copy, UI placement, authorization policy, and decision
routing; they submit exact decisions and map typed conflicts, but do not keep a
second approval queue.

For provider continuations such as length truncation recovery, the detail stream
records the durable live prefix and any final suffix in ordinal order. It does
not expose a duplicated accumulated assistant snapshot as a separate transcript
fact.

Deleting a thread is a data lifecycle operation. `DeleteThread` deletes the
target thread and Floret-managed descendant child threads, plus their prompt
cache records, Agent todo state, and thread artifacts. Runtime resolves the
complete tree while holding the same Store-level authority mutation gate used
by root creation, ordinary fork, and SubAgent spawn, so a child cannot appear
between descendant discovery and deletion on one Store. Runtime then issues one
root-scoped storage delete operation. SQLite independently reloads and validates
the complete authority graph inside its immediate transaction, derives the
current descendant set there, and applies threads, entries, active
leases, Floret authority records, artifacts, prompt segments, toolsets, provider requests,
provider responses, and provider continuation in one immediate transaction, so
a failure rolls back the whole tree without changing the public store schema.
Generic host metadata records are not Floret Agent state and are deliberately
retained for the owning host to clean up after canonical deletion commits.
Hosts should use this
public API instead of querying or mutating Floret storage tables directly.

Lifecycle operations are intentionally separate handles. Use
`TurnExecutionHost` for admitted turn execution,
`ThreadCompactionHost` for idle compaction, `SubAgentHost` for interactive
child lifecycle, parent-bound `SubAgentReadHost` for child reload and detail,
`ThreadReadHost` for canonical reloads, `ThreadCreateHost` for top-level
creation, `ThreadTitleHost` for title writes, `ThreadForkHost` for forks,
`ThreadDeleteHost` for thread-tree deletion, parent-bound
`SubAgentReadHost` for child reads, `PendingToolRecoveryHost` for idle recovery
settlement, and `InterruptedTurnRecoveryHostFactory` plus its proof-bound
`InterruptedTurnRecoveryHost` for exact expired-turn recovery.
Active settlement stays on `TurnExecutionHost` or `SubAgentHost`. Only binder
constructors inside `ConfigureHostCapabilities` accept `HostBootstrap`;
provider options and bound handle options do not accept or expose a Store or
bootstrap authority. Thread and parent identities are binder arguments, not
runtime-owner options.

Capability responses should be handled with `errors.Is` against the public
runtime sentinels. Not-found and ownership checks use
`runtime.ErrThreadNotFound`, `runtime.ErrThreadNotActive`,
`runtime.ErrTurnNotFound`, `runtime.ErrRunNotFound`, and
`runtime.ErrSubAgentNotFound`. Mutation and retry coordination use
`runtime.ErrThreadBusy` and `runtime.ErrNoRetryTarget`. Pending settlement uses
`runtime.ErrPendingToolNotFound`, `runtime.ErrPendingToolNotActive`, and
`runtime.ErrPendingToolSettlementConflict`; closed child writes use
`runtime.ErrSubAgentClosed`. Hosts should not parse error strings or import
Floret internal package sentinels. Interrupted-turn discovery and completion
use `runtime.ErrInterruptedTurnNotFound` and
`runtime.ErrRecoveryTargetResolved`; a stale proof-bound handle uses
`runtime.ErrStaleAuthority` and must be refreshed only through its same exact
factory.

Authority failures should use `errors.Is` with
`runtime.ErrSubAgentParentRequired` or
`runtime.ErrThreadAuthorityInvariant`; hosts must not repair, reinterpret, or
recreate the affected canonical thread.

Fork replay failures should likewise use `errors.Is` with
`runtime.ErrForkOperationConflict`, `runtime.ErrForkDestinationConflict`,
`runtime.ErrThreadDeleted`, and `runtime.ErrAuthorityCorrupt`.

Hosts validate `ProjectionAvailability`, `Projection`, and `ProjectionError` as
one outcome. `ready` requires a valid projection and no projection error;
`unavailable` requires no projection and a non-empty diagnostic. Explicit
`ReadTurnProjection` remains the durable reload operation.

`StreamObservation` is for host rendering and diagnostics. It is not raw
provider wire data and must not carry prompt text, tool arguments, tool results,
local paths, or secrets.

`runtime.Event`, `observation.Event`, `TurnResult`, and `StreamObservation`
expose normalized `FinishReason`, separate `RawFinishReason`, `FinishInferred`,
and finite completion or continuation reasons as typed fields. Metadata is not a
secondary encoding of those decisions. `runtime.Event.Validate` recursively
validates stream observations, activity timelines, turn projections, and event
to projection identity. Non-empty unknown values, simultaneous completion and
continuation, and inferred finishes without a normalized reason fail validation.

`runtime.Event.Committed` carries a `ThreadDetailEvent` after Floret has
successfully appended the corresponding journal entry. Its typed thread, turn,
run, and step identity must match the enclosing event. Hosts can render provider
text deltas as temporary live output, but durable display reconciliation should
use committed thread events or `ListThreadDetailEvents` so visible ordering
matches Floret's stored transcript. Internal harness events such as
`entry_appended` and `turn_started` remain on `HarnessSink`; they are not runtime
observation events and never enter the public `EventSink`.

Projected control signals are declared through `TurnSignalSpec`. Waiting
signals interrupt the turn with their projected prompt. Terminal signals complete
the turn with a human-visible output. A terminal signal may supply that output in
the signal payload, or it may rely on assistant text produced earlier in the
same provider step; if neither exists, the turn fails with a control-contract
error instead of inventing a completion. After a signal projector returns,
Floret encodes and decodes its product-neutral payload once as a standard JSON
value tree before it enters the transcript or durable journal. Named Go structs
and slices therefore use their JSON representation consistently across Memory
and SQLite, while channels, functions, cycles, and other non-JSON values fail
the turn as contract errors. Activity payloads remain on their separate public
presentation sanitizer and validator boundary. Control signals are projected as
control activity and control-signal display segments; they are not ordinary
local tool execution records.

Reasoning selection is request intent, not provider wire data. Floret normalizes
the public selection and provider adapters translate only values supported by the
selected model capability. Hosts that own model transport through `ModelGateway`
receive the effective selection and must render provider-specific payloads
outside Floret.

Dynamic tool surfaces are host-owned policy projection points. A host may set
`TurnExecutionHostOptions.ToolSurfaceProvider` or
`SubAgentHostOptions.ToolSurfaceProvider` for a bound provider capability, or
`RunTurnRequest.ToolSurfaceProvider` for one turn. Floret invokes it before
provider requests, before ordinary local tool dispatch, and before compact-only
provider request rebuilds. The returned `ToolSurface` can supply a current
`tools.Registry`, explicit local tool definitions, provider-hosted tools, system
prompt text, host context, and optional epoch/reason metadata.

Floret does not interpret product modes or authorization labels inside that
surface. If a provider returns a local tool call that was visible in an older
request but is no longer in the refreshed registry, the normal Floret tool
permission and dispatch lifecycle rejects or gates it at dispatch time. Provider
requests and observations include the current toolset, hosted toolset, prompt
hash, epoch, and reason metadata so hosts can audit surface changes without
reading internal provider-cache records.

Manual compaction is a control surface, not a transcript message. Hosts pass
active-run requests through `ManualCompactionSource`; Floret decides the safe
point, runs the same compaction pipeline as automatic pressure compaction with
`manual/manual` trigger and reason, includes request correlation in
start/complete/failed/cancelled observations, and then continues the provider
loop when the manual compaction failed without cancellation. Every accepted
manual request requires both `RequestID` and `Source`; automatic compactions use
a Floret-generated request ID and `Source=engine`.
Manual compaction is admission controlled by Floret. A valid manual request does
not force checkpoint creation: when the current Floret-owned context is below
the minimum useful compaction target, has no safe cut point, or would not shrink
after checkpoint overhead, Floret emits a terminal `noop` compaction observation
with request correlation and a coarse reason such as `context_too_small`,
`no_compactable_context`, or `insufficient_savings`. A no-op manual compaction
does not install a checkpoint, does not update continuation state, and does not
end the hosted run.
`ManualCompactionOperationID` exposes the public operation identity for a known
run id, provider-loop step, and manual request id so hosts can correlate
accepted manual work with later Floret observations without depending on
internal engine formatting. A failed manual compaction is observable but does
not by itself end the active run; a non-cancellation manual source poll failure
is emitted as a safe debug observation and the provider request continues.
Cancellation during manual polling or compaction is terminal for that hosted
turn and does not continue to a provider request.

Hosted compactions also emit `runtime.Event.CompactionDebug` diagnostics.
Those events identify safe pipeline stages and include operation/request
correlation, token pressure, message counts, durations, provider-state kind, and
the next action without exposing local paths, secrets, prompt text, tool
payloads, or generated summaries. Compaction attempts emit begin diagnostics
before preflight checks and a terminal `preflight` failure when configuration or
circuit-breaker checks stop the operation before summary generation. Manual poll
errors use the `poll` stage; non-cancellation poll errors continue to the
provider request, while cancellation poll errors use terminal `fail_turn`
semantics. Cancelled compactions use cancelled lifecycle and debug statuses,
preserving operation/request correlation without exposing raw request strings in
public runtime events.

For idle hosted threads, `ThreadCompactionHost.CompactThread` is the public
compaction-only entry point. The result reports one canonical terminal compaction event,
metrics, and activity timeline. Opaque provider continuation is loaded,
invalidated, cleared, and persisted only inside Floret Store; it is not returned
to the host. A successful context-changing compaction clears continuation,
while noop, failed, or cancelled operations preserve it only when
provider-visible context did not change. Provider-state persistence failure is
a finalization failure, never a successful result with a warning.

# Key Source Files

* [Runtime Contracts](/runtime/runtime.go)
* [Provider Capabilities](/runtime/provider_capabilities.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Core Control Helpers](/runtime/control.go)
