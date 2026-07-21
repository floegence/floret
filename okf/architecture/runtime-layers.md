---
type: Architecture Concept
title: Runtime Layers
description: Runtime, AgentHarness, and Engine form separate layers for public hosting, durable conversation lifecycle, and single-run execution.
resource: /runtime/runtime.go
tags: [architecture, runtime, engine]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Floret separates public hosting, durable conversation lifecycle, and low-level
provider execution.

# Layer Responsibilities

Provider-backed authority is split by lifecycle owner. `TurnExecutionHost`
runs and resumes admitted turns for one bound thread;
`ThreadCompactionHost` compacts one bound thread; and `SubAgentHost` manages
interactive child lifecycle under one bound parent. Requests keep explicit
thread identities and fail before provider or journal work when they do not
match the handle authority. Canonical reads remain on `ThreadReadHost`.

Provider-free lifecycle transitions use narrow concrete capabilities.
`ConfigureHostCapabilities` runs once per Store, issues responsibility-specific
binders, and seals its `HostBootstrap` before returning. Provider binders later
fix one root or parent before returning a provider-configurable factory;
provider-free binders return exact create, title, fork, delete, read, SubAgent
read, pending settlement recovery, or exact interrupted-turn recovery handles.
Binders remain inside the composition
root. No coordinator or run receives a raw `runtime.Store`, the bootstrap token,
or any binder; it receives only the exact authority-bound factory or handle for
its responsibility. `ThreadReadHost` is read-only and is the reload source for
top-level thread, turn, context, todo, and approval-queue projections;
parent-bound `SubAgentReadHost` owns child list/detail/activity reads. Approval
resolve remains on the bound `TurnExecutionHost`, while the Floret Store owns
one durable aggregate root/descendant queue and exact decision CAS. A
`ThreadReadHost` approval read does not require an active turn owner, so
reconnect/bootstrap code can reload the canonical queue through the public
capability without a host-side copy.
Terminal execution facts and terminal display projection availability are
separate results. If durable detail cannot be read after a turn terminates,
the runtime preserves the engine result and reports projection status as
`unavailable` without changing the execution error. Pending-tool settlement
also preserves a committed settlement event when its projection read fails.
Provider options contain no Store, bootstrap, thread, parent, or runtime root.
`runtime.Store` is opened and closed by the composition owner; neither it nor a
usable `HostBootstrap` is stored in a coordinator or run object. Binders and
capability handles never close the Store, so shutdown closes it exactly once
after active work ends. `Store.Close` first fences new operations, cancels
Store-owned active execution and automatic-title workers, waits for terminal and
title finalization plus lease release, and only then closes the backend.
Retained binders, factories, and handles return `ErrStoreClosed` after closing
starts.
Runtime resolves a target thread plus its descendants before submitting one
tree delete request to storage. Root creation, ordinary fork, SubAgent spawn,
and root tree deletion share one Store-level authority mutation gate; deletion
holds it across root validation, descendant discovery, and physical deletion so
concurrent spawn on that Store cannot leave an orphan child. The SQLite
implementation accepts only the root identity, reloads and validates the full
authority graph inside its immediate transaction, derives the current child
tree, and then deletes thread rows, journal entries, active leases, Agent todo
state, Floret authority records, artifacts, prompt scopes, and provider ledgers.
Host-owned generic metadata is outside canonical deletion and remains for its
own coordinator to clean up. This closes the
gap across multiple Store instances or processes. Every SQLite open validates
the complete current authority graph before exposing the Store.

Pending tool completion and pending tool settlement are intentionally separate.
`CompletePendingTool` creates a provider-visible follow-up turn when the model
should reason over a host-owned completion. `SettlePendingTool` appends only a
detail/activity event for the original turn when the host needs to update UI
state without resuming the provider loop. Settlement is keyed by the public
`PendingToolSettlementTarget`, which carries complete thread, turn, run, tool
call, tool name, and handle identity. It is idempotent for the same terminal
status and may be recorded before the pending result row when host-owned work
finishes faster than the provider turn can commit that row.
Active settlement is available only through the exact bound
`TurnExecutionHost` or `SubAgentHost` and requires that owner's local durable
lease proof. It fails with `ErrThreadNotActive` instead of reacquiring turn
admission after the owner becomes idle. A restart coordinator may construct a
provider-free `PendingToolRecoveryHost` bound directly to exactly one root or
parent. It is not a general maintenance or read facade and must be owned by the
one coordinator responsible for that pending work.
For a host that still owns the original local execution, an exact valid terminal
`TurnResult` is the authority-release barrier: Floret has committed the terminal
outcome and released the active lease. A return without that result requires an
exact terminal confirmation through the public canonical read API. The host may
then use its already scoped `PendingToolRecoveryHost` to settle remaining
host-owned work. It must not start recovery settlement before either proof or
mask the resulting `ErrThreadBusy` with polling.

The durable Floret journal, public turn pages and projections, canonical titles,
typed failures, ordered user references, aggregate approval queue, and typed
Agent todo state are the canonical Agent source. Hosts may keep product audit and
diagnostics, but must not persist a queryable copy of conversation content,
admitted references, title, turn/run lifecycle, todo state, approval lifecycle,
tool status, arguments, results, or errors.

`TurnExecutionHostOptions.ModelGateway` and
`SubAgentHostOptions.ModelGateway` let a host route parent and child turns
through product-owned model transport while Floret still owns request
construction, provider loop control, ledgers, tool dispatch, and runtime events.
Their `EffectAuthorizationGate` receives a one-shot `AuthorizedEffect` callback
that requires an execution context. The selected context reaches the handler,
and Floret composes it with active turn cancellation before crossing durable
effect dispatch. A host may therefore narrow one effect lifetime without
creating a second lifecycle owner or allowing the effect to outlive the turn.
Title generation is host-owned by default, but title persistence is always
Floret-owned. `SetThreadTitle` is the explicit host write contract.
`ThreadTitleModeProvider` routes a dedicated Floret title request through the
same transport immediately after first canonical user admission, concurrently
with the main turn. Its durable generation/token and `pending`/`ready`/`failed`
status are Floret authority; a manual title wins a late provider result. A nil
internal title generator means disabled rather than an implicit provider
fallback.
Their `ModelGatewayIdentity` fields supply the provider/model identity for that
host-owned transport plus a required non-sensitive continuation compatibility
key. Gateway-backed capabilities keep provider transport settings out of their
`Config`, so fake provider configuration cannot leak into a production gateway
integration.
Before invoking that gateway, Floret projects the journal into typed model
messages. One assistant response carries text/reasoning plus its ordered
parallel tool-call group; tool results must follow in the same order with exact
call ID and tool name. Empty or invalid JSON arguments, duplicate IDs, orphaned
results, unresolved calls, and illegal adjacency fail before transport. The
gateway adapter only performs a direct wire-shape mapping.
User messages may also carry opaque attachment resource references. Floret
stores the association in the journal and prompt-cache snapshot while the
`ModelGateway` host resolves the resource into provider-native content. A native
provider host without that resolver rejects attachments before admission.

`AgentHarness` is the internal durable conversation layer. Its production
options receive only `sessiontree.JournalRepo`, so normal runs cannot create,
delete, fork, claim, acquire leases, or mutate provider state through generic
repository methods. Lifecycle coordinators call the dedicated semantic storage
kernel for those transitions. AgentHarness owns turn execution, retries,
SubAgent runtime behavior, titles, and projection of an active journal path into
one engine execution.
It also consumes opaque provider continuation from Floret Store. A turn loads
continuation only when the current journal leaf and compatibility key match
exactly. Incompatible state is ignored for that request and is replaced or
cleared only as part of the next atomic `FinishTurn`; there is no early cleanup
side effect. Fresh response state, terminal outcome, and exact lease release are
committed together. If projection or finalization fails, the turn remains
unfinished instead of returning a fabricated terminal result.
The public observation boundary carries normalized finish, raw finish,
inference, completion, and continuation as distinct typed facts. Hosts consume
those fields directly instead of interpreting metadata keys.
Public thread forks are durable operations rather than best-effort recursive
copies. AgentHarness prepares one immutable plan containing the pinned source
leaf, destination IDs, turn/run rewrites, terminal child list, and child
metadata patches before any target exists. Storage persists that plan and final
result independently from generic metadata. Replays execute exact marked nodes;
they do not infer success from destination existence or inspect a newer source
path.
Realtime turn projection and final engine-result backfill share one durable
journal. When a provider continuation appends more assistant text to the same
turn, the harness commits the live prefix at continuation boundaries and only
backfills the uncommitted suffix, preserving the ordered execution transcript
without duplicating accumulated provider output.
Durable compaction entries are committed only after `Engine` has rebuilt and
validated the compacted provider request, so a journal checkpoint is an
installed continuation boundary rather than a candidate summary.
Restart recovery is an explicit proof-bound capability. `ResumeThread` only
binds an existing canonical journal and never settles or rewrites it.
`InterruptedTurnRecoveryHost` requires the complete takeover-eligible turn lease
proof plus exact root or parent-child authority. The storage kernel replaces
that proof with a recovery mutation generation, rereads the active path in the
same critical section or transaction, marks dispatching effects unknown,
cancels prepared effects, appends the exact typed terminal outcome, and releases
authority atomically. An unknown effect outcome takes failure-code precedence
over generic interruption. Recovery never selects an unfinished turn without a
lease, scans another branch, moves the leaf, or infers recovery from host state.

`Engine` is the prompt-first single-run executor. It owns provider loop control,
tool invocation, compaction decisions, prompt-cache requests, metrics, and event
emission.
It checks cumulative `MaxInputTokens` after each provider usage merge before the
independent cumulative `MaxTotalTokens` limit. Per-request output limits remain
part of provider request/context policy and are not treated as run-level input
budget.
Ordinary calls in one model tool-call batch execute concurrently and may emit
each tool result as soon as that individual tool finishes, so hosts can observe
a pending tool result without waiting for a slower sibling. Durable harness
save points remain provider-safe boundaries: the harness commits the tool calls first, may
commit partial results for live observation, and writes the tool-result batch
save point only after all calls in that batch have matching results.
Provider-visible transcript messages for the batch are appended in the original
tool-call order after all sibling results are available, even though observation
events may arrive in completion order.
For terminal control signals, `Engine` normalizes the visible completion output
from the signal itself or from assistant text produced in the same provider
step. A terminal control signal with neither source is a contract error, so the
engine never fabricates a successful completion. `Engine` also normalizes every
projected control payload into a standard JSON value tree before transcript and
journal persistence. This single boundary keeps Memory and SQLite representations
identical for nested named structs and slices and rejects non-JSON values instead
of storing an entry that cannot pass later integrity validation. Tool activity
payloads continue to use the independent observation sanitizer and renderer
payload validator.

# Hosted Context Lifecycle

`TurnExecutionHost` is the public capability for admitted turns, while
`ThreadCompactionHost` owns idle compaction. The host supplies product input,
tools, permissions, and optional model transport; Floret owns provider-visible
context assembly, trimming, summary generation, checkpoint installation,
provider continuation state, prompt-cache ledgers, and lifecycle events.

`RunTurnRequest.Input` is the durable user message contract. Its text, opaque
attachments, and ordered product-neutral `MessageReference` values enter the
canonical journal together and remain attached through public events, turn
pages, detail reads, reopen, and forks. References form a closed display union
for text, files, directories, terminals, and processes; opaque file/directory
resource identity remains host-owned and is never authorization evidence.
References are not automatically inserted into provider history or compaction
summaries. Resource bytes and resource lifecycle remain host-owned.

`RunTurnRequest.SupplementalContext` is the host-facing current-turn context
slot. AgentHarness forwards it to Engine as provider-request projection input,
and Engine normalizes the bounded payload and renders it into each provider
request for that turn without appending it to the durable journal. It never
enters prompt-cache segments, raw plans, provider request/response ledgers,
compaction summaries/checkpoints, later history, or opaque continuation state.
The field is product-neutral: hosts decide which source facts are safe,
redacted, and intentionally truncated before calling Floret, while Floret keeps
`Input`, thread history ownership, runtime working directory, permissions, and
approvals unchanged. A reference-only root turn requires renderable supplemental
context for the current provider request; exact replay ignores supplemental
changes because ephemeral context is not admission identity.
A reference-only canonical turn is therefore not retry-eligible: Floret cannot
reproduce its provider intent after discarding supplemental context. Resolving
fresh host material must create a new turn. A reference turn that also has
durable text or attachments remains eligible under the normal retry contract.

Active manual compaction flows through `RunTurnRequest.ManualCompactions`. The
host owns the user-facing command or policy that creates a request; `Engine`
owns polling at safe provider-loop points, summary generation, checkpoint
installation, and continuation of the same run. `Engine` also owns manual
compaction admission. If the current context is too small, has no safe cut
point, or would not shrink after checkpoint overhead, the manual request ends
with a `noop` observation rather than a checkpoint or a run failure. Hosts must
not provide target tokens, history ranges, or summary policy to override that
decision.

Idle compaction uses `ThreadCompactionHost.CompactThread` instead of pretending
to be a user turn. It requires a request ID and source, runs the compaction pipeline once,
and returns one canonical terminal compaction event, metrics, and activity
timeline. Floret clears continuation after a successful context change and
preserves it across noop/failed/cancelled operations only when provider-visible
context is unchanged. The host never receives or persists the opaque envelope
and must not rebuild provider-visible history from product messages, debug
reports, or checkpoint internals.

# Child Threads

Hosted subagents are durable child threads managed by `AgentHarness` and exposed
through a parent-bound `SubAgentHost`. A parent can spawn, send input to, wait
for, and close child threads; canonical list and detail reads use a separately
parent-bound `SubAgentReadHost`. The child runs as a normal Floret thread with its own
`ThreadID`, `TurnID`s, prompt scope, provider request ledger, and journal.
Floret persists neutral delegated-work metadata such as task name and task
description with the child lifecycle; downstream products decide how to render
that description and how to attach UI routing actions.
Queued input is represented in that journal as Floret lifecycle state, not as an
in-memory host queue, so restart recovery, wait semantics, and close semantics
derive from the same durable source.

Waiting for child threads is intentionally separate from reading child detail.
`WaitSubAgents` returns bounded snapshots and timeout state; it does not return
the child transcript, tool outputs, or detail timeline.
`ListSubAgentActivityTimeline` gives host UIs a parent-scoped activity summary
derived from child snapshots without exposing the child transcript.
`ReadSubAgentDetail` is the single public host API for parent-scoped, paginated
inspection of the child journal. It lets a product UI show complete child
execution detail while keeping parent model context small. Its top-level
activity timeline is rebuilt from retained child detail events on each read so
later tool results and pending-tool settlements replace earlier running updates;
row events remain the ordered child journal.
Its top-level context block carries neutral model-bound context facts:
provider/model identity, model-derived context policy, current pressure/usage
status, and public compaction operations. Fork mode and parent/child thread
identity affect lookup and journal ownership only; they do not define context
window size.

Hosts that need lifecycle metadata plus the latest admitted turn use
`ReadThreadOverview`, which projects both from one active path. Hosts create a
missing canonical journal only through `CreateThread`; transcript-free
`ReadThread` and `ThreadSummary` projections never create or recover one, and
the public runtime exposes no alternate start-or-create entry point.
Conversation bootstrap and pagination use `ListThreadTurns`: initial reads use
bounded `Tail`, historical reads use the returned entry-identity
`BeforeCursor`, and incremental reads use the returned non-empty
`SinceCursor`. Every mode returns admitted canonical turns in ascending order;
a stale cursor fails instead of falling back to a host-derived ordinal or full
path scan. A marker-only turn remains outside the public turn list even if it is
later cancelled or terminal; only a canonical user entry admits it. Retry turns
carry exact source turn/entry identity without duplicating the user message. The
page through ordinal continues to cover the full read boundary. Hosts that need
to reload a single known turn may use
`ReadTurnProjection` with explicit `ThreadID`, `TurnID`, and `RunID`. Floret
rebuilds assistant text, verified control-signal payloads, and activity timelines
from durable detail; hosts do not assemble a shadow transcript.
Live, terminal-result, pending-tool settlement, and read-back projections share
one version rule: `ThroughOrdinal` is the greatest durable journal-detail
ordinal included by the common projector. Projection timestamps are diagnostic
only.

Closing a subagent stops current child execution and queued work. It preserves
the child thread and journal so the host can still read detail after close,
timeout, restart, or terminal completion.

This layer does not define product roles such as reviewer or worker beyond
opaque labels. Hosts own prompt policy, permissions, UI, and product workflow.

# Key Source Files

* [Runtime Contracts](/runtime/runtime.go)
* [Provider Capabilities](/runtime/provider_capabilities.go)
* [Projected Turns](/runtime/projected_turn.go)
* [Agent Harness](/internal/agentharness/harness.go)
* [Replayable Forks](/internal/agentharness/fork_operation.go)
* [Engine](/internal/engine/engine.go)
