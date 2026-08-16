---
type: Public API
title: Runtime Package
description: Immutable Agents, durable Host ownership, bound thread operations, revision reads, and subscriptions.
resource: /runtime
tags: [api, runtime, agent, host]
timestamp: 2026-07-29T00:00:00Z
---

# Runtime

## Phase 1 migration boundary

The runtime's target model is one Thread-owned ordered event stream with
revision fencing and one pending interaction union for approval or input. New
host integrations must converge through that model; receipt, handoff, barrier,
and projection values are not additional lifecycle authority. Existing v3
capability methods remain only where a caller has not yet migrated and must be
converted at the boundary before entering the thread actor. The internal
`agentharness` event log deduplicates event IDs, rejects revision gaps, and
supports read-after-revision so a host can resync instead of guessing state.

`runtime.Open` accepts `runtime.Options{Storage: storage.Source}` and returns a
composition-root-only `*runtime.Host`. Its public method set is intentionally
small: `Threads`, `Thread`, and `Shutdown`. `Threads.CreateThread`,
`Threads.ListThreads`, and `Threads.ListInterruptedTurnRecoveryCandidates` are
the unbound collection operations. `Host.Thread`
returns a handle bound to one exact `identity.ThreadID`. The composition root
grants `ThreadReader`, `ThreadLifecycle`, `TurnExecutor`, `ThreadCompactor`, or
`SubAgentManager`; downstream services retain only the narrow interface they
need. `Thread` itself exposes only capability issuers and identity, so consumers
cannot accidentally mix read, lifecycle, execution, compaction, or SubAgent
authority through one broad object.
Each bounded `Threads.ListThreads` page projects its exact thread snapshots,
optional latest turns, and revisions from one canonical inventory snapshot.
The latest turn is absent for an empty thread and otherwise uses the same
validated projection as `ThreadReader.ReadOverview`. Page size therefore does
not multiply backend domain decoding or hold a sequence of read fences that can
starve approval and lifecycle mutations. This projection reuses the existing
v4 inventory path; it does not change the session-tree schema lineage.
`Threads.ListInterruptedTurnRecoveryCandidates` performs one canonical,
read-only scan of active turn leases and returns only stable root/parent-child
identities. It does not grant recovery authority; hosts bind each returned
identity through the exact interrupted-turn capability before recovery.
Standalone compaction remains exact-thread authority. Active-turn manual
compaction is an immutable Agent capability polled only at engine safe points.
`SubAgentManager.WaitSubAgents` and `CloseSubAgent` validate direct children
beneath the bound parent; close is a durable, replayable logical mutation.

`Child.ReadDetail` and `Child.ListPendingToolTargets` apply only to the bound
direct child. `DescendantReader.ListTurns`, `ReadTurn`, and `ReadArtifact` apply
to the one validated descendant bound beneath the parent, including deeper
descendants. Callers cannot substitute a target identity after either handle is
issued.

The bound `ThreadReader` is the canonical read source for an atomic bootstrap,
overview, exact turn, turn pages, typed todos, context, approval queue,
authoritative turn projection, pending-tool targets, and direct-child inventory.
`Child.ReadTurn` and `Child.ListTurns` provide the corresponding direct-child
reads. `ThreadLifecycle.SetTitle` is a durable logical mutation.
`ThreadLifecycle.PendingToolRecovery` binds one exact settlement target, and
`ThreadLifecycle.InterruptedTurnRecovery` binds one exact current interrupted
lease proof before either operation can mutate lifecycle state.

The first canonical user message installs a non-empty fallback title in the
same session-tree transaction as turn acceptance. Text is whitespace-normalized
and bounded by the canonical title limit; attachment- or reference-only input
uses its first display name. Provider-owned automatic title generation keeps
that fallback visible while pending and after failure, and replaces it only on
successful completion. A host `SetTitle` remains final authority over an
in-flight automatic request. Startup repairs historical empty titles from the
first canonical user entry before summaries are projected. Automatic title
provider work is tool-free background execution bounded by runtime lifetime and
timeout; it does not share the main turn's effect-permission admission.

Mutation commands carry a host-supplied `identity.LogicalRequestID`. Floret
allocates every `ThreadID`, `TurnID`, `RunID`, and child identity. The durable
request key combines operation kind, bound authority, and logical request ID;
replay returns the original receipt, while changed durable input returns a
typed request conflict. Production `runtime.Open` allocates lifecycle identity;
deterministic identity injection belongs to `florettest.NewIDSource`. There is no
host-owned Store facade or caller-assigned production lifecycle identity.

`TurnExecutor.AdmitTurn` splits canonical user-message admission from provider
execution. It persists the admitted user message and immutable execution plan,
allocates the `TurnID` and `RunID`, returns a `TurnAdmissionReceipt`, and does
not issue a provider request. Hosts may persist product coordination beside
that receipt, then call `TurnExecutor.ExecuteAdmission` with the receipt and an
`ExecutionContext` containing only ephemeral supplemental context and executable
signal bindings. The host never persists or resubmits the canonical command.
`AdmitTurnResult.Execute` is only a same-process convenience over the same
receipt-first path. There is no command-bearing execution fallback.

Each canonical `ThreadTurnSnapshot` optionally carries the same
`logical_request_id` as its admission receipt. The identity is a
product-neutral association for reconciling one user-visible admission across
bootstrap, exact reads, pages, replay, and restart; it is not a run, attempt,
trace, storage locator, or authorization capability. Retry turns inherit the
source turn's logical lifecycle identity and do not create a second canonical
user admission. Legacy started markers without this metadata remain readable
and project an empty optional field.

Admission is memory-first inside the single Floret process: the actor assigns
stable thread, turn, and run identities and publishes the accepted/running
receipt before prompt projection or provider dispatch. High-frequency prompt
segments, toolsets, provider observations, and live drafts remain in process
memory until a semantic checkpoint such as terminal turn commit, effect intent,
explicit checkpoint, or shutdown. `runtimeStore.Close` checkpoints any backend
authority before physical close, so hand-built or test storage paths have the
same recovery boundary. A restart recovers only the last checkpoint; transient
token, draft, subscriber, and cursor state is intentionally not claimed as
durable authority.

Each thread has one in-process mailbox owner for ordered lifecycle mutation and
live projection state. It owns the active turn/run/logical-request identity,
current provider attempt, and transient assistant/reasoning drafts. Provider
network I/O and approval waits run outside that mailbox, so a waiting effect
cannot block its exact approval resolution or unrelated threads. Shutdown first
stops new mailbox submissions and drains accepted mutations before checkpointing
and closing storage.

Provider requests carry one stable `logical_request_id` plus an `attempt_id` and
monotonic `attempt_epoch` for every dispatch in that run, including ordinary
multi-step turns. The turn projection activates the newest attempt, clears its
transient draft, and drops late older deltas before either live publication or
canonical assistant commit. An epoch reused by a different attempt ID, or a
different logical request, fails closed. Canonical assistant, tool, and terminal
commits remain keyed by durable entry/effect identity and replay to the existing
result; assistant text comparison is never used for deduplication.
Provider execution is serialized per thread rather than by the host-wide
mutation fence. It may wait for a canonical approval while
`TurnExecutor.ResolveApproval` commits that exact decision; concurrent replay of
the same admission converges on the committed receipt without a second provider
or tool invocation. A rejected approval returns after the canonical decision,
queue revision, and rejection entry commit; provider continuation and
observation-sink publication run outside that receipt path. The runtime store
tracks and drains that post-commit publication during shutdown, while restart
rebuilds any unpublished display state from the same durable entry. Terminal
turn commit and stale-renewal classification share
a narrow settlement fence from durable commit through local lease cleanup, so
completion cannot race into a committed result paired with a stale-authority
error. Renewal I/O and detached thread-title work remain independent. Physical
backend transactions remain short and serialized across session-tree state,
prompt state, and the logical-request ledger.

Every failed engine result carries an explicit failure origin. Provider,
tool-dispatch, control-signal, storage, and cancellation errors keep their
specific origin; otherwise an internal validation failure is classified as an
engine contract failure. Malformed control calls retain the provider-authored
assistant text and exact control tool identity while their canonical turn
failure uses `control_error`. AgentHarness persists that original classified
error and never replaces it with a secondary missing-classification failure.
The canonical control signal keeps its expected disposition for protocol
diagnostics, but the activity is terminal `error` and the thread does not expose
a pending user prompt. Public turn pages and restart projections preserve the
same `error_code`, call identity, and assistant text.

Every thread has a monotonic `ThreadRevision`.
`ThreadReader.Bootstrap` returns the thread, initial turn page, approval queue,
todo state, context, pending work, and direct SubAgents while the exact thread
remains at one revision. The standard backend projects all of those read models
from one canonical domain snapshot and one backend read transaction; it does
not repeatedly decode the complete session tree. That snapshot projection uses
the same process-local execution registry as ordinary reads, so an admitted or
executing turn remains running while its exact lease proof is active. A restarted
host has no such in-memory proof and projects the unfinished turn as a recoverable
interruption. Each transaction compares the exact durable domain envelope with
the last validated in-process snapshot;
only byte-identical state reuses its decoded projection, while any change is
strictly decoded before use. Tombstoned identities retain the public `ErrThreadDeleted`
classification, while identities absent from both live state and tombstones
return `ErrThreadNotFound`. Consumers then call `Subscribe(after=revision)`.
History pages retain stable cursor semantics. An unavailable revision fails
with `ErrRevisionUnavailable`; it never silently reads current state.
Exact-thread subscriptions use `Next(ctx)`. A queue overflow returns one Gap
and then `ErrSubscriptionStale` until the consumer bootstraps again.

`ThreadReader.ReadAuthoritativeProjection` returns canonical projection plus
revision and authoritative provenance. `DeriveThreadTurn` is a validated offline
calculation from caller-supplied events and always reports derived provenance;
it must not be persisted as Floret lifecycle authority.

Committed runtime events expose a compatible full `ThreadTurnProjection` and a
validated `ThreadTurnProjectionDelta`. `DiffThreadTurnProjections` produces the
minimal index replacements from one exact running ordinal to the next. A
terminal projection produces a self-contained base-zero checkpoint so a host
can converge after missing intermediate observation events.
`ApplyThreadTurnProjectionDelta` rejects incomplete identity, stale bases,
non-advancing ordinals, duplicate or unordered indexes, truncation drift, and
invalid reconstructed projections. A base-zero checkpoint replaces any local
lineage; other gaps return the consumer to the authoritative read. Delta streams
remain transient observation, not durable authority.

`runtime.NewAgent` requires a valid `config.AgentConfig` and non-nil
`provider.Gateway`. It snapshots profile, prompt policy, static tools, effect
gate, event sink, dynamic tool surface, loop limits, title mode, capabilities,
SubAgent timeout, and optional manual compaction source. It has no mutating API.

The typed service returned by `Host.ThreadService` also implements the narrow
`ThreadContextReader` capability. Its `Context` snapshot is derived directly
from the canonical journal and merges compaction lifecycle checkpoints by
operation identity, so restart restores one latest terminal record rather than
leaving a prior `start` checkpoint as an independent operation.

`ThreadService.View`, `Subscribe`, and `History` expose one ordered
presentation through `ThreadView.Items`. User, thinking, assistant, tool, and
independent input-interaction segments receive stable IDs and thread-global
ordinals on first appearance. Live text grows only its open segment; approval,
dispatch, result, and failure update the original tool segment without changing
its ID, ordinal, or position. Canonical journal facts deterministically rebuild
the same sequence for reload and process restart, so no presentation ledger or
host ordering store exists. The deprecated `AssistantDraft` and
`ThinkingDraft` fields are derived compatibility views of the active item and
must not be used as a second sequence.

`ThreadService.Send` completes low-frequency canonical turn acceptance before
it publishes or returns the user segment. A storage or validation failure
therefore returns an error with the actor view unchanged; a segment in an
accepted receipt cannot disappear because background admission later failed.
Provider preparation and execution remain asynchronous after that receipt and
do not block the command boundary.

Input waits are represented by one interaction segment; resolving the input
updates that segment in place, while the resumed provider output receives a new
assistant segment. A continuation atomically claims the exact waiting run so
concurrent recovery paths cannot dispatch it twice. At every non-waiting
terminal outcome, canonical activity facts settle matching tool segments in
place and remove only still-live provisional calls that never became canonical,
including provider schema corrections. Terminal views therefore cannot retain
duplicate interactions or noncanonical running tools.

Runtime owns admitted conversation, turn/run lifecycle, projections, approval,
Todo, SubAgent, artifact, pending settlement, provider state, and prompt cache
facts. Hosts read these through handles and do not persist a second Agent
lifecycle.

Opening a Floret v3 backend validates the exact persisted session-tree domain
schema before the Host becomes available. Exact supported older domain state is
migrated through every contiguous version edge and committed atomically with the
new version and final verification. Current state is not rewritten. Drifted,
ambiguous, corrupt, unknown, or future state fails closed without mutation.
SubAgent admission migration preserves an active lease exactly; terminal
history receives only a source-bound read proof and never a fabricated
executable lease.

`Host.Shutdown(ctx)` stops admission, cancels Host-managed execution, and waits
for completion. A deadline leaves the Host closing so a later call can continue
waiting. Runtime does not automatically convert or dual-read the external
v2.2/SQLite-v16 physical schema; that separate migration remains explicit.
