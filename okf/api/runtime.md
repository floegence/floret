---
type: Public API
title: Runtime Package
description: Immutable Agents, durable Host ownership, bound thread operations, revision reads, and subscriptions.
resource: /runtime
tags: [api, runtime, agent, host]
timestamp: 2026-07-29T00:00:00Z
---

# Runtime

`runtime.Open` accepts `runtime.Options{Storage: storage.Source}` and returns a
composition-root-only `*runtime.Host`. Its public method set is intentionally
small: `Threads`, `Thread`, and `Shutdown`. `Threads.CreateThread` and
`Threads.ListThreads` are the only unbound collection operations. `Host.Thread`
returns a handle bound to one exact `identity.ThreadID`. The composition root
grants `ThreadReader`, `ThreadLifecycle`, `TurnExecutor`, `ThreadCompactor`, or
`SubAgentManager`; downstream services retain only the narrow interface they
need. `Thread` itself exposes only capability issuers and identity, so consumers
cannot accidentally mix read, lifecycle, execution, compaction, or SubAgent
authority through one broad object.
Each bounded `Threads.ListThreads` page projects its exact thread snapshots and
revisions from one canonical domain snapshot. Page size therefore does not
multiply backend domain decoding or hold a sequence of read fences that can
starve approval and lifecycle mutations.
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
Provider execution is serialized per thread rather than by the host-wide
mutation fence. It may wait for a canonical approval while
`TurnExecutor.ResolveApproval` commits that exact decision; concurrent replay of
the same admission converges on the committed receipt without a second provider
or tool invocation. Physical backend transactions remain short and serialized
across session-tree state, prompt state, and the logical-request ledger.

Every thread has a monotonic `ThreadRevision`.
`ThreadReader.Bootstrap` returns the thread, initial turn page, approval queue,
todo state, context, pending work, and direct SubAgents while the exact thread
remains at one revision. Consumers then call `Subscribe(after=revision)`.
History pages retain stable cursor semantics. An unavailable revision fails
with `ErrRevisionUnavailable`; it never silently reads current state.
Exact-thread subscriptions use `Next(ctx)`. A queue overflow returns one Gap
and then `ErrSubscriptionStale` until the consumer bootstraps again.

`ThreadReader.ReadAuthoritativeProjection` returns canonical projection plus
revision and authoritative provenance. `DeriveThreadTurn` is a validated offline
calculation from caller-supplied events and always reports derived provenance;
it must not be persisted as Floret lifecycle authority.

`runtime.NewAgent` requires a valid `config.AgentConfig` and non-nil
`provider.Gateway`. It snapshots profile, prompt policy, static tools, effect
gate, event sink, dynamic tool surface, loop limits, title mode, capabilities,
SubAgent timeout, and optional manual compaction source. It has no mutating API.

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
