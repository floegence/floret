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
returns a handle bound to one exact `identity.ThreadID`; `Thread.Turns`,
`Thread.SubAgents`, `Thread.Child`, and `Thread.DescendantReader` preserve that
authority without repeating it in command DTOs.

`Child.ReadDetail` and `Child.ListPendingToolTargets` apply only to the bound
direct child. `DescendantReader.ListTurns`, `ReadTurn`, and `ReadArtifact` apply
to the one validated descendant bound beneath the parent, including deeper
descendants. Callers cannot substitute a target identity after either handle is
issued.

Mutation commands carry a host-supplied `identity.LogicalRequestID`. Floret
allocates every `ThreadID`, `TurnID`, `RunID`, and child identity. The durable
request key combines operation kind, bound authority, and logical request ID;
replay returns the original receipt, while changed durable input returns a
typed request conflict. There is no bootstrap, binder, factory, Store facade,
rename-only adapter, or caller-assigned lifecycle identity.

Every thread has a monotonic `ThreadRevision`. Consumers read `Snapshot`, load
all first-screen queries at that revision, then call `Subscribe(after=revision)`.
Pages have stable ordering, limits from 1 through 200, and cursors bound to the
thread, revision, direction, and position. An unavailable revision fails with
`ErrRevisionUnavailable`; it never silently reads current state. Exact-thread
subscriptions use `Next(ctx)`. A queue overflow returns one Gap and then
`ErrSubscriptionStale` until the consumer resynchronizes.

`runtime.NewAgent` requires a valid `config.AgentConfig` and non-nil
`provider.Gateway`. It snapshots profile, prompt policy, static tools, effect
gate, event sink, dynamic tool surface, loop limits, title mode, capabilities,
and SubAgent timeout. It has no mutating API.

Runtime owns admitted conversation, turn/run lifecycle, projections, approval,
Todo, SubAgent, artifact, pending settlement, provider state, and prompt cache
facts. Hosts read these through handles and do not persist a second Agent
lifecycle.

`Host.Shutdown(ctx)` stops admission, cancels Host-managed execution, and waits
for completion. A deadline leaves the Host closing so a later call can continue
waiting. Runtime never migrates or dual-reads legacy state.
