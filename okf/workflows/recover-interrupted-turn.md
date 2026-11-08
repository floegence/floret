---
type: Adoption Workflow
title: Recover an Interrupted Turn
description: Recover one exact Floret turn after restart without replaying a domain side effect.
resource: /runtime/thread_runtime.go
tags: [workflow, adoption, recovery, authority]
timestamp: 2026-08-18T00:00:00Z
---

# Goal

Resume or reconcile an interrupted turn after process loss without fabricating
identity, following the wrong user, or replaying an irreversible effect.

# Steps

1. Open the Host with the same durable storage source.
2. Hydrate the exact thread through `ThreadService.View`; Floret validates the
   canonical turn markers and current leaf.
3. Let the runtime recover only the admitted input, attachment references, and
   retry source recorded in the journal. It must not use the thread creator as
   the acting host user or rebuild a provider transcript.
4. Treat waiting approval or input as a pending interaction and surface it
   through the normal view; resolve it with `Respond` using a stable request key.
   After process loss, approval does not resurrect an executor. Floret atomically
   cancels a prepared effect and pairs its original tool call with a factual
   not-executed result before provider continuation. A new call needs current
   authorization; renamed tools never inherit an old call's approval. Concurrent
   hydration and response recovery share one in-process owner and cannot replace
   a live executor.
5. If a dispatched effect outcome is unknown, Floret fails the turn during
   live execution or startup. Show the typed failure and never replay the tool.
6. If the journal already contains a cancel request without its terminal,
   complete the same canonical cancellation transaction during hydration. Do
   not create a second cancel fact or redispatch the old effect.

# Verify

Test restart hydration, attachment identity, waiting interactions, unknown
effects, cancellation, and write failure. The [runtime API](../api/runtime.md)
defines the host-facing contracts.

# Boundary

Floret owns turn admission, execution identity, canonical terminal state,
effect attempt identity, and durable thread facts. The host owns endpoint
authorization, external resource resolution, and product audit; it must not
infer lifecycle state from those records or rewrite the Floret journal.

Graceful cancellation bounds in-process effect-finalization waiters by one
five-second deadline, even when the engine
has already stopped consuming tool results. Uncertain dispatched effects retain
the existing terminal classification; shutdown must not wait for an abandoned
finalization callback. See [effect dispatch](../../internal/agentharness/effect_dispatch.go).

A user stop selects the current Turn and binds its latest Run under the same
runtime lock that commits cancellation. A concurrent Ask User response may
advance the Run within that Turn, but cannot invalidate the user's stop.
Execution-context cancellation retains the exact Run fence, so a late context
callback cannot cancel a later continuation.

A resolved input waits for the prior execution to finish its durable pause before
claiming its continuation. ThreadRuntime appends the continuation save point and
installs its in-memory Run under one actor lock. AgentHarness consumes that
admission without appending another marker. Stop during finalization therefore
still targets the prior owner; Stop during preparation targets an already
canonical successor. The same cancellation transaction closes both cases.

Exact published v7.107.9 synthetic databases qualify approved and denied
approvals, unchanged and renamed tools, pending input, retained call/result
pairs, and zero replayed effects through ordinary and blank-module adoption
tests. Settlement failure rolls back; a repeated restart does not append again.
