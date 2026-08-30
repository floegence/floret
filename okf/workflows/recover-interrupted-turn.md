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
