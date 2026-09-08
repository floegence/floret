---
type: Architecture
title: Runtime Layers
description: Composition, durable lifecycle, and single-run execution responsibilities.
resource: /runtime
tags: [architecture, runtime, layers]
timestamp: 2026-07-29T00:00:00Z
---

# Runtime Layers

```text
host composition root
  -> runtime.Host owns storage.Backend
  -> runtime.Agent snapshots provider and execution policy
  -> identity-bound handles
       -> AgentHarness durable thread and turn lifecycle
            -> Engine single provider/tool loop
                 -> provider.Gateway
```

`Host` owns backend lifetime and capability issuance. `Agent` owns immutable
execution configuration and run labels without durable conversation identity.
The provider host applies those labels at its fresh and resumed execution
entry points, so command paths cannot create competing metadata flows. `AgentHarness`
owns journals, turns, retries, forks, titles, approvals, todos, SubAgents, and
projection. `Engine` owns one run's provider loop, tool dispatch, compaction
decision, control signals, and events. Gateway owns transport and provider
rendering.

Engine constructs each canonical local tool-call message once. Its execution
history and the internal ToolCall event carry detached copies of that same
message; AgentHarness validates the event identity and saves a deep copy rather
than reconstructing reasoning from stream fragments. This internal payload is
not serialized and is removed at observation boundaries, including raw sinks.
Missing or conflicting messages fail as contract errors.

Hosted-tool events flush preceding assistant text and reasoning into the
journal. Engine preserves these same fragment boundaries in execution history
so the next turn sees the same canonical context prefix. Final reconciliation
still compares complete messages by occurrence: later legitimate reuse of a
tool-call ID is a new exchange. Forks retain strict history and effect isolation;
this write-path correction does not migrate existing invalid histories.

Testing harnesses remain outside production control flow.
