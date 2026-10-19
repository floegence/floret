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

Engine constructs each local tool-call message once, then AgentHarness commits
assistant output, calls and opaque provider state in one transaction. Engine
applies only the returned canonical messages. Engine-generated continuation and
validation feedback use the same synchronous commit boundary; effect finalizers
return their canonical result identities. ToolCall, ToolResult and ContextContinue
events observe committed facts without rebuilding or persisting model messages.

One journal projection assembles live execution, restart, Retry, Fork and
compaction. A control result has a deterministic identity derived from its
canonical source; saved continuation output and interaction resolution supply its
content. Compaction validates canonical source boundaries and baselines inside
the installation transaction and returns the entire replacement projection.
Missing boundaries fail explicitly. Historical Engine hashes and released Fork
writer shapes are interpreted only by permanent schema 26 -> 27 migration.

The public gateway projects complete tool-call reasoning into assistant reasoning
once per batch, including calls preceded only by thinking. Already projected
hosted assistant fragments remain a single prefix; parallel calls must not repeat
that reasoning. This projection never mutates canonical messages. See the
[continuation and restart regression](../../runtime/thread_tool_history_test.go).

Testing harnesses remain outside production control flow.
