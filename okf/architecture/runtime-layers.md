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
execution configuration without durable conversation identity. `AgentHarness`
owns journals, turns, retries, forks, titles, approvals, todos, SubAgents, and
projection. `Engine` owns one run's provider loop, tool dispatch, compaction
decision, control signals, and events. Gateway owns transport and provider
rendering.

Testing harnesses remain outside production control flow.
