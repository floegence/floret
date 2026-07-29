---
type: Public API
title: Runtime Package
description: Immutable Agents, durable Host ownership, and identity-bound capability handles.
resource: /runtime
tags: [api, runtime, agent, host]
timestamp: 2026-07-29T00:00:00Z
---

# Runtime

`runtime.Open` accepts `runtime.Options{Storage: storage.Source}` and returns a
composition-root-only `*runtime.Host`. Host opens the Backend, validates or
initializes the exact logical schema, waits for active runtime work during
close, and closes the Backend exactly once.

Host issues narrow handles:

| Handle | Bound authority |
| --- | --- |
| `ThreadCreator` | root ThreadID and create intent |
| `ThreadReader` | one root thread |
| `ThreadTitleEditor` | one root thread |
| `ThreadForker` | one source root |
| `ThreadDeleter` | one root ownership tree |
| `TurnRunner` | one root and Agent |
| `ThreadCompactor` | one root and Agent |
| `SubAgentManager` | one parent and Agent |
| `SubAgentReader` | one parent |
| `PendingToolRecovery` | one exact settlement target |
| `InterruptedTurnRecovery` | one exact interrupted authority target |
| `ThreadInventory` | bounded store-wide root discovery at composition startup |

Request types omit identities already fixed by a handle. There is no public
bootstrap, binder, factory, family-specific Host options, Store facade, or host
generator.

`runtime.NewAgent` requires a valid `config.AgentConfig` and non-nil
`provider.Gateway`. It snapshots profile, prompt policy, static tools, effect
gate, event sink, dynamic tool surface, loop limits, title mode, capabilities,
and SubAgent timeout. It has no mutating API.

Runtime owns admitted conversation, turn/run lifecycle, projections, approval,
Todo, SubAgent, artifact, pending settlement, provider state, and prompt cache
facts. Hosts read these through handles and do not persist a second Agent
lifecycle.

Opening exact v1 schema-v16 returns typed `MigrationRequiredError`. Runtime does
not migrate or dual-read legacy state.
