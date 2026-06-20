---
type: Project Overview
title: Floret Project Overview
description: Floret is a reusable Go runtime for interactive tool-using AI agents.
resource: /README.md
tags: [project, overview, agent-runtime]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Floret is a Go runtime for applications that need interactive agent
conversations without rebuilding the same provider loop, durable thread state,
tool execution, context management, compaction, and observation plumbing in each
host.

It is a reusable agent engine. It is not a graph workflow framework, a
multi-agent orchestration framework, or a product UI framework.

# Integration Model

Hosts integrate through a small public surface:

* [config](api/config.md) resolves provider, prompt, and context policy.
* [runtime](api/runtime.md) runs durable threads or projected turns.
* [tools](api/tools.md) defines and dispatches host-provided local tools.
* [observation](api/observation.md) projects sanitized runtime facts for hosts.

Product concerns such as users, workspace policy, billing, credentials,
provider profile persistence, and UI rendering stay in the downstream host.

# Key Source Files

* [README](/README.md)
* [Repository Guide](/AGENTS.md)
* [Runtime Package](/runtime/runtime.go)
* [Projected Turns](/runtime/projected_turn.go)
