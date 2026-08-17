---
type: Workflow
title: Compose A Durable Host
description: Open one v4 Host and expose only the typed thread service.
resource: /runtime/host_v2.go
tags: [workflow, host, composition]
timestamp: 2026-08-18T00:00:00Z
---

# Compose A Durable Host

1. Construct a `storage.Source` without local module replacement wiring.
2. Call `runtime.Open` in the composition root and retain `*runtime.Host` only
   there.
3. Construct immutable Agents with explicit config and Gateway.
4. Bind an `AgentFactory` through `Host.ThreadService` and retain the returned
   `ThreadService` at the host boundary.
5. Use its `Create`, `View`, `Send`, `Respond`, queue, fork, and delete methods;
   do not expose the backend or import `internal/*` in a downstream product.
6. On shutdown, call `Host.Shutdown(ctx)` and continue waiting after a timeout.

Use `ThreadService.View` and `History` for the initial read model and
`Subscribe` only for transient observation. A reconnect always reloads the
canonical view.
