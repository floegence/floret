---
type: Workflow
title: Compose A Durable Host
description: Open one v3 Host and distribute only bound thread capabilities.
resource: /runtime/v3_host.go
tags: [workflow, host, composition]
timestamp: 2026-07-29T00:00:00Z
---

# Compose A Durable Host

1. Construct a `storage.Source` without local module replacement wiring.
2. Call `runtime.Open` in the composition root and retain `*runtime.Host` only
   there.
3. Construct immutable Agents with explicit config and Gateway.
4. Create or list roots through `Host.Threads`, then bind an exact thread with
   `Host.Thread`.
5. Grant `Thread.Reader`, `Thread.Lifecycle`, `Thread.TurnExecutor`,
   `Thread.Compactor`, or `Thread.SubAgentManager`, and pass only the returned
   narrow interface to the service that needs it.
6. On shutdown, call `Host.Shutdown(ctx)` and continue waiting after a timeout.

Use `ThreadReader.Bootstrap` for the initial read model. There is no generator,
callback bootstrap, binder, or secondary Store to configure.
