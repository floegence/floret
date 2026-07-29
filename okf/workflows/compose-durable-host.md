---
type: Workflow
title: Compose A Durable Host
description: Open one v2 Host and distribute only identity-bound narrow handles.
resource: /runtime/host_v2.go
tags: [workflow, host, composition]
timestamp: 2026-07-29T00:00:00Z
---

# Compose A Durable Host

1. Construct a `storage.Source` without local module replacement wiring.
2. Call `runtime.Open` in the composition root and retain `*runtime.Host` only
   there.
3. Construct immutable Agents with explicit config and Gateway.
4. Issue a handle for the exact thread, parent, or recovery target.
5. Convert the handle to a local minimal interface or closure before handing it
   to a service.
6. On shutdown, call `Host.Close`; it joins runtime work and closes the Backend.

There is no generator, bootstrap callback, binder, factory, or secondary Store
to configure.
