---
type: Architecture
title: Host Capability Authority
description: The v5 host owns one typed thread service and validates every durable mutation.
resource: /runtime/host_v2.go
tags: [architecture, authority, capabilities]
timestamp: 2026-08-18T00:00:00Z
---

# Host Capability Authority

`runtime.Host` owns the backend and exposes `ThreadService` through the
composition root. The service binds every command to an exact `ThreadID` and
keeps one in-process actor per active thread. It is the only host-facing owner
of thread views, turns, queues, interactions, effects, forks, and tombstones.
Child threads are ordinary durable threads with explicit parent metadata; a
host must not infer authority from an agent path or task name.

Host and every issued service share one lifetime fence. Shutdown prevents new
work, cancels and joins managed execution, and closes owned storage only after
the join. Delete fences the complete active subtree, waits for provider/tool
and effect work, then commits the canonical tombstone; late output cannot
revive a deleted thread.

Creation, fork, title, queue, turn, interaction, cancellation, and deletion
commands use explicit request identity and replay checks. Canonical journal
acceptance happens before a successful live view is published. Recovery
hydrates the canonical path and re-dispatches only the exact admitted turn,
input, and retry source; hosts do not rebuild a second lifecycle from audit
records.

`ThreadService.List` and `View` are read-only projections and do not grant
mutation authority. `Subscribe` is an observation stream; `ViewVersion` is a
per-thread notification fence, not durable replay state. A dropped or closed
subscriber must reconnect and obtain a complete baseline. Deleted or unknown
threads fail closed.

No public aggregate can recover or reissue composition-root authority. Provider
credentials, endpoint authorization, resource resolution, and product audit
remain host-owned and never become Floret thread authority.
