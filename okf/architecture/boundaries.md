---
type: Architecture Boundary
title: Public and Host Integration Boundaries
description: Floret keeps a compact public API and separates reusable engine contracts from host product policy.
resource: /AGENTS.md
tags: [architecture, boundary, public-api]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Floret exposes a compact downstream API and keeps implementation packages under
`internal/`. Downstream applications should integrate through the public
packages described in [Public API](../api/) and should not bypass `runtime` to
manage Floret-owned journals, prompt cache records, provider ledgers, or engine
contracts.

# Boundary Rules

Floret owns:

* provider loop execution;
* local tool dispatch;
* permission, resource, and approval lifecycle;
* the product-neutral aggregate root/descendant approval queue;
* runtime observation;
* control signal contracts;
* admitted user/assistant conversation, ordered user references, canonical
  titles and typed failures, canonical turn pages, projections, and typed Agent
  todo state;
* opaque model state lifecycle and persistence in Floret Store;
* engine thread-tree lifecycle, including child-thread fork mode, stop/close,
  replayable fork operations, prompt cache retention, and engine-data deletion;
* Floret Store inspection, schema verification, migration planning/application,
  and product-neutral maintenance facts.

The host owns:

* product UI and user policy;
* credentials, provider routing, wire adapters, and provider profile persistence;
* durable product metadata;
* workspace-specific resource policy;
* concrete domain tool actions and their defense-in-depth authorization;
* approval UI, localized copy, product approval summaries, and Store maintenance
  presentation and operator authorization.

Hosts provide provider credentials, provider profiles, and direct wire adapters,
but they do not persist opaque continuation, context usage, compaction state,
assistant/tool history, admitted references, canonical title/failure, turn/run
state, Agent todos, approval lifecycle/queue, or another model-visible message
projection. Those facts are read through `ReadThread`, `ListThreadTurns`,
`ReadApprovalQueue`, `ReadThreadAgentTodos`, `ReadThreadContext`,
`ReadTurnProjection`, and detail APIs. Product/UI DTO mapping may be transient
or response-scoped, never a second durable engine source of truth.

# Maintenance Notes

When a change adds a new host-facing capability, expose it as general public API
with tests and documentation. Do not move product-specific policy into Floret
core to make one downstream integration easier.

`runtime.Store` is opaque and owned by the composition lifetime owner.
`ConfigureHostCapabilities` can run only once for that non-copyable Store and
seals `HostBootstrap` when its callback returns. The callback distributes narrow
binders, which remain private to the composition root. Coordinators and runs
receive only an exact authority-bound factory or handle and cannot mint an
unrelated capability. Provider-free binders validate exact canonical authority
using `context.Context` before returning a handle; create instead binds an exact
absent `ThreadID` plus durable `CreateIntentID`. The owner closes the Store once;
close fences new operations and waits for active finalization.
`OpenSQLiteStore` accepts only a context-aware request bound to a prior
missing/empty/current inspection and never migrates implicitly. It creates only
after an in-transaction no-user-schema check, or opens exact v16 after
in-transaction identity, lease, contract, and authority verification. Explicit
maintenance apply is the one forward migrator for supported v3 through v15
stores. Exact v14/v15 migrate directly. Published historical v3
through v13 stores migrate only with quiescent execution authority; active or
ambiguous authority is rejected atomically rather than assigned synthetic
`ThreadID`, `TurnID`, or `RunID` values. Version 15 was an intermediate
repository schema rather than a released tag, but remains supported input.
Legacy artifacts additionally require one exact canonical tool-result binding
and no obsolete product URL; otherwise migration preserves the source Store and
fails instead of encoding downstream routing in Floret.
Unknown, unversioned, invalid-contract, future, or fingerprint-mismatched
databases return typed facts without a dual-read or host repair path. Open
failures use the typed maintenance error with operation `open`; the host
re-inspects rather than inferring rollback from failed post-transaction WAL
initialization. See the
[`runtime` Store contract](../api/runtime.md) for the authoritative API and
state vocabulary.

Hosts may choose when product actions stop or delete work, but they should
express those choices through Floret runtime APIs. Stop-style product actions
close exact unfinished Floret subagents through their parent-bound
`SubAgentHost` and keep history; delete-style product actions delete
Floret-owned thread trees through
`ThreadDeleteHost.DeleteThread`. Floret owns the atomic engine-store deletion
of the resolved tree; the host remains responsible for deleting or retaining
its separate product records. Floret root deletion never treats generic host
metadata as canonical Agent state.

The Test UI keeps its provider-profile and local session configuration in a
separate WAL sidecar keyed by the canonical runtime database path. It never
queries, imports, maps, or repairs host metadata from the Floret runtime
database. SQLite Test UI mode rejects `:memory:`; explicitly ephemeral Test UI
state uses memory mode instead of creating a disk sidecar for an in-memory
runtime.

Cross-store product fork coordination stays in the host. Floret owns only its
operation-marked engine thread-tree plan and result. A host should persist its
own product snapshot and use the same public `ForkOperationID` when resuming;
it must not inspect Floret operation tables or compensate by deleting Floret
targets after an uncertain outcome.

# Key Source Files

* [Repository Guide](/AGENTS.md)
* [README Integration Surface](/README.md)
* [Architecture Tests](/internal/architecture/architecture_test.go)
