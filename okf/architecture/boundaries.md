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
* product-neutral pending approval snapshots;
* runtime observation;
* control signal contracts;
* opaque model state lifecycle;
* engine thread-tree lifecycle, including child-thread fork mode, stop/close,
  replayable fork operations, prompt cache retention, and engine-data deletion.

The host owns:

* product UI and user policy;
* credentials and provider profile persistence;
* durable product metadata;
* workspace-specific resource policy;
* domain tools, approval UI, and product approval summaries.

# Maintenance Notes

When a change adds a new host-facing capability, expose it as general public API
with tests and documentation. Do not move product-specific policy into Floret
core to make one downstream integration easier.

Hosts may choose when product actions stop or delete work, but they should
express those choices through Floret runtime APIs. Stop-style product actions
close unfinished Floret subagents and keep history; delete-style product actions
delete Floret-owned thread trees through `runtime.Host.DeleteThread`. Floret
owns the atomic engine-store deletion of the resolved tree; the host remains
responsible for deleting or retaining its separate product records.

Cross-store product fork coordination stays in the host. Floret owns only its
operation-marked engine thread-tree plan and result. A host should persist its
own product snapshot and use the same public `ForkOperationID` when resuming;
it must not inspect Floret operation tables or compensate by deleting Floret
targets after an uncertain outcome.

# Key Source Files

* [Repository Guide](/AGENTS.md)
* [README Stable API](/README.md)
* [Architecture Tests](/internal/architecture/architecture_test.go)
