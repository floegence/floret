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
* runtime observation;
* control signal contracts;
* opaque model state lifecycle.

The host owns:

* product UI and user policy;
* credentials and provider profile persistence;
* durable product metadata;
* workspace-specific resource policy;
* domain tools and approval UI.

# Maintenance Notes

When a change adds a new host-facing capability, expose it as general public API
with tests and documentation. Do not move product-specific policy into Floret
core to make one downstream integration easier.

# Key Source Files

* [Repository Guide](/AGENTS.md)
* [README Stable API](/README.md)
* [Architecture Tests](/internal/architecture/architecture_test.go)
