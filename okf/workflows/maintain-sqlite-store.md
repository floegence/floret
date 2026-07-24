---
type: Adoption Workflow
title: Maintain a SQLite Store
description: Classify and migrate a Floret SQLite Store through typed, explicit, operator-safe maintenance operations.
resource: /cmd/examples/store-maintenance-host/main.go
tags: [workflow, adoption, sqlite, store, maintenance]
timestamp: 2026-07-24T00:00:00Z
---

# Goal

Give operators a calm, predictable maintenance flow that never guesses from
error text and never mutates a Store during discovery.

# Steps

1. Stop Store users and retain exclusive ownership of the database path before
   an apply operation.
2. Call `InspectSQLiteStore` and `VerifySQLiteStore` first. Render their typed
   state, reason, suggested actions, lease-policy state, and safe detail; both
   operations are read-only.
3. For an upgradeable Store, call `MigrateSQLiteStore` in plan mode with the
   exact observed schema as `ExpectedSchema`.
4. Show the ordered steps and a clear consequence summary. Require a deliberate
   operator command before apply; do not trigger migration as a side effect of
   viewing status.
5. Apply with a stable `OperationID`, the same expected schema, and exclusive
   access. Render monotonic typed progress and allow cancellation only while
   `SafeToCancel` is true.
6. Branch on `SQLiteStoreMaintenanceError`, result status, reason,
   `Retryable`, `SafeToRetry`, `Committed`, and `RolledBack`. Verify again after
   success before opening the runtime Store.

# Verify

Run the [maintenance host example](/cmd/examples/store-maintenance-host) or the
[`floret-store` command](/cmd/floret-store). Schema states, migration support,
and typed contracts are authoritative in the [`runtime` API](../api/runtime.md).

# Boundary

Floret owns its schema classification, validation, migration transaction, and
typed progress. The host owns when maintenance runs, operator authorization,
backup/retention policy, UI hierarchy, labels, and recovery guidance. A host
must not inspect or repair Floret tables directly.
