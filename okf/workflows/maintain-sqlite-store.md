---
type: Workflow
title: Migrate A V1 SQLite Store
description: Explicitly convert exact schema-v16 into the v2 backend format.
resource: /storage/migrate_v2.go
tags: [workflow, storage, sqlite, migration]
timestamp: 2026-07-29T00:00:00Z
---

# Migrate A V1 SQLite Store

1. Stop writers and keep readiness closed.
2. Move every v1 `metadata_records` row into the owning product store. The
   Floret migrator rejects non-empty host metadata.
3. Run `floret-store migrate-v2` with an explicit stable operation ID.
4. Treat only committed success or same-operation replay as completion.
5. Start the v2 Host only after migration succeeds.

The migrator accepts exact schema-v16 and fingerprint only. It exports every
Floret lifecycle domain, replaces v1 tables with two physical backend tables in
one `BEGIN IMMEDIATE` transaction, validates authority, counts, and content
hash, then commits. Error, cancellation, and panic roll back completely.

Different operation IDs, v3-v15, unknown, future, corrupt, fingerprint-mismatched,
or damaged replay content fail explicitly. Normal runtime startup contains no
migration branch beyond returning typed migration-required state.
