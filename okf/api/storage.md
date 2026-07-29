---
type: Public API
title: Storage Package
description: Opaque transactional Backend SPI and explicit v1-to-v2 migration.
resource: /storage
tags: [api, storage, backend, migration]
timestamp: 2026-07-29T00:00:00Z
---

# Storage

`storage.Source` opens one `storage.Backend`. Runtime Host takes exclusive
ownership of the opened Backend lifecycle. Backends expose only snapshot
`View`, serializable `Update`, namespaced `Get` and bounded lexicographic
`Scan`, `Put`, `Delete`, and `Close`.

Floret owns the tuple key codec, versioned JSON envelopes, indexes, lifecycle
authority, and all domain interpretation. Memory and SQLite are physical
sources for the same internal domain kernel. Third-party implementations run
`florettest.RunBackendContract`.

SQLite v2 contains only `floret_backend_metadata` and
`floret_backend_records`. `storage.MigrateV2` and `cmd/floret-store migrate-v2`
accept only exact schema-v16 and never run during normal startup. Non-empty v1
`metadata_records` are rejected because they are host-owned product data.
