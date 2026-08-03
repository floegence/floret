---
type: Public API
title: Storage Package
description: Opaque ordinary-host Sources, advanced physical storage SPI, and explicit v2.2-to-v3 migration.
resource: /storage
tags: [api, storage, backend, migration]
timestamp: 2026-07-29T00:00:00Z
---

# Storage

`storage.Source` is an opaque value consumed by `runtime.Open`. Ordinary hosts
cannot transact through it or use it as an Agent lifecycle query path. Runtime
Host takes exclusive ownership of the opened storage lifecycle.

Floret owns the tuple key codec, versioned envelopes, indexes, lifecycle
authority, and all domain interpretation. Memory and SQLite are physical
sources for the same internal domain kernel. Advanced physical implementations
use `storage/spi` and its dedicated conformance suite; SPI records remain opaque
and must not be decoded into another Agent model.

Within an existing v3 backend, the storage kernel owns a permanent internal
session-tree domain schema lineage. Version 4 is current: v2 reconstructs exact
SubAgent admission authority through v3, then v3 creates the strict derived
root-thread inventory while advancing to v4. The inventory and complete domain
commit in one transaction. Current startup decodes the complete domain once and
verifies that the inventory is its exact canonical projection; runtime inventory
reads then decode only that bounded record. Failure, cancellation, panic, a
missing or mismatched inventory, schema drift, or a future version rolls back or
fails closed and leaves prior bytes intact. Downstream hosts must not inspect or
patch these opaque records.

The v2.2-to-v3 migration surface provides representability preflight, an
immutable plan, preview, apply, semantic hashes, and a receipt. Only built-in
v2.2 states named by the conversion table are supported. A legal extension
without one unique v3 representation returns `UnsupportedLegacyContentError`
without mutation. The conversion writes canonical session state and its derived
v4 root inventory in the same physical transaction, and binds both records into
the target semantic hash. Migration-only readers do not become runtime decoders,
and normal startup never converts, dual-reads, or falls back to that external
legacy physical shape. This explicit conversion is separate from automatic
internal domain-version migration inside an already valid v3 backend. Immutable
`/1` plans issued before the v4 inventory remain valid: applying one commits its
exact v3 target, startup advances it through the normal v3-to-v4 edge, and later
replay verifies the original semantic receipt against that upgraded authority.
