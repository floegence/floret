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
session-tree domain schema lineage. Startup migrates each exact supported older
domain version in the same backend transaction that writes the new version and
verifies current authority. Failure, cancellation, panic, schema drift, or a
future version rolls back and leaves the prior bytes intact. Downstream hosts
must not inspect or patch these opaque records.

The v2.2-to-v3 migration surface provides representability preflight, an
immutable plan, preview, apply, semantic hashes, and a receipt. Only built-in
v2.2 states named by the conversion table are supported. A legal extension
without one unique v3 representation returns `UnsupportedLegacyContentError`
without mutation. Migration-only readers do not become runtime decoders, and
normal startup never converts, dual-reads, or falls back to that external legacy
physical shape. This explicit conversion is separate from automatic internal
domain-version migration inside an already valid v3 backend.
