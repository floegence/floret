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

Complete-domain runtime reads still enter one backend snapshot and read the
exact durable envelope. When those bytes match the last successfully validated
snapshot, the kernel reuses its decoded in-process domain; any byte change
forces strict envelope and schema decoding before the result can replace the
cache. Mutations continue to load an independent transactional state, so a
failed update cannot contaminate the read cache. This optimization adds no
persisted field, schema edge, compatibility reader, or host-visible authority.

The backend kernel keeps the live canonical domain and prompt observation store
in process memory while a turn is running. `AdmitTurn` changes that authority
without encoding the complete session-tree blob; `Checkpoint` clones and writes
the current authority atomically with the prompt cache and root inventory.
Semantic effect intent and terminal commit paths remain durable barriers. A
failed checkpoint leaves the previous durable envelope intact and prevents
startup from claiming uncheckpointed live state after restart.

Between checkpoints, semantic mutations append compact, checksummed
session-tree journal frames. Startup decodes the last checkpoint, replays a
contiguous journal sequence, and rebuilds the in-process authority and indexes.
Only a malformed final frame may be discarded as a torn write; a missing,
out-of-order, or corrupt earlier frame fails closed. A checkpoint folds the
journal into one domain state and starts a fresh sequence. Live drafts, token
flow, subscriber cursors, and transport diagnostics are not journal data.
Effect intent, approval decision/rejection, canonical assistant outcome, and
terminal settlement remain semantic journal barriers, so memory-first admission
is included when a later irreversible boundary is committed.

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
