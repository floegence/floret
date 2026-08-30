---
type: Public API
title: Storage Package
description: Opaque storage Sources, physical SPI contracts, and durable domain migration.
resource: /storage
tags: [api, storage, backend, migration]
timestamp: 2026-08-18T00:00:00Z
---

# Storage

`storage.Source` is an opaque value consumed exclusively by `runtime.Open`.
Ordinary hosts cannot transact through it or use it as a lifecycle query path.
Floret owns tuple-key encoding, envelopes, indexes, and all domain
interpretation. Implementations of `storage/spi` remain physical backends and
must not be decoded into a second Agent model by hosts.

## Domain migration

Floret's session-tree domain schema is a permanent v2 -> v3 -> v4 -> v5 -> v6
-> v7 lineage. Version 7 is current. The v2 -> v3 edge reconstructs the exact
SubAgent admission authority, v3 -> v4 validates and establishes the root
inventory projection, and v4 -> v5 moves lifecycle identity onto canonical
entries and metadata. The v5 -> v6 edge replays any pending recovery frames,
validates the complete authority, and splits the monolithic checkpoint into
thread, entry, artifact, and compact index records. The v6 -> v7 edge fills
exact historical run identity, deletes effect-retry authority, and atomically
fails active turns whose dispatched effect outcome is unknown. It also removes
Effect Attempt entries copied by the released v6 fork implementation only when
the complete fork ancestry, turn, run, request, and terminal state prove that
they are historical source-thread authority. Active, unrelated, or conflicting
records still fail closed. Every edge validates its source authority and the
final current invariant.

`runtime.Open` performs migration, logical schema update, and final invariant
verification in one backend transaction. Write failure, cancellation, panic,
schema drift, corrupt state, and future versions roll back or fail closed with
the prior canonical records intact. A current store opens without rewriting its
canonical records. During v5 migration, the exact adoption repair accepts a
tool-result Raw value only when its stored hash is valid and replacing JSON's
legacy `\\ufffd` escape produces the exact current projection. Floret validates
the correspondingly repaired legacy root inventory before writing v6 atomically.
Other representation or authority differences remain corrupt. Repeating
startup is idempotent.

This automatic Floret-owned migration is distinct from the explicit legacy
physical conversion package. Normal startup must not inspect, convert,
dual-read, or mutate that external schema. Downstream hosts treat backend
records as opaque.

## Runtime storage behavior

The backend kernel coordinates session-tree, prompt-cache, artifact, and todo
facts under the runtime owner. In-process active execution, drafts,
subscribers, and short-lived deduplication are not durable projections. Facts
that survive restart are committed as affected canonical records; a failed
transaction never replaces the validated in-memory authority.

Terminal turn settlement writes its affected session-tree records and prompt
state in one transaction. Ordinary mutations do not encode unrelated threads,
diff whole JSON documents, or rewrite full active paths. The v5 recovery journal
exists only in the v5 -> v6 migration reader and is deleted after migration.

## SQLite space maintenance

Fresh SQLite stores select `auto_vacuum=INCREMENTAL` before creating tables.
`MaintainSQLite` is the only public physical space-reclamation boundary. A host
may call it for an idle file before `runtime.Open`, with explicit file-size,
reclaimable-byte, reclaim-ratio, and retained-free-space thresholds.

Maintenance validates the exact Floret physical schema and SQLite integrity.
An older `auto_vacuum=NONE` file is converted with SQLite's native `VACUUM` only
when sufficient temporary disk space is available. An incremental store uses
`incremental_vacuum`. A database owned by an open Floret runtime, a busy file,
insufficient disk space, or an expired maintenance context is safely skipped
and reported through `SQLiteMaintenanceResult`; corruption and schema drift
remain errors. No record is decoded, copied, or replaced by the host.
