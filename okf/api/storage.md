---
type: Public API
title: Storage Package
description: Opaque storage Sources, physical SPI contracts, and durable domain migration.
resource: /storage
tags: [api, storage, backend, migration]
timestamp: 2026-08-18T00:00:00Z
---

# Storage

`storage.Source` is an opaque runtime storage configuration.
Ordinary hosts cannot transact through it or use it as a lifecycle query path.
Floret owns tuple-key encoding, envelopes, indexes, and all domain
interpretation. Implementations of `storage/spi` remain physical backends and
must not be decoded into a second Agent model by hosts.

## Domain migration

Floret's session-tree domain schema is a permanent v2 -> v3 -> v4 -> v5 -> v6
-> v7 -> v8 -> v9 lineage. Version 9 is current. The v2 -> v3 edge reconstructs the exact
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
final current invariant. The v7 -> v8 edge identifies only an Engine-generated
continuation message immediately paired with its exact `context_continue` save
point and classifies it as a control signal. Malformed or ambiguous pairs fail
closed without changing the store. The v8 -> v9 edge removes duplicate
Thread, Turn, and Run identity from context payloads and makes each canonical
entry authoritative. Within one Turn, a post-interaction Run advances from the
latest canonical entry carrying `RunID`; it does not reuse the initial Run.
The migration repairs the released fork mismatch only with exact fork ancestry
and copied-entry evidence.

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

Immediately before an ordinary provider dispatch, the backend kernel commits
the current raw segments, toolsets, immutable Turn surface, and request lineage
in one prompt checkpoint. The in-memory prompt authority advances only after
that transaction succeeds. Restart can therefore restore the exact execution
surface and render prefix even when no terminal turn was written; write failure
or cancellation leaves both memory and storage unchanged.

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

## Coordinated maintenance

`runtime.InspectSQLite` opens an existing file read-only and evaluates the same
logical and domain migration code in a transaction-local memory overlay. It
creates no Host, never dispatches providers or tools, and reports whether a
migration would be needed. Formal startup verifies again under its writable
transaction. Missing files are reported without creating them. `ErrStoreTooNew`
retains `ErrUnsupportedSchema` classification for newer logical formats.
Physical checks report `storage.ErrUnsupportedSQLiteFormat`,
`storage.ErrSQLiteTooNew`, and `storage.ErrSQLiteIntegrity`. Driver and operating
system errors retain their original identities, including temporary lock errors.

`storage.BackupSQLite` exclusively creates a destination containing committed
SQLite state, including WAL records. It rejects a live Host, existing destination,
and cancelled context. The caller owns writer exclusion across the complete
multi-store snapshot. Floret never opens another owner's database.
Read-only maintenance removes only sidecars absent before its connection opened;
existing WAL and shared-memory files remain owned by their original writer.

`runtime.Options.DeferExecution` and `Host.Activate` provide one process-local
activation boundary for coordinated startup. Before activation, `View` and
`ImportPendingInputs` can hydrate and migrate without recovery dispatch. Explicit
execution commands return `ErrExecutionDeferred`. Closing an unactivated Host
never activates it. Default Hosts preserve immediate execution behavior.

`Host.PrepareRestore` requires deferred execution and is applied to a staged
snapshot. It settles unfinished turns using canonical cancellation and resolves
outstanding interactions. Queue deletion records retain the original queue input
as the read-only `ThreadView.RestoredInputs` projection. Canonical records mark
restore cancellation with `restored_from_backup=true`; those turns return
`ErrRestoredTurn` on retry. Existing unknown-effect turns retain their non-retryable
classification. No alternate Agent ledger or replacement lifecycle is introduced.
Preparation is idempotent; interrupted multi-thread preparation is retried before
the host application publishes the restored storage set.

Evidence: `runtime/storage_maintenance_test.go`, `runtime/storage_inspection.go`,
`runtime/storage_restore.go`, and `storage/sqlite_snapshot.go`.
