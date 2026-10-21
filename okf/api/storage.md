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
-> v7 -> v8 -> v9 -> v10 -> v11 -> v12 -> v13 -> v14 -> v15 -> v16 -> v17 -> v18 -> v19 -> v20 -> v21 -> v22 -> v23 -> v24 -> v25 -> v26
-> v27 -> v28 -> v29 -> v30 -> v31 lineage. Version 31 is current. The v2 -> v3 edge reconstructs the exact
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
and copied-entry evidence. The v10 -> v11 edge preserves existing bytes while
admitting explicit stop provenance and confirmed canceled effect results. Old
records without a stop source stay unchanged. A canceled effect with a result
must refer to the matching canonical canceled tool result and exact execution.
The v11 -> v12 edge preserves historical bytes and admits validated input requests
only on completed tool-result entries with exact execution identity. Older schema
validators reject this new contract. Input interactions and responses reuse the
canonical journal; no host-owned lifecycle table is introduced.

The v12 -> v13 edge preserves existing canonical bytes and admits typed
context-only turns without a user message. Historical `/compact` conversations
remain ordinary turns. A validated compaction installs its summary, lifecycle,
estimate, and independent-turn completion in one domain transaction; the same
installer serves automatic compaction while its ordinary turn remains active.

The v13 -> v14 edge makes explicit `Entry.RunID` on canonical lifecycle markers
the only runtime execution identity. Historical markers missing that field are
repaired only from a validated marker identity or an unambiguous ancestor path
for the same turn. Conflicting metadata, missing ancestry, invalid identities,
and redacted observation values fail closed. Custom observation entries retain
their original content and never advance execution identity. The migration
preserves entry IDs, ordering, timestamps, Raw/RawHash, final-output
references, provider state, and all earlier migration edges; it writes the v14
records and removes v13 records in one transaction.

The v14 -> v15 edge adds typed thread references and the optional referenced
ThreadID. It validates exact v14 authority, preserves all canonical bytes,
request fingerprints and execution identities, and installs v15 in the same
startup transaction. v14 rejects new thread-reference shapes, including queued
input; current v15 opens without rewriting records. The derived search index stores only canonical source locators, metadata and
contentless postings, maintained in the same canonical transaction. Ordinary
startup verifies source correspondence and reuses postings. Frozen pagination
snapshots never persist.

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
additions in one transaction. Ordinary mutations do not encode unrelated threads,
diff whole JSON documents, or rewrite full active paths. The v5 recovery journal
exists only in the v5 -> v6 migration reader and is deleted after migration.

Ordinary session-tree writes stage only changed keys and affected journal indexes.
They validate new canonical entries and their exact thread, turn, and artifact
dependencies, rather than cloning or revalidating unrelated history. The repository
lock isolates staging, the working maps are restored before the backend callback
returns, and successful backend commit installs the staged values once. Failure,
cancellation, and panic preserve both prior memory and durable records. Startup,
migration, and explicit verification retain complete domain validation; schema 15
adds thread references while preserving existing canonical history bytes.

Immediately before an ordinary provider dispatch, the backend kernel commits
the current raw segments, toolsets, immutable Turn surface, and request lineage
in one prompt checkpoint. The in-memory prompt authority advances only after
that transaction succeeds. Restart can therefore restore the exact execution
surface and render prefix even when no terminal turn was written; write failure
or cancellation leaves both memory and storage unchanged.

Logical storage version 8 uses `floret.domain.prompt.v2`: an explicit schema
marker, small per-scope counters, and records keyed by prompt scope, category,
and append ordinal. Segments, toolsets, request attempts, and responses retain
their complete values and ordering. A repeated RequestID is a separate attempt,
not an overwrite. The kernel reads the requested scope and overlays only its
uncommitted facts; successful dispatch and turn boundaries clear those pending
facts after commit. Shutdown flushes remaining additions. The legacy global
snapshot decoder lives only in [startup migration](../../internal/storage/prompt_migration.go).

SQLite physical format 4 uses unique indexed atomic term/document rows and
fixed source-boundary metadata. The transactional 3 -> 4 migration removes only
the derived FTS5 index, then the canonical owner rebuilds the projection before
Host exposure. Failure, cancellation and panic roll back the physical change
and derived rebuild together. Canonical opaque records remain byte-identical.
Optional `storage/spi.IndexDocumentAppender` atomically inserts delta terms
and replaces exact-scoped document metadata; missing identity returns
`spi.ErrNotFound` and errors never switch implementations. Existing
`IndexWriter.PutIndexDocument` remains complete replacement. The v7 public
addition serves SQLite's merged-source update path and does not enlarge any
required backend interface.

SQLite physical format 3 introduced persistent metadata and contentless FTS5
postings. The contiguous 2 -> 3 edge joins logical storage 8 -> 9 in the runtime
startup transaction, preserving domain schema 19. Optional transaction-local
IndexReader and IndexWriter keep physical indexing separate from canonical
query semantics; other backends use the KV adapter.

SQLite physical format 2 uses a rowid records table with the same unique
`(namespace, key)` primary key; metadata retains its original layout. Format 1
conversion copies opaque records with SQL, verifies their content, and commits
with cache splitting, session-tree migration, and logical version update. No
extra database or recovery log is introduced. Unknown, mixed, future, and
drifted formats fail closed. Current startup does not rewrite history, and
compaction continues to change only effective model context.

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
transaction. Both paths validate canonical state and the complete search
projection; inspection omits text postings that would be discarded, while formal
startup verifies and reuses the searchable index or builds missing derived
state transactionally. Optional counted startup work reports real thread totals. Missing files are reported without
creating them. `ErrStoreTooNew`
retains `ErrUnsupportedSchema` classification for newer logical formats.
Physical checks report `storage.ErrUnsupportedSQLiteFormat`,
`storage.ErrSQLiteTooNew`, and `storage.ErrSQLiteIntegrity`. Driver and operating
system errors retain their original identities, including temporary lock errors.

Physical format conversion contributes to `MigrationRequired`. A transaction-local
inspection overlay merges and sorts each modified namespace once; writes invalidate
that namespace, and subsequent pages reuse the merged records. Phase order stays
`migrating`, then `verifying`; fresh and current stores report only `verifying`.

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

The v9 -> v10 edge accepts optional durable user-context snapshots and typed tool
validation feedback. It validates exact v9 source authority, preserves raw
journal payloads and request records, and commits the new namespace and complete
current invariant atomically. Current v10 opens without rewriting bytes; failures,
cancellation, panic, mixed formats, and future versions leave the store unchanged.

Schema 16 appends the automatic 15 -> 16 edge for typed steering request and resolution facts. Historical input and queued facts remain unchanged; pending input is not model history until its application fact.

Schema 16 -> 17 adds managed snapshots and exact compaction baselines. Historical
facts remain byte-identical; prose and queues are not inferred. The current
envelope upgrades in the existing namespace, ensuring older programs fail closed.
See [managed context](../decisions/managed-model-context.md).

The 17 -> 18 edge upgrades the domain envelope atomically for request-fitted
output budgets. Existing canonical entries retain their exact facts; historical
model capabilities and request limits are never inferred.

The 18 -> 19 edge admits typed parent-event receipt and admission entries in the
same journal and envelope namespace. It preserves historical canonical bytes
and every released edge, rejects older writers, and never backfills historical
terminal events. Startup opt-in bootstraps only unresolved direct-child
interactions once. Source facts and receipts commit in one existing domain
transaction. Admission freezes bounded text and a closed event range after tool
pairs; coverage is derived from the journal, including retry, fork and compaction.
There is no durable inbox or separate consumption table. See
[runtime parent events](runtime.md#passive-parent-events).

# Completed-error input migration

Domain schema 20 appends 19 -> 20 in the existing namespace with a new envelope.
Historical schemas still require success for tool-requested input; current writes
accept completed success or error results. Exact v19 validation precedes migration,
which preserves canonical facts and verifies v20 in the startup transaction. Older
writers reject the envelope. All older migration edges remain automatic.

## Returned uncertainty migration

Schema 21 appends 20 -> 21 in the same physical namespace with a new envelope.
It retains every historical edge and canonical fact. Returned uncertainty is an
error ToolResult with explicit effect_outcome; the EffectAttempt remains failed.
No new recovery ledger is created. Older writers reject this envelope. Startup
validates source and current invariants atomically and current restart is read-only.

## Request-bound accounting

Domain schema 24 appends the atomic 23 -> 24 edge and preserves canonical facts.
`TokenEstimate.Characters` / `RequestEstimate.Characters` provide decoded Unicode
code points from the existing prepared-input collector.
[Context reconciliation](../../observation/context_accounting.go) validates the
same-request native input overview and independent visible character distribution.
Character totals and details add exactly; ciphertext, media budgets and protocol
framing never become characters. Historical missing characters stay unavailable.

The additive v7.60 API decision retains published token contracts and deprecates
legacy display breakdown/remainder fields without changing their semantics.
Adapters explicitly supply input presence and scope; only qualified request input
with complete visible coverage calibrates Engine pressure. Older anchors expire
without changing stored bytes. Existing estimate and bounded overflow recovery
remain the sole execution path. No host scanner, durable ledger or online send-time
counting is introduced.

## Canonical output-source migration

Domain schema 25 appends the atomic 24 -> 25 edge. Minimal content/reasoning
source references remain on the original assistant/tool entries. Existing public
item IDs, text, tool facts, execution identity, timestamps and final-response
entry references remain intact. Released implicit adjacency/shared reasoning is
decoded only during migration. Hidden validation-response reasoning receives an
entry-scoped identity without inventing provider request IDs. Mixed batches use
canonical parent relationships, never observation logs or insertion-adjacent
branches. A new envelope rejects old writers in the existing physical namespace.
Current startup is byte-stable; all earlier migration edges remain mandatory.

## Compaction decision evidence migration

Domain schema 26 appends the atomic 25 -> 26 edge. Historical journal payloads
remain unchanged and absent frozen request evidence remains unavailable. The new
envelope rejects older writers; current startup remains byte-stable. Optional
compaction decision facts belong to the canonical lifecycle, never a second
request ledger or host-owned migration.


## Canonical compaction boundary migration

Domain schema 27 appends the atomic 26 -> 27 edge without changing earlier edges.
It validates all checkpoints, including inactive branches and nested Fork copies,
in ancestor order. Canonical message references, unique same-path stored aliases,
and exact released Go JSON/hash identities are the only accepted authority.
Historical counts corroborate identity; they never select a boundary. Fork
reconstruction verifies every copied fact against the released writer before
rewriting a reference. Missing or ambiguous authority fails without mutation.

Only proven boundary references and their Raw/hash representations change. All
other durable facts and unavailable historical decision evidence remain intact.
Migration, schema update and current validation share one startup transaction;
read-only inspection runs the same parser in its disposable overlay. Repeated
current startup does not rewrite canonical bytes. Runtime accepts canonical
references only and never guesses a replacement tool exchange.

Release adoption gates must continue immutable, checksummed synthetic
released-writer conversations and their Forks through public APIs after restart.
See the [release requirement](../../AGENTS.md#historical-conversation-release-gate).

### Request measurement boundary migration

Domain schema 28 appends the atomic 27 -> 28 edge. It validates schema 27
authority and changes the writer envelope without rewriting canonical messages,
compactions, native usage or absent evidence. Existing input anchors are
revalidated; response-end calibration requires new attempt-bound protocol proof.
Current-schema restart is byte-stable, and old writers reject the new envelope.

### Explicit Goal continuation migration

Domain schema 29 appends the atomic 28 -> 29 edge. Shared current validation
admits a typed Goal continuation with durable requested intent and no user entry;
historical validators retain their released pure-compaction constraints. Migration
preserves every canonical fact and earlier edge, changes the writer envelope in
the existing namespace, and rejects older writers. Invalid source, future schema,
write failure, cancellation and panic roll back the entire startup transaction.
Current restart is byte-stable. See [Goal execution](../decisions/thread-goals.md).

## Exact derived-document reads

Optional `storage/spi.IndexDocumentReader` reads one exact key within one thread
scope. Missing keys and scope mismatches return `ErrNotFound`; other failures
remain explicit. SQLite and the canonical KV adapter implement it. Existing
custom `IndexReader` implementations keep their published query contract; the
owner selects the requested key from their scoped candidates when they do not
implement the additional capability. No read error switches implementations.

Query projection continuations retain source locators and lifecycle metadata,
not complete historical projected bodies. Transactions materialize only changed
records and install their continuation after commit. Path paging follows derived
ancestor locators, never a complete Path followed by slicing. These indexes are
rebuildable, memory-only metadata; domain schema and canonical bytes do not change.
Canonical body residency and giant-message indexing still require bounded storage
work; these improvements do not establish a global history memory bound.

## Canonical body blocks

The permanent [29 -> 30 migration](../../internal/sessiontree/backend_repo_v30.go)
separates large string fields from entry headers into immutable 16 KiB byte
blocks. [Body records](../../internal/sessiontree/body_records.go) retain SHA-256,
length and same-transaction reference counts. Equal fields and Fork copies share
one body. Removing an owner releases its reference; the final owner deletes all
blocks. The canonical entry raw hash remains unchanged and full reconstruction
must match it. Startup checks every reference, block key, length and hash and
rejects orphans, unsupported fields and corrupt data without mutation.

Body range reads touch only intersecting blocks and return whole UTF-8 runes.
Explicit complete materialization streams and hashes all blocks. These are
internal storage mechanics, not a host authorization or presentation API.
After validation, decoded journals retain body locators instead of the extracted
strings. Requested complete entries hydrate in the caller's backend snapshot
and remain detached from retained journal headers. A process-wide 128 MiB LRU
caches immutable, hash-verified source bytes; oversized bodies bypass it.
Actor residency, other payload fields, actual heap/RSS and complete Flower
interaction budgets still require separate qualification.

## Bounded canonical body access

The optional public runtime.ThreadBodyReader delegates to canonical block
storage, checking a freshly reconstructed threadquery.Scope and current active
path on every read. Entry/field locators and optional SHA-256 are identities,
never authority. Source chunks preserve UTF-8 and CRLF and stay within 16 KiB;
encoded responses remain below 64 KiB. Full-field hashes allow consumers to
verify streaming copy or export after restart. No complete entry or source-cache
population is needed for a chunk. Hosts must separately authorize private tool
arguments and reasoning, as for ThreadQueryReader. This capability is one part
of long-history delivery; it does not qualify bounded presentation or product UI.

# Body-free canonical metadata pages

The internal PathMetadataReader shares PathPage's validated ancestry/cursor
kernel and returns source field hashes and lengths without reading body blocks.
Its Entry is deliberately incomplete: extracted fields and Raw are absent and
must never be used as provider context. The capability is internal substrate,
not a public display window or a 512 KiB presentation guarantee. Metadata may
still contain large structured Activity fields. Tests use a backend that rejects
body reads and verify detached ownership, active-path cursor rejection,
cancellation and restart. See [implementation](../../internal/sessiontree/path_metadata.go)
and [tests](../../internal/sessiontree/path_metadata_test.go).

PathMetadataWindow extends that kernel with chronological before/after/around
selection in at most 256 entries. It uses ancestry lookup to find the selected
interval without walking from the root or slicing a complete Path. A frozen
leaf remains valid during append-only growth, but is rejected if no longer on
the active path. Around selection reuses the same field detachment path. No
cursor registry or long-lived backend transaction is retained. Exact wall-time
and wire facts survive restart; Go monotonic-clock metadata is not durable.

Internal JournalMetadataReader pages immutable physical inventory in at most
256 body-free records, including inactive branches. An exclusive frozen ordinal
boundary survives append and active-leaf movement without holding a database
transaction. Queue and restored-input hydration consume this one inventory;
consumed input payloads are released and explicit restore markers resolve the
latest immutable addition. This inventory is never provider context or a second
transcript. See [inventory regressions](../../internal/sessiontree/journal_metadata_test.go).

The subsequent [30 -> 31 edge](../../internal/sessiontree/backend_repo_v31.go)
extracts typed activity JSON into the same body-block inventory. Both source
and target invariants commit atomically; failed writes, cancellation, panic,
corruption and future formats preserve the original bytes. Current restart does
not rewrite canonical records. [Activity regression](../../internal/sessiontree/activity_body_test.go)
checks exact canonical state, chunk hashes, sharing deletion and rollback.
