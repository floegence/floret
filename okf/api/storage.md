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

Floret's session-tree domain schema is a permanent v2 -> v3 -> v4 -> v5
lineage. Version 5 is current. The v2 -> v3 edge reconstructs the exact
SubAgent admission authority, v3 -> v4 validates and establishes the root
inventory projection, and v4 -> v5 moves lifecycle identity onto canonical
entries and metadata. Every edge validates its source authority and the final
current invariant.

`runtime.Open` performs migration, logical schema update, and final invariant
verification in one backend transaction. Write failure, cancellation, panic,
schema drift, corrupt state, and future versions roll back or fail closed with
the prior canonical bytes intact. A current store opens without rewriting its
canonical envelope; repeating startup is idempotent.

This automatic Floret-owned migration is distinct from the explicit legacy
physical conversion package. Normal startup must not inspect, convert,
dual-read, or mutate that external schema. Downstream hosts treat backend
records as opaque.

## Runtime storage behavior

The backend kernel coordinates session-tree, prompt-cache, artifact, and todo
facts under the runtime owner. In-process active execution, drafts,
subscribers, and short-lived deduplication are not durable projections. Facts
that survive restart are committed through the canonical journal and domain
snapshot; a failed transaction never reports a successful live mutation.
