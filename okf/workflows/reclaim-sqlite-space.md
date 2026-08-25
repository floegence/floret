---
type: Workflow
title: Reclaim SQLite Space
description: Reclaim free pages in an idle Floret SQLite backend before opening a Host.
resource: /storage/sqlite_maintenance.go
tags: [workflow, storage, sqlite, maintenance]
timestamp: 2026-08-25T00:00:00Z
---

# Reclaim SQLite Space

1. Keep product readiness closed and ensure no Floret Host owns the file.
2. Create a bounded maintenance context and an explicit
   `SQLiteMaintenancePolicy`.
3. Call `storage.MaintainSQLite` before `runtime.Open`.
4. Record the returned action, reason, before/after space use, and duration
   without reading message or record content.
5. Treat schema or integrity errors as startup failures. A busy database,
   timeout, or insufficient temporary disk space is a safe maintenance skip.
6. Call `runtime.Open` exactly once after success or a safe skip.

Fresh files already use incremental auto-vacuum. Legacy NONE files require one
native SQLite `VACUUM` to change the pointer-map layout; subsequent maintenance
uses `incremental_vacuum` and may retain a bounded amount of reusable space.
Maintenance never copies or replaces the database file and cannot run through
an open Floret runtime.
