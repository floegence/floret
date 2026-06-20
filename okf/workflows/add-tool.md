---
type: Maintainer Workflow
title: Add a Tool
description: Local tools must be schema-validated, permission-aware, observable, and separate from hosted provider tools.
resource: /tools/tools.go
tags: [workflow, tools, permissions]
timestamp: 2026-06-20T00:00:00Z
---

# When To Use

Use this workflow when adding or changing a local tool capability.

# Steps

1. Define a strict provider-visible schema.
2. Declare effects, read-only status, destructive status, open-world status, and
   permission behavior.
3. Provide resource extraction when approval or observation needs a concrete
   file, command, URL, or domain object reference.
4. Add focused tests for schema validation, permission behavior, execution, and
   output projection.
5. Keep hosted provider tools outside the local tool runtime.

# Guardrails

Risky tools must not silently default to allowed execution. Parallel-safe tools
must be strictly read-only.
