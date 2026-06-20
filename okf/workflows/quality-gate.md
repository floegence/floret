---
type: Maintainer Workflow
title: Quality Gate
description: Floret changes must preserve architecture rules, public API boundaries, OKF conformance, and repository tests.
resource: /AGENTS.md
tags: [workflow, tests, quality]
timestamp: 2026-06-20T00:00:00Z
---

# Required Check

Run before integration:

```bash
go test ./...
```

# What It Protects

The test suite includes unit behavior, provider contracts, storage behavior,
architecture boundaries, documentation import hygiene, and OKF conformance.

# OKF Maintenance

When code or policy changes alter project knowledge, update `okf/` in the same
change. The OKF conformance tests protect frontmatter, reserved filenames, root
version declaration, update logs, and forbidden downstream import guidance.
