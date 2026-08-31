---
type: Maintainer Workflow
title: Quality Gate
description: Floret changes must preserve architecture rules, public API boundaries, OKF conformance, and repository tests.
resource: /AGENTS.md
tags: [workflow, tests, quality]
timestamp: 2026-06-20T00:00:00Z
---

# Required Checks

Run before integration with workspace discovery disabled so repository checks
cannot accidentally resolve an unpublished sibling module:

```bash
GOWORK=off go test ./...
GOWORK=off go vet ./...
GOWORK=off go test -race ./internal/engine ./internal/agentharness ./internal/sessiontree ./storage ./runtime ./tools
GOWORK=off go test ./florettest ./cmd/examples/...
for example in cmd/examples/*; do GOWORK=off go run "./${example}"; done
GOWORK=off govulncheck ./...
```

Install `govulncheck` from `golang.org/x/vuln/cmd/govulncheck` when it is not
already available. CI runs these repository-wide tests, vet, focused race
packages, public conformance helpers, examples, dependency-boundary checks, and
the reachable vulnerability scan with `GOWORK=off`.

# Published Release Adoption

Repository CI proves that the source tree is internally consistent. After a tag
is published and available through the configured Go module proxy and checksum
database, run the external adoption gate:

```bash
./scripts/check_published_release_adoption.sh <exact-tag>
```

The script creates a blank temporary consumer module with `GOWORK=off`, a fresh
module cache, and no `replace` directive or sibling path. It verifies the exact
resolved module version, module zip, and module checksums through structured Go
command output. It then generates all five host profiles, runs their downstream
smoke and profile-specific approval/recovery behavior tests, and runs the
published durable host, custom gateway, tool approval, startup recovery, and
Store maintenance examples. The `Published
release adoption` workflow invokes the same script for a published GitHub
release; the workflow does not maintain a second smoke implementation.

Before committing a release candidate, run
`scripts/check_candidate_release_adoption.sh` from a clean worktree. It packages
committed `HEAD` into a local immutable module proxy and rejects workspace or
replacement wiring. It intentionally refuses dirty trees so its result cannot
be mistaken for validation of uncommitted source.

This repository gate validates a blank downstream consumer, not Redeven.
Redeven must pin the published tag and run its own notice, module-boundary, and
focused integration checks in a dedicated Redeven feature worktree before that
downstream upgrade is integrated.

Before a tag exists, validate the embedded consumer and verifier templates with:

```bash
./scripts/check_published_release_adoption.sh --check
```

# What It Protects

The test suite includes unit behavior, provider contracts, storage behavior,
architecture boundaries, documentation import hygiene, and OKF conformance.

# OKF Maintenance

When code or policy changes alter project knowledge, update `okf/` in the same
change. The OKF conformance tests protect frontmatter, reserved filenames, root
version declaration, update logs, and forbidden downstream import guidance.
