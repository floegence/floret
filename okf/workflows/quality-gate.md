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
GOWORK=off go test -race ./internal/engine ./internal/agentharness ./internal/sessiontree ./internal/storage/sqlite ./runtime ./tools
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
command output. It then runs a fixed public-API consumer test and the published
durable host, custom gateway, tool approval, startup recovery, and Store
maintenance examples. The `Published release adoption` workflow invokes the
same script for a published GitHub release; the workflow does not maintain a
second smoke implementation.

Before a tag exists, validate the embedded consumer and verifier templates with:

```bash
./scripts/check_published_release_adoption.sh --check
```

For context compaction runtime, observation, or Test UI changes, also exercise
the focused scenario target:

```bash
GOWORK=off go test ./runtime ./internal/testui
GOWORK=off go run ./cmd/floret-test-ui -addr 127.0.0.1:8765
# In the Test UI, run the "context compaction" check.
```

# What It Protects

The test suite includes unit behavior, provider contracts, storage behavior,
architecture boundaries, documentation import hygiene, and OKF conformance.

# OKF Maintenance

When code or policy changes alter project knowledge, update `okf/` in the same
change. The OKF conformance tests protect frontmatter, reserved filenames, root
version declaration, update logs, and forbidden downstream import guidance.
