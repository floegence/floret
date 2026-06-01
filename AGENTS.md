# Floret Repository Guide

This file is the repository-level operating guide for `floret/`.

## Git Workflow

- Never develop directly on `main`.
- Every change must be done in a dedicated worktree plus feature branch whenever the repository has an initial commit.
- For an unborn repository, create the initial implementation on a dedicated orphan feature branch first, then integrate it into `main` only after review.
- Do not create backup branches unless the user explicitly asks for one.
- Keep feature branches private until they are ready to integrate.
- Do not introduce or rely on `go.work` or `go.work.sum` in this repository, sibling repositories, or their shared parent directory.
- Do not wire local sibling repositories into manifests, lockfiles, imports, or build/test configuration.
- Resolve conflicts only inside the feature worktree, never on `main`.

Recommended setup after the first commit exists:

```bash
git fetch origin
git switch main
git pull --ff-only

BR=codex/<topic>
WT=../floret-<topic>
git worktree add -b "$BR" "$WT" origin/main
```

## Engineering Principles

- Floret is a reusable interactive AI chat/coding agent runtime, not a graph workflow or multi-agent orchestration framework.
- Keep the engine prompt-first, event-observable, deterministic under fake providers, and easy to test without real model calls.
- Prefer small contracts over framework-style extension layers.
- Keep provider, context, tool runtime, session storage, and host UI concerns separated.
- Important intent and policy decisions must be observable through events or testable state.

## Language Policy

- English is the default language for maintained repository content.
- Use English for source identifiers, comments, tests, scripts, docs, and commit/PR-facing text.
- Non-English text is allowed only for explicit localization or language-sensitive fixtures.

## Quality Gate

Run before integration:

```bash
go test ./...
```
