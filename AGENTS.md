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
- Conflict resolution must preserve the semantic intent of all involved branches, not just produce text that compiles.
- Before resolving merge or rebase conflicts, review the substantive commits on each side for new features, bug fixes, behavior changes, tests, and user-facing workflows.
- Do not drop, overwrite, or silently weaken current or historical functionality unless the user explicitly approves that product decision.
- If two branches introduce incompatible behavior, surface the product or architecture tradeoff instead of choosing one side silently.
- After resolving conflicts, run focused checks for the affected behavior in addition to the repository quality gate.

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

## IMPORTANT Design Constraints

- `IMPORTANT:` comments mark product, security, or interaction invariants that must stay rare, intentional, and backed by code or tests where practical.
- If a change would remove, bypass, weaken, or contradict an `IMPORTANT:` comment, discuss the design impact with the user and receive explicit confirmation before implementing that change.
- Do not work around an `IMPORTANT:` constraint with hidden fallback behavior, alternate entry points, or silent compatibility paths.
- When adding a new `IMPORTANT:` comment, keep it concise, explain the invariant rather than the implementation detail, and add focused test coverage or another enforceable guard whenever possible.

## Language Policy

- English is the default language for maintained repository content.
- Use English for source identifiers, comments, tests, scripts, docs, and commit/PR-facing text.
- Non-English text is allowed only for explicit localization or language-sensitive fixtures.

## Quality Gate

Run before integration:

```bash
go test ./...
```
