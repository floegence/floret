# Floret Repository Guide

This file is the repository-level operating guide for `floret/`.

## Git Workflow

Goals:

- keep development, release, and open-source hygiene consistent and auditable;
- never develop directly on `main`;
- preserve every intentional commit;
- keep local `main` and `origin/main` aligned whenever `main` is pushed;
- keep repository workflow rules in `AGENTS.md`.

### Main Rules

- Never develop directly on `main`.
- Every change must be done in a dedicated worktree plus feature branch whenever the repository has an initial commit.
- For an unborn repository, create the initial implementation on a dedicated orphan feature branch first, then integrate it into `main` only after review.
- `main` is only for `pull --ff-only` and final integration.
- Do not leave uncommitted changes in the `main` worktree.
- Do not create backup branches unless the user explicitly asks for one.
- If local `main` is pushed, push the full current local `main` tip together with all of its latest commits.
- Do not partial-push `main`, and do not update `origin/main` through another branch while newer local `main` commits remain unpublished.
- One feature equals one dedicated worktree plus one local private branch.
- Keep feature branches private until they are merged into `main`.
- Do not introduce or rely on `go.work` or `go.work.sum` in this repository, sibling repositories, or their shared parent directory.
- Do not wire local sibling repositories into manifests, lockfiles, imports, or build/test configuration.
- Default sync strategy for a feature branch: `git rebase origin/main`.
- Do not merge `origin/main` into a feature branch in the normal flow.
- Preserve intentional commit history when integrating:
  - use `git merge --ff-only "$BR"` on `main` once the feature branch history is ready;
  - if the feature branch history is too noisy, clean it inside the feature branch before integration instead of hiding it behind `--squash`.
- Resolve conflicts only inside the feature worktree, never on `main`.
- Do not merge feature branches into each other.
- Conflict resolution must preserve the semantic intent of all involved branches, not just produce text that compiles.
- Before resolving merge or rebase conflicts, review the substantive commits on each side for new features, bug fixes, behavior changes, tests, and user-facing workflows.
- Do not drop, overwrite, or silently weaken current or historical functionality unless the user explicitly approves that product decision.
- If two branches introduce incompatible behavior, surface the product or architecture tradeoff instead of choosing one side silently.
- After resolving conflicts, run focused checks for the affected behavior in addition to the repository quality gate.
- If a feature branch has already been pushed and someone depends on it, switch to a conservative coordination flow instead of freely rewriting history.

Recommended setup after the first commit exists:

```bash
git fetch origin
git switch main
git pull --ff-only

BR=codex/<topic>
WT=../floret-<topic>
git worktree add -b "$BR" "$WT" origin/main
```

### Feature Sync

Inside the feature worktree:

```bash
git status
# The worktree must be clean before rebasing.

git fetch origin
git rebase origin/main
```

If conflicts happen:

```bash
git add <resolved-files>
git rebase --continue
```

If you are unsure:

```bash
git rebase --abort
```

After every rebase:

```bash
git diff origin/main...HEAD
```

Then rerun the relevant local quality gate from this file.

### Integration Back To Main

Once the feature branch is ready:

```bash
git switch main
git fetch origin
git pull --ff-only

# If local main is already ahead of origin/main, publish the full local main tip first.
# Do not keep older local main commits unpublished while only pushing the new feature result.
# git push origin main

git merge --ff-only "$BR"
git push origin main
```

Cleanup:

```bash
git worktree remove "$WT"
git branch -d "$BR"
```

If the feature branch was pushed:

```bash
git push origin --delete "$BR"
```

Additional rules:

- Remote `main` should always move directly to the latest local `main` tip whenever `main` is pushed.
- Do not discard, collapse, or silently rewrite meaningful feature commits during integration.
- If a conflict happens on `main`, abort and go back to the feature worktree.
- Do not create safety or backup branches for routine rebases. If extra safety is needed, ask the user before creating one.
- Recommended Git configuration:

```bash
git config --global rerere.enabled true
git config --global merge.conflictstyle zdiff3
```

### Conflict Resolution Principles

- Resolve conflicts only in the feature worktree.
- During `git rebase origin/main`, do not use `--ours` and `--theirs` blindly:
  - `--ours` usually means the rebasing target (`origin/main`);
  - `--theirs` usually means the replayed feature commit.
- Start from the latest `main` structure and then re-apply the real feature intent on top of it.
- For renames, file moves, formatting changes, or import reshuffles:
  - keep the latest `main` layout first;
  - then restore the feature logic in the new location.
- For generated files, snapshots, and lockfiles:
  - prefer regeneration over manual conflict stitching.
- For shared contracts, schemas, and cross-package payload fields:
  - align semantics manually instead of blindly taking one side.
- For behavior conflicts that are not obvious from conflict markers, inspect the relevant commit history and tests so that fixes and existing behavior are not regressed.
- If you are not confident about the resolution, abort the rebase and reassess.

## Engineering Principles

- Floret is a reusable interactive AI agent engine, not a graph workflow or multi-agent orchestration framework.
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
