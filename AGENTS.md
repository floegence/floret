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

### Commit Messages

Use Conventional Commit style for every commit:

```text
<type>(<scope>): <summary>
```

Rules:

- Use a lowercase type. Prefer `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `build`, or `ci`.
- Always include a concise lowercase scope that names the affected area, for example `runtime`, `provider`, `tools`, `session`, `storage`, `docs`, or `repo`.
- Keep the summary in imperative mood, start it lowercase, and omit a trailing period.
- Use English for commit messages.
- Examples:
  - `feat(runtime): add streaming event contracts`
  - `fix(provider): disable reasoning for title summaries`
  - `docs(repo): document commit message format`

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

## Host Integration Boundary

Floret is the reusable agent engine. It owns provider loop execution, tool
dispatch, permission/resource/approval lifecycle, runtime observation, core
control signal contracts, and opaque model state lifecycle.

Floret must not import or encode downstream product concerns such as UI blocks,
timeline reducers, Desktop or Env App adapters, durable product thread stores,
session grants, filesystem scope policy, target routing, approval UI, provider
credentials, provider profile persistence, or product-specific modes.

Downstream hosts must integrate through Floret public packages such as
`config`, `runtime`, `tools`, and `observation`; they must not depend on Floret
`internal/*`. New host-facing boundary capabilities must be exposed as general
public API with tests and documentation.

General capabilities may move into Floret only when they are product-neutral
agent-engine contracts. Product policy and UI semantics stay in the host. If a
future design intentionally changes this boundary, update the relevant
repository rules, public API documentation, tests, and release notes before a
downstream host consumes the change.

## Concept Vocabulary and Identity Rules

Floret concepts are intentionally small and strict. Use these names exactly; do not
invent near-synonyms when a concept below already fits.

### Runtime Actors

- `Agent` is the configured assistant persona plus capabilities exposed to a run or
  thread. It is not a process, provider client, or durable conversation.
- `Engine` is the lower-level single-run executor. It owns provider loop control,
  tool invocation, compaction decisions, prompt-cache requests, and event emission.
- `AgentHarness` is the durable host-facing conversation layer. It owns threads,
  turn lifecycle, retries, forks, titles, and the projection of a thread path into
  one engine execution.
- `TestingHarness` means deterministic scripted providers and test helpers. It must
  stay outside production control flow.

### Execution Identity

- `ThreadID` identifies a durable conversation journal. It is the identity used by
  `agentharness`, `sessiontree`, and host UIs that persist conversation state.
- `TurnID` identifies one user-facing turn in a thread. A turn has lifecycle state
  such as idle, running, waiting, completed, failed, interrupted, or cancelled.
- `RunID` identifies one engine/provider execution. A standalone engine run uses a
  `RunID` without a durable `ThreadID`; a normal harness turn usually sets
  `RunID == TurnID`, but code must never rely on that equality.
- `PromptScopeID` identifies the reuse boundary for prompt-cache segments, toolset
  snapshots, provider request ledgers, and provider response ledgers. Durable threads
  normally use `PromptScopeID == ThreadID`; standalone runs use `PromptScopeID == RunID`.
- `TraceID` correlates events across a logical execution trace. It is for observation,
  not storage ownership.
- `LogicalRequestID` may identify a user-visible request spanning retries or transport
  attempts. It must not replace `RunID`, `TurnID`, or `TraceID`.
- `SessionID` and `session_id` are not core execution identities. They are allowed
  only inside host/test UI API and view-state types where the product resource is an
  "agent session"; code crossing into engine, provider, cache, observation, storage,
  or tools must convert that value to `ThreadID` and/or `PromptScopeID`.

### Conversation Storage

- `Entry` is one immutable journal entry in a `sessiontree.Repo`.
- `Journal` is the ordered set of entries for a thread.
- `Path` is the active branch projection from root to leaf.
- `Leaf` is the current active entry pointer for a thread.
- `Fork` creates a new thread or branch from an existing entry.
- `TranscriptMessage` is the provider-visible message projection. It is not the same
  thing as a journal entry, tool event, or UI display row.
- `TranscriptStore` stores engine-level transcript messages for isolated runs. It
  must not pretend to be durable thread storage.

### Tools, Permissions, and Effects

- `ToolDefinition` is the provider-visible local tool schema.
- `HostedToolDefinition` is a provider-native capability and must not be dispatched
  by the local tool runtime.
- `ToolInvocation` is one validated local tool execution.
- `ToolResult` is the structured outcome returned to the provider and observation
  layers.
- `Permission`, `Approval`, `Effect`, and `ResourceRef` describe whether a tool may
  run, who approved it, what side effects it can cause, and which files, commands,
  URLs, or other resources it touches.
- `ControlSignal` is engine-owned loop control such as completion or user input. It
  is not a normal host command and must remain observable.

### Messages, Context, and Prompt Cache

- User, assistant, system, tool-call, tool-result, compaction-summary, and control
  messages are distinct categories. Do not encode these distinctions only in free
  text.
- `ContextPolicy` is configuration. `Usage` is message-context budget state.
  `RequestEstimate` is provider-request size estimation. `ContextPressure` is the
  decision signal derived from native usage, estimates, or provider overflow.
- Prompt-cache rows and JSON must use `prompt_scope_id` / `PromptScopeID`, not
  `run_id` as the reuse boundary. Segment authorship may record
  `CreatedByRunID` and `CreatedByTurnID`, but those fields do not define reuse.
- Provider raw plans are provider-specific rendered fragments. If raw fragments are
  missing, malformed, or for the wrong adapter, fail explicitly instead of rebuilding
  them from higher-level request data.

### Events, Observation, Artifacts, and Storage

- `Event` is the engine/harness event stream. Important decisions, policy boundaries,
  retries, compactions, approvals, provider requests, and tool outcomes must be
  observable through events or testable state.
- `Observation` is a sanitized host/test projection of events, context, prompt-cache
  records, and session-tree entries.
- `Artifact` is durable tool or run output. Artifact ownership must use explicit
  `ThreadID`, `TurnID`, `RunID`, and `TraceID` fields where applicable.
- `internal/storage` defines storage contracts. `internal/storage/sqlite` is one
  implementation. Public storage creation goes through `runtime.Store`. Storage APIs must name the domain object they delete or load,
  such as thread data, prompt scopes, metadata, or transcripts.

### Profiles and Capabilities

- `ProviderProfile` is the host/test UI provider configuration.
- `AgentProfile` is persona metadata plus the base system prompt.
- `PromptIdentity` is the hashable identity of the resolved prompt source.
- `Capability` describes what the host/provider can expose. Skills and MCP servers
  are capability sources, not core engine identities.

### Visibility and Go Naming

- Export a type, method, or field only when it is part of a package contract.
  Package-local helper concepts stay unexported.
- Cross-package data structures must carry explicit identity fields; they must not
  infer `ThreadID`, `TurnID`, `RunID`, or `PromptScopeID` from each other.
- Do not use vague names such as `session`, `context`, `request`, `state`, or `id`
  for package contracts when the narrower concept is known. Prefer names such as
  `ThreadID`, `TurnID`, `PromptScopeID`, `ProviderRequest`, `ContextPressure`, or
  `TranscriptStore`.
- Project code must not keep unsupported contract parsing, transitional shape paths, silent substitutes, or comments explaining removed shapes. Floret has not launched; when a shape is wrong, reject it or delete it completely.

## IMPORTANT Design Constraints

- `IMPORTANT:` comments mark product, security, or interaction invariants that must stay rare, intentional, and backed by code or tests where practical.
- If a change would remove, bypass, weaken, or contradict an `IMPORTANT:` comment, discuss the design impact with the user and receive explicit confirmation before implementing that change.
- Do not work around an `IMPORTANT:` constraint with hidden substitute behavior, alternate entry points, or silent unsupported-contract paths.
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
