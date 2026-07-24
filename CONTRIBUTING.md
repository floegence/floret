# Contributing

Start with [AGENTS.md](AGENTS.md). It is the authoritative guide for repository
boundaries, terminology, language, worktrees, branches, commits, integration,
and conflict resolution.

Develop changes in the dedicated worktree and feature branch required by the
[Git workflow](AGENTS.md#git-workflow). Keep each change focused and add tests
that exercise the affected behavior.

Before review, run the repository [quality gate](AGENTS.md#quality-gate):

```bash
go test ./...
```

Run additional focused checks when the affected area requires them. Changes to
important architecture, public contracts, execution identity, durable design,
or maintainer workflow must also evaluate and update the
[OKF project knowledge bundle](okf/index.md) as described in
[AGENTS.md](AGENTS.md#okf-project-knowledge-bundle).
