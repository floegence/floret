---
type: Decision
title: Canonical Thread Goals
description: One journal-owned objective and same-context natural-stop reflection.
tags: [runtime, goals, context, recovery]
---

# Summary

Floret owns one optional current Goal per thread. Enable model maintenance and
natural-stop reflection with runtime.WithAgentGoals. User management uses the
optional runtime.ThreadGoalController returned by Host.ThreadService. Neither
management operation starts execution or interrupts an in-flight request.
The canonical journal is the sole durable authority; products only authorize,
map and present it. Goals have no dependency on todos.

# Contract

SetGoal and DeleteGoal carry a stable request key and expected Goal version.
A replay preserves the committed operation; a stale version returns Conflict
and the current Goal without changing it. Text is retained exactly, with no
Goal-specific length ceiling or truncation. Prompts ask for clear, concise,
understandable desired results. User and model mutations share the same journal
writer and increment the same version.

User deletion suppresses automatic recreation for the same task. An answer,
approval, retry, restart or compaction is not a new task. The model can update
user-created Goals while preserving unfinished requirements. Only a genuinely
new ordinary user task can justify new_task after a deletion.

Goal updates append typed runtime facts at the next logical model request,
including subsequent requests in the same Turn. Stable instructions and tool
definitions remain unchanged. The existing prepared-request retry owner retains
its frozen snapshot and provider continuation. Compaction retains exact Goal
facts beside the summary; it never asks a summary model to reconstruct them.
Cache reuse depends on the provider and actual usage, not a promised percentage.

# Natural stop and recovery

The executing model uses the existing stop hook and continuation path. A typed
internal reflection asks it to compare the Goal, user constraints and existing
results, finish necessary work, ask for needed decisions, or honestly report an
external blocker. It should reuse existing evidence and perform only necessary
missing checks. This is not an independent reviewer or a second model session.

The runtime-owned goal tool supports set, delete and review. Set never marks
completion. Review supplies completed, continue or blocked with brief evidence,
bound to the Goal version, Turn, Run and latest work boundary. Pending work tools
must finish first. Later work, steering, interaction resolution or a Goal change
invalidates the review. A valid completed review becomes completed only in the
same atomic journal commit as successful Turn completion. Deleting an obsolete
Goal is not a replacement for reflection.

At most two forced continuations are admitted per Turn, including recovered
attempts; editing the Goal does not reset this count. Exhaustion is a recoverable
execution failure, retaining the Goal for explicit retry/continue or deletion.
Stop cancels execution and reflection while retaining the objective. Deletion
cancels later checks without stopping ongoing work. There is no background wake
or infinite execution loop. A last-moment Goal edit cannot produce stale success;
if it races the terminal commit the Turn fails recoverably.

Ordinary Fork copies the Goal at the selected path position and then evolves
independently. A delegated mission resets inherited Goal content so the child
maintains its own task. Domain schema 22 -> 23 preserves existing canonical
entries and every earlier migration; old writers reject the new envelope.

# Evidence

- Public opt-in and controls: ../../runtime/agent.go, ../../runtime/thread_goals.go
- Canonical mutations, review boundaries and terminal guard: ../../internal/sessiontree/goals.go
- Same-context tool and reflection: ../../internal/agentharness/goals.go
- Exact compacted facts: ../../internal/session/compaction/compaction.go
- Behavioral and migration coverage: ../../runtime/thread_goals_test.go,
  ../../internal/sessiontree/goals_test.go,
  ../../internal/sessiontree/backend_domain_v23_migration_test.go
