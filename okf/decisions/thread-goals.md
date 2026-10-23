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
A mutation or review replay preserves its single committed fact; a stale version returns Conflict
and the current Goal without changing it. Text is retained exactly, with no
Goal-specific length ceiling or truncation. Prompts ask for clear, concise,
understandable desired results. Multi-stage work with verification normally uses
a Goal; direct answers and brief read-only checks normally do not. An existing
unfinished Goal remains in force across those checks. User and model mutations share the same journal
writer and increment the same version.

The model should finish necessary work tools, submit its review, then compose
the final deliverable with the requested result and essential supporting sources.
A premature answer before natural-stop reflection remains intermediate; review
instructions ask the model to preserve its useful substance in the final answer.
Goal status confirmation alone does not fulfill the user's deliverable.

User deletion suppresses automatic recreation for the same task. An answer,
approval, retry, restart or compaction is not a new task. The model can update
user-created Goals while preserving unfinished requirements. Only a genuinely
new ordinary user task can justify new_task after a deletion.

Goal updates append typed runtime facts at the next logical model request,
including subsequent requests in the same Turn. Stable instructions and tool
definitions remain unchanged. The existing prepared-request retry owner retains
its frozen snapshot and provider continuation. Canonical regeneration admits typed Goal facts after the exact retry source without accepting ordinary conversation suffixes. Compaction retains exact Goal
facts beside the summary; it never asks a summary model to reconstruct them.
Cache reuse depends on the provider and actual usage, not a promised percentage.

# Natural stop and recovery

Explicit user submission uses the optional ThreadGoalSubmitter. One journal
transaction writes the Goal and, only when no canonical Turn is active, accepts
the submitted user text as a normal Turn. Active execution, questions and
approvals receive only the Goal change. The existing actor installs the admitted
run with its canonical item prefix, including the Goal fact before the user input,
and the existing execution path prepares it. Later edits and deletion remain
immediately visible. The actor installs user mutations and their canonical view
in one serialized operation. Stable item IDs reconcile persisted facts with
uncommitted live text; only presentation ordinals are reassigned, preserving
canonical Path positions and rejecting missing or reordered durable identities. Products do not inspect state
and call Send afterward. SetGoal and DeleteGoal retain their save-only contract.

The Goal entry's existing request key and fingerprint deduplicate both outcomes.
An exact replay retains its original admission decision after completion, Stop
or restart; it cannot wake a thread that was only updated. A stale Goal version
changes nothing. Persistence failure rolls back both changes. Preparation or
provider failure uses the normal recoverable Turn failure while retaining the
saved Goal. A fresh explicit submission may start again. No new journal shape,
receipt, queue owner or domain schema is introduced.

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

Product authorization persists under the public GoalSubmissionRequestKey before
SubmitGoal. The factory receives that same ordinary execution identity; products
must not derive private hashes or duplicate admission decisions.

# Explicit compaction continuation

ContinueGoal on CompactContextInput is an additive public API decision. Omission
or false preserves the published pure-compaction contract. True authorizes one
idle-only admission that selects goal_continuation from an active canonical Goal,
otherwise context_compaction. The journal stores requested intent, effective kind
and fingerprint atomically; exact replay cannot reevaluate Goal or wake work.

One execution first performs required compaction or explicit noop, then enters the
existing Engine loop. Installation does not commit its terminal. Preparation
errors, bounded reflection exhaustion and provider errors settle recoverably with
the Goal retained. Questions, approvals, Stop, Retry and restart use existing
owners. Canonical preparation facts and confirmed tools prevent repeated work;
Unknown effects reject compaction-driven Goal continuation until a new explicit user input acknowledges the recovery boundary; inspection-and-continue remains available without automatic replay.
Goal context admission is the first business-request boundary. A deletion before
it prevents work; later deletion keeps the established review-only cancellation
semantics. Edits and stale-review protection retain the existing journal owner.

Schema 28 -> 29 preserves historical pure compaction and every migration edge.
Hosts authorize, pass intent and display canonical state. They never send a second
message after completion, reconnect or initialization. See
[behavioral coverage](../../runtime/thread_goal_compaction_test.go) and
[migration coverage](../../internal/sessiontree/backend_domain_v29_migration_test.go).

# Goal activity presentation

Structured activity optionally carries a detached GoalActivity: operation, version,
original objective, confirmed mutation flag and recorded review outcome/evidence.
It is display data, not lifecycle authority. A completed review never replaces the
atomic completed Goal fact. Live tools and history share one projection; historical
reads recover only the matching invocation and canonical review at its position,
without rewriting entries or borrowing later Goal text. Matching uses the
invocation span so fork entry re-keying preserves confirmed review evidence. Unknown or rejected
reviews never inherit requested outcomes. Text has no artificial preview limit;
observation redaction still applies. Existing structured renderers remain valid.

# Evidence

- Public opt-in and controls: ../../runtime/agent.go, ../../runtime/thread_goals.go
- Canonical mutations, review boundaries and terminal guard: ../../internal/sessiontree/goals.go
- Same-context tool and reflection: ../../internal/agentharness/goals.go
- Exact compacted facts: ../../internal/session/compaction/compaction.go
- Behavioral and migration coverage: ../../runtime/thread_goals_test.go,
  ../../runtime/thread_goal_submit_test.go,
  ../../internal/sessiontree/goals_test.go,
  ../../internal/sessiontree/backend_domain_v23_migration_test.go

User Goal changes may precede the first Turn. Public current and history omit absent execution identities; they never invent a run to render a control fact. Executable items retain their populated identities.

## Mutation before presentation hydration

User Goal mutations initialize the existing actor through its acknowledgment
read before canonical installation. Creating with `CreateAck`, then setting a
Goal without first reading a view, preserves ThreadID and the idle lifecycle.
The same rule covers deletion as the first operation after restart. Mutating a
Goal never relies on a host presentation read for actor initialization.
