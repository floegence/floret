# Floret

<p align="center">
  <strong>A Go runtime for interactive, tool-using AI agents.</strong><br />
  <sub>Floret owns the agent loop, durable thread runtime, context pressure, tool dispatch, neutral activity, and sanitized observation. Your product owns the UI, users, permissions, secrets, and domain tools.</sub>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="License" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go Version" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

<p align="center">
  <img alt="Runtime host" src="https://img.shields.io/badge/Runtime-Host-0f766e?style=for-the-badge" />
  <img alt="Bring your UI" src="https://img.shields.io/badge/Bring-Your%20UI-1d4ed8?style=for-the-badge" />
  <img alt="Fake providers" src="https://img.shields.io/badge/Fake-Providers-7c2d12?style=for-the-badge" />
</p>

<p align="center">
  <a href="#-why-floret">Why Floret</a> ·
  <a href="#-at-a-glance">At a glance</a> ·
  <a href="#-quick-start">Quick Start</a> ·
  <a href="#-projected-turns">Projected Turns</a> ·
  <a href="#-responsibility-boundary">Boundaries</a> ·
  <a href="#-runtime-flow">Runtime Flow</a> ·
  <a href="#-quality-gate">Quality Gate</a> ·
  <a href="#-license">License</a>
</p>

Floret is a reusable Go runtime for applications that need interactive agent
conversations without rebuilding the same provider loop, durable thread state,
tool execution, context management, compaction, and event projection in every
host.

![Floret AI agent app runtime](okf/assets/readme/floret-agent-app-whiteboard.png)

Your product remains the place where users, permissions, approval UX,
credentials, billing, and domain data live. Floret sits behind that product UI
as the runtime layer: it starts threads, runs turns, loops through model and tool
calls, records runtime facts, and emits host-safe events for rendering. The host
connects Floret to model transport, product tools, storage, and observability
through the public `config`, `runtime`, `tools`, and `observation` packages.

Floret is not a graph workflow framework and not a multi-agent orchestration
framework. The intended integration path is compact: configure an agent,
register domain tools, start a thread, run turns, and render snapshots or
observations.

## ✨ Why Floret

Most agent products end up with the same hard plumbing: provider request shaping,
stream parsing, tool-call validation, approval hooks, durable conversation state,
long-context pressure, retries, usage metrics, and UI-friendly runtime events.
Floret packages those concerns behind a compact public API so product code can
stay focused on product behavior.

- **Agent loop**: continue after tool calls, enforce loop limits, track finish
  reasons, and return clear turn results.
- **Durable threads**: start, read, retry, delete, and manage parent-owned child
  threads through `runtime.Host`.
- **Tools**: register strict schemas with `tools.Registry`, declare effects, ask
  for approval, and dispatch domain handlers.
- **Dynamic tool surfaces**: refresh exposed tools, hosted tools, host context,
  and prompt instructions at provider-loop safe points without encoding product
  policy in Floret.
- **Storage**: choose `runtime.NewMemoryStore` for tests or
  `runtime.OpenSQLiteStore` for Floret-managed durable runtime storage.
- **Observation**: stream sanitized `runtime.EventSink` records, project neutral
  activity timelines, and use `observation` DTOs for context and compaction UI.
- **Deterministic tests**: use the fake provider path to test host flows without
  real model calls.

## 🧭 At a glance

| You need to... | Use... |
| --- | --- |
| Configure a provider and agent persona | `config.Config` or `config.Load` |
| Build a durable conversation host | `runtime.NewHost` |
| Run provider-free thread maintenance operations | `runtime.NewThreadMaintenanceHost` |
| Manage child threads under a hosted conversation | `runtime.Host` subagent methods |
| Run turns and request context compaction | `runtime.Host` and `runtime.CompactThreadRequest` |
| Supply product-owned model transport | `runtime.ModelGateway` |
| Refresh tool exposure during a run | `runtime.ToolSurfaceProvider` |
| Keep Floret runtime data in memory | `runtime.NewMemoryStore` |
| Keep Floret runtime data in SQLite | `runtime.OpenSQLiteStore` |
| Expose product-specific actions | `tools.Registry` and typed tool handlers |
| Render progress and diagnostics | `runtime.EventSink` plus `observation` DTOs |

## 📦 Stable downstream API

Production downstream projects should import only these packages:

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

Everything under `internal/` is Floret implementation. Downstream applications
should not bypass the `runtime` facade to build turn requests, call Floret
implementation contracts, manage Floret journal tables, or parse prompt-cache
and provider-ledger records. If the product owns model transport, implement
`runtime.ModelGateway` through `runtime.HostOptions.ModelGateway` and let
Floret construct provider requests, own context lifecycle, dispatch tools, and
record runtime facts. Product data such as owners, workspaces, pinned state,
read watermarks, and billing metadata belongs in the host database keyed by
`runtime.ThreadID`. Any package outside the stable list above is contributor or
runtime implementation, not a downstream contract.

## 🚀 Quick Start

Install Floret:

```bash
go get github.com/floegence/floret/config github.com/floegence/floret/runtime github.com/floegence/floret/tools github.com/floegence/floret/observation
```

Create a host with the fake provider:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

type echoArgs struct {
	Text string `json:"text"`
}

func main() {
	ctx := context.Background()

	cfg := config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		FakeResponse: "hello from floret",
		AgentProfile: config.AgentProfile{
			ID:           "example-agent",
			Name:         "Example Agent",
			SystemPrompt: "You are a concise example assistant.",
		},
	}

	registry := tools.NewRegistry()
	err := registry.Register(tools.Define[echoArgs](
		tools.Definition{
			Name:        "echo",
			Title:       "Echo",
			Description: "Return the supplied text.",
			InputSchema: tools.StrictObject(map[string]any{
				"text": tools.String("Text to echo."),
			}, []string{"text"}),
			ReadOnly: true,
		},
		nil,
		nil,
		func(ctx context.Context, inv tools.Invocation[echoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text}, nil
		},
	))
	if err != nil {
		log.Fatal(err)
	}

	store := runtime.NewMemoryStore()
	host, err := runtime.NewHost(runtime.HostOptions{
		Config: cfg,
		// Set ModelGateway when the host owns model transport.
		Store: store,
		Tools: registry,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	thread, err := host.StartThread(ctx, runtime.StartThreadRequest{ThreadID: "thread-1"})
	if err != nil {
		log.Fatal(err)
	}

	result, err := host.RunTurn(ctx, runtime.RunTurnRequest{
		RunID:    "run-1",
		ThreadID: thread.ID,
		TurnID:   "turn-1",
		Input:    "Say hello in one short sentence.",
		Limits: runtime.TurnLimits{
			MaxInputTokens: 100_000,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Output)
}
```

`TurnResult` and the returned error are independent parts of the host contract.
If a turn reaches a terminal engine state but Floret cannot read the durable
detail needed for its final display projection, `RunTurn` preserves the terminal
status, output, metrics, provider state, and control signal while returning an
error that matches `runtime.ErrTurnProjectionUnavailable`. Hosts should inspect
both values and may retry `ReadTurnProjection` with the explicit thread, turn,
and run identities.

`TurnLimits.MaxInputTokens` caps cumulative provider input usage across all
model requests in one run. `MaxTotalTokens` remains the cumulative input plus
output limit. Provider/context `MaxOutputTokens` remains a per-request output
bound and is not converted into either cumulative limit.

Use `runtime.OpenSQLiteStore(path)` when the host wants Floret-managed durable
runtime storage. Treat `runtime.Store` as an opaque handle; do not reach into its
tables or implementation details from downstream code.

Use `EnsureThread` when a host needs to initialize or recover a thread by
identity without reading its transcript. It returns `ThreadSummary`, the
thread's lifecycle and metadata view without `messages`; reserve `ReadThread`
for compatibility or explicitly transcript-oriented tools.

Use `NewThreadMaintenanceHost` when a process only needs provider-free thread
maintenance operations such as ensuring a thread summary, reading a turn
projection, settling host-owned pending tool work, reading parent-scoped
subagent lists, activity timelines, and detail events, closing child threads, or
deleting a Floret-owned thread tree. It accepts the same opaque `runtime.Store`
handle as `NewHost` and requires that store explicitly, but accepts no provider,
model, fake response, gateway, tools, or product UI configuration. Hosts should
choose this constructor for cleanup, reload, settlement, detail, and maintenance
paths that must not be coupled to provider loop configuration. After a host
process restart, `ThreadMaintenanceHost.ListSubAgents` is the canonical public
source for a parent thread's child-thread list; downstream UIs should use it
instead of reading Floret storage tables or reconstructing child identity from
transcript display rows.

`DeleteThread` resolves the target and all Floret-managed descendants before it
issues one store deletion. The SQLite store removes journals, active leases,
metadata, artifacts, prompt scopes, and provider ledgers in one immediate
transaction, so any deletion failure rolls back the complete tree.

## 🌿 Parent-managed child threads

`runtime.Host` can manage product-neutral subagents as durable child threads.
Use `SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`, and
`CloseSubAgent` when a parent conversation needs Floret-owned child lifecycle
management. Each child has its own `ThreadID`, turn lifecycle, prompt-cache
scope, provider ledger, and journal. The parent relationship is durable
metadata. `TaskName` and `TaskDescription` are neutral delegated-work metadata:
the name identifies the child in compact lists, while the description records
the parent-authored responsibility or objective for that child. Product policy,
UI labels, permissions, routing actions, and orchestration prompts remain
host-owned.

When `HostOptions.ModelGateway` is set, parent turns, child turns, and internal
hosted model work such as title generation use the same host-owned model
transport. Child turns still carry their own child `ThreadID` and prompt scope.

Lifecycle operations target child `ThreadID`s. Task names and agent paths are
display/reference metadata and may repeat under the same parent. Inputs queued
for a child are stored in that child's journal, so waiting, closing, and
restarted hosts observe the same pending work instead of process-local state.
`WaitSubAgents` returns when every target is settled for parent control:
completed, waiting for input, failed, cancelled, interrupted, or closed, with no
queued child input remaining.

Subagents are not a graph workflow framework and are not host-owned pending tool
work. Pending tool results still represent work whose lifecycle belongs to the
host application. Child threads represent Floret-owned durable conversations
that a parent thread can create, steer, wait for, list, and close.

## 🔀 Hosted Context Lifecycle

Use `runtime.Host` for durable turns, child threads, active manual compaction,
and idle compaction maintenance. Hosts send product input and control signals
through the facade; Floret owns provider-visible context assembly, trimming,
summary generation, checkpoint installation, continuation state, and lifecycle
observations.

`runtime.RunTurnRequest.SupplementalContext` lets a host attach structured
context items that should be visible to the model for the current user turn
only. Floret renders those items into the provider request without changing
`Input`, durable thread history, working directory, permissions, tool approval
state, or opaque provider continuation state. Hosts remain responsible for
deciding what data is safe to include and for truncating or redacting product
content before passing it to Floret.

Manual compaction is requested through `runtime.RunTurnRequest.ManualCompactions`
for an active turn or `runtime.Host.CompactThread` with
`runtime.CompactThreadRequest` for an idle thread. `observation.ContextStatus`
reports usage and pressure. `observation.CompactionEvent` is the only
user-visible context compaction lifecycle event. Hosts may persist returned
provider state envelopes, but must treat them as opaque carry-through state
rather than transcript or summary data.

## ⚙️ Configuration

`config.Load` reads `.env.local` and environment variables. A host may also build
`config.Config` directly in code.

```bash
FLORET_PROVIDER=fake
FLORET_MODEL=fake-model
FLORET_FAKE_RESPONSE=ok
FLORET_CONTEXT_WINDOW_TOKENS=256000
FLORET_RESERVED_OUTPUT_TOKENS=64000
FLORET_RECENT_TAIL_TOKENS=12000
FLORET_REASONING_LEVEL=medium
FLORET_REASONING_BUDGET_TOKENS=0
```

For a custom OpenAI-compatible gateway:

```bash
FLORET_PROVIDER=openai-compatible
FLORET_MODEL=your-model
FLORET_BASE_URL=https://api.example.com/v1
FLORET_API_KEY=your-api-key
```

Provider secrets should be resolved by the host configuration path and passed to
Floret configuration. Events, snapshots, and observation DTOs must not be used as
secret stores.

## 🛠️ Tools

Hosts register domain tools with `tools.Registry`. Floret validates JSON
arguments, extracts generic resource and effect information, asks the configured
approver when required, dispatches the handler, shapes output, records runtime
facts, and maintains product-neutral activity state. Tool handlers still enforce
product-specific permissions such as user, tenant, workspace, environment, and
target ownership.

| Tool concern | Floret handles | Host handles |
| --- | --- | --- |
| Schema | strict provider-visible JSON shape | domain argument meaning |
| Permission | generic approval hook and effect metadata | product authorization policy |
| Execution | scheduling, panic recovery, result projection | the actual domain action |
| Output | model projection, neutral activity, and artifact references | product-specific display choices |

Important tool rules:

- Read-only tools may run in parallel only when `ParallelSafe` is explicitly
  valid.
- Mutating, shell, network, destructive, or open-world tools must declare
  permission behavior.
- Provider-native hosted capabilities are not local tools and are not dispatched
  by `tools.Registry`.
- Large outputs should be represented by artifact references when the model or UI
  does not need full inline content.
- When tool execution fails before a host handler owns the call, such as invalid
  arguments, permission denial, resource extraction failure, or panic recovery,
  Floret emits a neutral failed activity presentation with public error text.
  This lets host UIs show the user why the tool failed without reading Floret
  storage internals.
- Floret does not invent product copy, renderer-specific fields, or
  domain-specific detail rows. Hosts can attach their own
  `tools.ActivityPresentation` for successful tools and for product-owned
  handler failures; Floret preserves that presentation while keeping framework
  failures observable.

### Dynamic tool surfaces

Hosts that need run-time capability changes can set
`runtime.HostOptions.ToolSurfaceProvider` or
`runtime.RunTurnRequest.ToolSurfaceProvider`. Floret calls the provider before
model requests, before local tool dispatch, and before compact-only provider
request rebuilds. The returned `runtime.ToolSurface` may replace the active
`tools.Registry`, provider-visible tool definitions, hosted tool definitions,
system prompt, and host context for that safe point.

Floret treats this as a product-neutral engine surface. It does not interpret
policy names such as read-only, approval-required, or full-access modes. The
host owns those policies and projects the current state into a tool registry,
hosted tools, prompt text, and host context. If a model produced a tool call
against an older surface, Floret refreshes the surface before local dispatch, so
stale calls are checked against the latest registry and approval lifecycle.
`ToolSurface.Epoch` and `ToolSurface.Reason` are emitted as observation
metadata together with stable prompt and toolset hashes for audit and debugging.

Floret owns the durable provider-visible tool-call ledger. If a turn is
cancelled or failed after assistant tool calls have been accepted, Floret closes
each unresolved provider-visible call with a neutral terminal tool result before
writing the terminal turn marker. This keeps future provider requests valid
without asking downstream hosts to edit Floret storage or reconstruct provider
history from product audit logs. `ResumeThread` applies the same rule for
interrupted turns after a host restart; if an unsafe active branch was already
created from the unresolved call, Floret moves the active leaf back to the last
provider-safe ancestor and records the neutral closure on a new branch while
preserving the old branch as durable history. Active turn leases are part of the
same durable lifecycle invariant: a recovered failed, cancelled, or otherwise
terminal turn must not retain an active lease, and hosts must recover through
the runtime facade rather than clearing lease rows themselves.

### Host-owned pending tool results

Some tools start work whose lifecycle belongs to the host application, such as a
terminal process, watcher, or remote task. Those handlers can return
`tools.Result{Pending: ...}` after the host has started the work. Floret records a
normal provider-visible tool result containing `<pending_tool_result>`, marks the
live activity as running during the provider turn, and exposes pending metadata
for observation. A successful provider turn does not complete that host-owned
work; the activity remains running until the host reports the observed outcome.
Failed or cancelled turns settle unresolved pending work to an unavailable
terminal state. Floret does not own the process, poll the handle, store a task
registry, or decide cancellation.
`PendingToolResult.Handle` is the only continuation token rendered in that
provider-visible result. Metadata is observation-only; if the model should pass a
token to a later host tool call, the host must make that token the handle.

The host application is also responsible for run-scoped cleanup. When a host run
finishes, fails, is cancelled, or shuts down while it still owns pending work, the
host must inspect its own live work registry, observe or stop that work, and call
`runtime.Host.SettlePendingTool` with `completed`, `failed`, or `canceled` for
each affected pending tool. Floret intentionally does not poll handles, kill
processes, infer host task status from turn status, or decide which host-owned
work should continue beyond a run.

When the host observes completion, failure, or cancellation, it calls
`runtime.Host.SettlePendingTool` to update the original activity item without
adding provider-visible context. If the agent should reason over the completed
work, the host can call `runtime.Host.CompletePendingTool`; Floret then appends a
host-authored user follow-up turn containing `<pending_tool_completion>` and
runs the normal agent loop. Neither path creates a second `role=tool` message
for the original tool call; the initial pending result already satisfied that
provider tool-call pairing.

### Subagent detail inspection

Hosted subagents are parent-managed durable child threads. The parent uses
`SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`, and
`CloseSubAgent` for lifecycle control. `WaitSubAgents` is deliberately bounded:
it defaults to five minutes, caps requests at twenty minutes, and returns child
snapshots rather than a full child transcript.

Use `ListSubAgentActivityTimeline` when a host UI needs a parent-scoped activity
summary for all child threads. The returned `observation.ActivityTimeline` is
derived from Floret child snapshots and contains product-neutral child-thread
facts such as task name, task description, status, timing, and lifecycle state.
Activity item labels use the task name, and descriptions use the task
description; live progress, final handoff text, and product actions belong in
the host's detail surface rather than this parent summary.
Hosts may wrap those facts in their own display actions and routing; Floret does
not emit product UI actions, window targets, or downstream-specific presentation
copy. Payload fields such as `thread_id` and `subagent_id` are durable
child-thread identities, not UI actions or product routing policies.

Use `ReadSubAgentDetail` or `ListSubAgentDetailEvents` when a host UI needs to
inspect a child thread's persisted journal. Detail reads are scoped by both
parent and child `ThreadID`, are paginated by ordinal, and expose durable child
facts such as delegated input, messages, tool calls, tool results, approvals,
turn markers, compaction checkpoints, lifecycle stops, and run failures. Detail
events include bounded, sanitized previews plus hashes and truncation metadata
by default. Raw message content, reasoning, tool arguments, and full tool result
content are omitted unless `IncludeRaw` is set for an explicitly authorized
human/debug surface. Do not use raw subagent detail responses as model-facing
`wait` or `inspect` tool output.

Subagent detail responses also include a top-level `activity_timeline`. This is
the canonical current tool/activity projection for the retained child journal:
Floret rebuilds it from persisted child detail events on every read so later
tool results or `SettlePendingTool` calls replace earlier running updates.
Paginated event rows are the ordered journal facts; host UIs should render the
top-level activity timeline for current tool state instead of treating old row
activity snapshots as live state. Tool result rows still expose a structured
`status`, so hosts do not need to infer result state from preview text.

Subagent detail responses include a top-level `context` block for neutral,
model-bound facts: provider/model identity, model-derived context policy,
current context pressure/usage status, and public compaction lifecycle
operations. Context window size is a property of the resolved model capability
and policy, not of the parent thread, child thread, subagent, or fork mode.
Parent and child thread IDs scope lookup, ownership, and pagination only. Raw
provider requests, transcript windows, prompt-cache internals, and trimming
strategy fields remain internal to Floret. Downstream products decide how to
present model status lanes, compaction dividers, and other UI affordances.
Floret does not manage downstream terminal processes or remote tasks inside a
subagent. If a host-owned pending tool remains live when a subagent run
terminates, the host must observe, stop, and settle that work through
`SettlePendingTool`.

### Thread detail inspection

Use `ListThreadDetailEvents` when a host UI needs the Floret-owned ordered
execution transcript for a hosted thread. The API projects the thread journal in
entry ordinal order and covers user messages, assistant messages, tool calls,
tool results, turn markers, compaction checkpoints, approvals, custom entries,
and run failures. It is the public read model for durable execution facts; hosts
should use `ThreadTurnProjection` for product display caches instead of reading
Floret storage internals or reconstructing assistant/tool order themselves.

Thread detail events are paginated by ordinal and default to bounded,
sanitized previews plus hashes, truncation metadata, and artifact references.
Raw message content, reasoning, tool arguments, and full tool result content are
omitted unless `IncludeRaw` is set for an explicitly authorized human/debug
surface. The same row-level `activity_timeline` and tool result `status`
contract is available here for host UI rendering.

`RunTurn`, `RetryTurn`, and `CompletePendingTool` return a
`ThreadTurnProjection` for the completed turn. Hosts that already hold committed
thread events, such as a live UI reacting to `runtime.Event.Committed`, can call
`ProjectThreadTurn` with those events and the latest Floret-owned activity
timeline. The projection returns product-neutral ordered segments such as
assistant text, activity timelines, and control signals; host applications own
only the final UI block mapping. Do not call `observation.BuildActivityTimeline`
in host applications to build the main thread activity surface.

When a host changes product-neutral activity items while reconciling a timeline,
use `observation.RebuildActivitySummary` to recompute counts, approval totals,
status, severity, and attention reasons. It preserves the existing duration and
settled run-level error or canceled status when no active item remains.

Use `ReadTurnProjection` when a host needs to reload the current display
projection for a known thread turn from durable Floret detail. The request
requires `ThreadID`, `TurnID`, and the concrete `RunID`; Floret does not infer
execution identity from storage rows or substitute an empty projection for a
missing turn. A missing thread returns `ErrThreadNotFound`, and a missing turn
returns `ErrTurnNotFound`. If the supplied run id is not recorded for that
turn, Floret returns `ErrRunNotFound`. Hosts can handle reload errors with
`errors.Is` without reading Floret storage internals.

Use `ForkThread` when a host needs to create a new durable thread from an
existing thread path. Floret owns the ledger copy, rewrites destination
`TurnID`/`RunID` execution identities, and returns the public identity mapping
that hosts need for later `ReadTurnProjection` calls. Hosts should store those
identity references alongside their product thread metadata, not clone Floret
rows or materialize Floret display projections into a separate transcript
store. Provider-free `ThreadMaintenanceHost` exposes the same fork contract for
restart and UI maintenance paths.

### Pending approval snapshots

Use `ListPendingApprovals` when a host UI needs the current tool approvals that
are waiting for a decision. The snapshot is product-neutral: it exposes the
approval id, tool call id, tool name, generic effects, resources, labels, host
context, state, and timing metadata from Floret's approval lifecycle. Hosts own
the product permission policy, user-facing summary, approval controls, and any
thread-list or composer presentation.

Pending approval snapshots are a current-state read model, not the durable audit
timeline. Continue to use thread detail events and observations when you need
ordered history.

## 🧱 Responsibility boundary

| Area | Floret owns | Host application owns |
| --- | --- | --- |
| Agent execution | provider loop, tool continuation, loop limits, finish reasons | choosing when a user can start, retry, or cancel work |
| Provider access | adapters, request shape, stream parsing, usage, continuation state | user-level provider profile, secret source, allowed model policy |
| Storage | thread journal, prompt material, provider ledger, artifacts, runtime metadata | product metadata keyed by `runtime.ThreadID` |
| Context lifecycle | model-bound context policy, pressure/usage observation, compaction lifecycle facts | visual meters, status lanes, divider placement, product copy |
| Tools | schema validation, generic effects, approval hook, dispatch, result projection | domain handlers and final product permission checks |
| Tool approvals | approval request state and current pending snapshots | user-facing approval UX, summaries, product mode policy, decision ownership |
| Pending tool work | pending result projection, terminal activity settlement, host settlement/completion APIs | handle ownership, process lifecycle, progress, cancellation, final artifacts |
| UI | sanitized events, snapshots, observation DTOs, thread-turn projection | layout, workflows, interaction states, recovery actions |

## 👁️ Observation

Use `runtime.EventSink` to receive sanitized runtime events from a host. Use
`observation` DTOs for context pressure and compaction state when building UI
surfaces. Observation records are not raw provider payloads and should not
contain prompt text, tool arguments, tool results, local paths, or secrets.

Activity timelines are also product-neutral. They expose lifecycle status,
timing, renderer identity, bounded public payload, and public error messages
when Floret itself rejects or recovers a tool call before the host handler can
shape a result. The host application owns the final presentation: labels,
localized copy, task-specific fields, controls, diagnostics policy, and any
detail lookup that belongs to product storage or product routing.

`observation.RebuildActivitySummary` is the public reducer for a timeline whose
items have been updated by a host. It shares Floret's internal summary rules, so
hosts do not need to duplicate status precedence, severity ranking, approval
counts, or attention-reason deduplication.

Compaction emits both lifecycle and diagnostic observations. Lifecycle events
(`runtime.Event.Compaction`) describe one user-visible operation as start,
complete, or failed. Diagnostic events (`runtime.Event.CompactionDebug`) expose
safe stage facts such as generation attempts, projected request rebuild,
context validation, installation, token pressure, counts, durations, the
post-install next action, and sanitized error text. They are intended for logs
and operator diagnostics, not transcript rendering, and never include prompt
text or generated summaries.

`runtime.StreamObservation` carries provider-neutral streaming facts for host
rendering. It includes assistant text deltas, reasoning deltas, model retry and
finish facts, and model tool-call stream facts. `ModelEventToolCallStart`,
`ModelEventToolCallDelta`, and `ModelEventToolCallEnd` identify the tool call
the model is generating without exposing argument text; the final executable
batch still arrives separately as `ModelEventToolCalls`.

When Floret commits a thread journal entry, `runtime.Event.Committed` carries
the corresponding `ThreadDetailEvent` after the entry is durable. Hosts can use
stream observations for temporary live token rendering, then reconcile durable
display order through `ProjectThreadTurn` or the `TurnResult.Projection` returned
by the host facade.

## 🔁 Runtime Flow

```text
Host UI/API
  |
  | StartThread / RunTurn / RetryTurn / CompletePendingTool / DeleteThread
  v
runtime.Host
  |
  | owns loop control, journal projection, tool dispatch, context pressure
  v
Floret runtime implementation
  |
  +--> tools.Registry for local domain tools
  +--> runtime.Store for Floret-owned runtime data
  +--> runtime.EventSink and observation DTOs for host rendering
```

A normal hosted conversation uses `runtime.ThreadID` as the durable journal
identity. `runtime.TurnID` identifies one user-visible turn. `runtime.RunID`
identifies one provider execution. `runtime.PromptScopeID` is the prompt-cache
and provider-ledger reuse boundary. `runtime.RunTurnRequest.RunID` is required;
hosts must pass the concrete execution identity instead of deriving it from a
turn or thread ID. Code must not rely on those identities being equal.

Execution identity is also distinct from host correlation identity. Downstream
hosts may carry product audit IDs, subagent IDs, child run IDs, UI routing IDs,
or permission snapshot IDs in labels or `HostContext`, but those values are not
Floret settlement authority. For example, a subagent host surface may see
`ToolSurfaceRequest.RunID=run-3` while `HostContext["child_run_id"]` is a
product audit run such as `run_child_audit`. `ReadTurnProjection` and
`SettlePendingTool` must use the Floret execution `RunID` (`run-3` in that
example), not the host correlation ID.

Bad settlement identity sources include host `child_run_id`, product audit run
IDs, subagent display IDs, tool stdout, elapsed time, process IDs, and
ActivityTimeline rows. `ActivityTimeline` is a display/lifecycle projection; it
is not an authority for reconstructing a pending settlement request unless a
future Floret public contract explicitly issues a settlement target there.

For host-owned pending work, use this lifecycle:

1. The tool returns a pending result and Floret records a running activity item.
2. The host stores the Floret execution identity (`ThreadID`, `TurnID`,
   `RunID`) together with the pending tool call ID, tool name, and handle in the
   host-owned work record.
3. The host-owned process finishes, fails, or is canceled.
4. The host calls `SettlePendingTool` with the stored Floret execution identity
   and pending tool target fields.
5. The host applies the returned canonical projection.
6. Only after successful settlement should the host mark its own audit record as
   acknowledged or complete.

## 🧪 Contributor Test Console

Floret includes a local test console for contributor inspection:

```bash
go run ./cmd/floret-test-ui
```

The console can run fake-provider sessions, inspect sanitized events, run
provider smoke checks, exercise tool scenarios, and manually operate hosted
subagents from the session workspace. It is not the downstream integration
contract.

## ✅ Quality Gate

```bash
go test ./...
```

The test suite covers host facade behavior, Floret-owned provider stream
contracts, tool validation and permissions, context pressure, compaction,
prompt-scope ownership, storage cleanup, and architecture boundaries that keep
Floret internals out of downstream APIs.

## 📄 License

Floret is licensed under the [MIT License](LICENSE).
