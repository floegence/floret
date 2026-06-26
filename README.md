# Floret

<p align="center">
  <strong>A Go runtime for interactive, tool-using AI agents.</strong><br />
  <sub>Floret owns the agent loop, durable thread runtime, context pressure, tool dispatch, and sanitized observation. Your product owns the UI, users, permissions, secrets, and domain tools.</sub>
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
- **Storage**: choose `runtime.NewMemoryStore` for tests or
  `runtime.OpenSQLiteStore` for Floret-managed durable runtime storage.
- **Observation**: stream sanitized `runtime.EventSink` records and use
  `observation` DTOs for context and compaction UI.
- **Deterministic tests**: use the fake provider path to test host flows without
  real model calls.

## 🧭 At a glance

| You need to... | Use... |
| --- | --- |
| Configure a provider and agent persona | `config.Config` or `config.Load` |
| Build a durable conversation host | `runtime.NewHost` |
| Manage child threads under a hosted conversation | `runtime.Host` subagent methods |
| Run one turn from a host-owned transcript projection | `runtime.RunProjectedTurn` |
| Supply product-owned model transport | `runtime.ModelGateway` |
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
`runtime.ModelGateway` through either `runtime.HostOptions.ModelGateway` or
`runtime.ProjectedTurnOptions.ModelGateway` and let Floret construct the turn
request, own the loop, dispatch tools, and record runtime facts. Product data
such as owners, workspaces, pinned state, read watermarks, and billing metadata
belongs in the host database keyed by `runtime.ThreadID`. Any package outside
the stable list above is contributor or runtime implementation, not a downstream
contract.

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
		ThreadID: thread.ID,
		TurnID:   "turn-1",
		Input:    "Say hello in one short sentence.",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Output)
}
```

Use `runtime.OpenSQLiteStore(path)` when the host wants Floret-managed durable
runtime storage. Treat `runtime.Store` as an opaque handle; do not reach into its
tables or implementation details from downstream code.

## 🌿 Parent-managed child threads

`runtime.Host` can manage product-neutral subagents as durable child threads.
Use `SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`, and
`CloseSubAgent` when a parent conversation needs Floret-owned child lifecycle
management. Each child has its own `ThreadID`, turn lifecycle, prompt-cache
scope, provider ledger, and journal. The parent relationship is durable
metadata, while product policy, UI labels, permissions, and orchestration prompts
remain host-owned.

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

## 🔀 Projected Turns

Use `runtime.RunProjectedTurn` only when the product already owns conversation
rows and needs Floret to execute one turn from a host-built transcript
projection. Durable Floret-managed conversations should use `runtime.Host`.

Projected turn requests must carry explicit `RunID`, `ThreadID`, `TurnID`,
`TraceID`, and `PromptScopeID`. `History` accepts only `user`, `assistant`, and
`tool` messages; system instructions belong in `config.Config`. The returned
`Transcript` is the provider-visible projection for the next turn, not a
sanitized UI display row. When `ProjectedTurnOptions.ModelGateway` is set,
Floret passes a `runtime.ModelRequest` to the host-owned model transport and
continues to own tool dispatch, loop control, ledgers, and events.

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
approver when required, dispatches the handler, shapes output, and records
runtime facts. Tool handlers still enforce product-specific permissions such as
user, tenant, workspace, environment, and target ownership.

| Tool concern | Floret handles | Host handles |
| --- | --- | --- |
| Schema | strict provider-visible JSON shape | domain argument meaning |
| Permission | generic approval hook and effect metadata | product authorization policy |
| Execution | scheduling, panic recovery, result projection | the actual domain action |
| Output | model/UI projection and artifact references | product-specific display choices |

Important tool rules:

- Read-only tools may run in parallel only when `ParallelSafe` is explicitly
  valid.
- Mutating, shell, network, destructive, or open-world tools must declare
  permission behavior.
- Provider-native hosted capabilities are not local tools and are not dispatched
  by `tools.Registry`.
- Large outputs should be represented by artifact references when the model or UI
  does not need full inline content.

### Host-owned pending tool results

Some tools start work whose lifecycle belongs to the host application, such as a
terminal process, watcher, or remote task. Those handlers can return
`tools.Result{Pending: ...}` after the host has started the work. Floret records a
normal provider-visible tool result containing `<pending_tool_result>`, marks the
activity as running, and exposes pending metadata for observation. It does not
own the process, poll the handle, store a task registry, or decide cancellation.

When the host observes completion, failure, or cancellation, it calls
`runtime.Host.CompletePendingTool`. Floret appends a host-authored user follow-up
turn containing `<pending_tool_completion>` and runs the normal agent loop. The
completion is not a second `role=tool` message for the original tool call; the
initial pending result already satisfied that provider tool-call pairing.

### Subagent detail inspection

Hosted subagents are parent-managed durable child threads. The parent uses
`SpawnSubAgent`, `SendSubAgentInput`, `WaitSubAgents`, `ListSubAgents`, and
`CloseSubAgent` for lifecycle control. `WaitSubAgents` is deliberately bounded:
it defaults to five minutes, caps requests at twenty minutes, and returns child
snapshots rather than a full child transcript.

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

## 🧱 Responsibility boundary

| Area | Floret owns | Host application owns |
| --- | --- | --- |
| Agent execution | provider loop, tool continuation, loop limits, finish reasons | choosing when a user can start, retry, or cancel work |
| Provider access | adapters, request shape, stream parsing, usage, continuation state | user-level provider profile, secret source, allowed model policy |
| Storage | thread journal, prompt material, provider ledger, artifacts, runtime metadata | product metadata keyed by `runtime.ThreadID` |
| Tools | schema validation, generic effects, approval hook, dispatch, result projection | domain handlers and final product permission checks |
| Pending tool work | pending result projection, running activity, host completion turn | handle ownership, process lifecycle, progress, cancellation, final artifacts |
| UI | sanitized events, snapshots, observation DTOs | layout, workflows, interaction states, recovery actions |

## 👁️ Observation

Use `runtime.EventSink` to receive sanitized runtime events from a host. Use
`observation` DTOs for context pressure and compaction state when building UI
surfaces. Observation records are not raw provider payloads and should not
contain prompt text, tool arguments, tool results, local paths, or secrets.

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
and provider-ledger reuse boundary. Code must not rely on those identities being
equal.

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
