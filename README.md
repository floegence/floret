# Floret

Floret is a Go runtime for building interactive, tool-using AI agents. It owns the agent loop, provider adapters, durable thread runtime, context pressure, compaction, tool dispatch, runtime storage, and sanitized observation. A downstream application owns product UI, users, workspaces, permission policy, secrets, and domain tool implementations.

Floret is not a graph workflow framework and not a multi-agent orchestration framework. The intended integration path is a small host facade: configure a host, register tools, start a thread, run turns, render snapshots and observations.

## Stable Downstream API

Production downstream projects should import only these packages:

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

Everything under `internal/` is Floret implementation. Downstream projects should not construct provider requests, call model SDKs for an agent turn, manage Floret journal tables, or parse prompt-cache/provider-ledger records directly.

## Responsibilities

| Area | Floret owns | Host application owns |
| --- | --- | --- |
| Agent execution | provider loop, tool continuation, loop limits, finish reasons | choosing when a user can start or retry work |
| Provider access | provider adapters, request shape, stream parsing, usage, continuation state | user-level provider profile, secret source, allowed model policy |
| Storage | thread journal, prompt material, provider ledger, artifacts, runtime metadata | product metadata keyed by `runtime.ThreadID` |
| Tools | schema validation, generic effects, approval hook, dispatch, result projection | domain handlers and final product permission checks |
| UI | sanitized events, snapshots, observation DTOs | layout, workflows, interaction states, recovery actions |

## Quick Start

```bash
go get github.com/floegence/floret
```

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
		Store:  store,
		Tools:  registry,
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

Use `runtime.OpenSQLiteStore(path)` when the host wants Floret-managed durable runtime storage. Treat `runtime.Store` as an opaque handle. Product data such as owners, workspaces, pinned state, and read watermarks belongs in the host database.

## Configuration

`config.Load` reads `.env.local` and environment variables. A host may also build `config.Config` directly.

```bash
FLORET_PROVIDER=fake
FLORET_MODEL=fake-model
FLORET_FAKE_RESPONSE=ok
FLORET_CONTEXT_WINDOW_TOKENS=256000
FLORET_RESERVED_OUTPUT_TOKENS=64000
FLORET_RECENT_TAIL_TOKENS=12000
```

For a custom OpenAI-compatible gateway:

```bash
FLORET_PROVIDER=openai-compatible
FLORET_MODEL=your-model
FLORET_BASE_URL=https://api.example.com/v1
FLORET_API_KEY=your-api-key
```

Provider secrets should be resolved by the host configuration path and passed to Floret configuration. Floret events, snapshots, and observation DTOs must not be used as secret stores.

## Tools

Hosts register domain tools with `tools.Registry`. Floret validates JSON arguments, extracts generic resource/effect information, asks the configured approver when required, dispatches the handler, shapes output, and records runtime facts. Tool handlers still enforce product-specific permissions such as user, tenant, workspace, environment, and target ownership.

Important tool rules:

- Read-only tools may run in parallel only when `ParallelSafe` is explicitly valid.
- Mutating, shell, network, destructive, or open-world tools must declare permission behavior.
- Provider-native hosted capabilities are not local tools and are not dispatched by `tools.Registry`.
- Tool outputs are projected before being shown to a model or UI; full outputs should be represented by artifact references when needed.

## Observation

Use `runtime.EventSink` to receive sanitized runtime events from a host. Use `observation` DTOs for context pressure and compaction state when building UI surfaces. Observation records are not raw provider payloads and should not contain prompt text, tool arguments, tool results, local paths, or secrets.

## Runtime Flow

```text
Host UI/API
  |
  | StartThread / RunTurn / RetryTurn / DeleteThread
  v
runtime.Host
  |
  | owns provider loop, journal projection, tool dispatch, context pressure
  v
Floret internal runtime
  |
  +--> tools.Registry for local domain tools
  +--> runtime.Store for Floret-owned runtime data
  +--> runtime.EventSink and observation DTOs for host rendering
```

A normal hosted conversation uses `runtime.ThreadID` as the durable journal identity. `runtime.TurnID` identifies one user-visible turn. `runtime.RunID` identifies one provider execution. `runtime.PromptScopeID` is the prompt cache and provider-ledger reuse boundary. Code must not rely on those identities being equal.

## Local Test Console

Floret includes a local test console for contributor inspection:

```bash
go run ./cmd/floret-test-ui
```

The console can run fake-provider sessions, inspect sanitized events, run provider smoke checks, and exercise tool scenarios. It is not the downstream integration contract.

## Quality Gate

```bash
go test ./...
```

The test suite covers host facade behavior, provider stream contracts, tool validation and permissions, context pressure, compaction, prompt-scope ownership, storage cleanup, and architecture boundaries that keep Floret internals out of downstream APIs.

## License

Floret is licensed under the [MIT License](LICENSE).
