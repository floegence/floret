# Floret

Floret is a reusable Go engine for interactive AI agents. It owns the model
loop, durable conversation lifecycle, tool dispatch, approvals, context,
SubAgents, recovery, provider state, and observable execution facts. The host
application keeps product UI, routing, credentials, authorization policy, and
product data.

The v2 module path is:

```text
github.com/floegence/floret/v2
```

Floret is not a graph workflow framework, a multi-agent orchestrator, or a
product persistence layer.

## Install

```bash
go get github.com/floegence/floret/v2@v2.0.0
```

Do not use a local `replace`, `go.work`, or sibling repository path for a
production integration. v1 remains available only from its published Git tag.

## Quick Start

Every Agent uses one explicit `provider.Gateway`. There is no internal provider
fallback and no production fake-provider configuration.

```go
package main

import (
	"context"
	"fmt"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/florettest"
	"github.com/floegence/floret/v2/provider"
	"github.com/floegence/floret/v2/runtime"
	"github.com/floegence/floret/v2/storage"
)

func main() {
	ctx := context.Background()
	gateway := florettest.NewScriptedGateway(
		provider.Identity{
			Provider: "example", Model: "deterministic",
			StateCompatibilityKey: "example:deterministic:v1",
		},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "Hello from Floret v2."},
			{Type: provider.EventDone, Reason: "stop"},
		}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "assistant", Name: "Assistant"},
		SystemPrompt: "Answer clearly and concisely.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		panic(err)
	}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
	if err != nil {
		panic(err)
	}
	defer host.Close()

	creator, err := host.ThreadCreator("thread-1", "create-thread-1")
	if err != nil {
		panic(err)
	}
	if _, err := creator.Create(ctx); err != nil {
		panic(err)
	}
	runner, err := host.TurnRunner(ctx, "thread-1", agent)
	if err != nil {
		panic(err)
	}
	result, err := runner.Run(ctx, runtime.TurnRequest{
		RunID: "run-1", TurnID: "turn-1",
		Input: runtime.TurnInput{Text: "Hello"},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Output)
}
```

`florettest` is test-only. Production applications normally construct a
gateway with `provider.NewOpenAICompatible`, `provider.NewAnthropic`, or their
own `provider.Gateway` implementation.

## Public Packages

| Package | Responsibility |
| --- | --- |
| `config` | Provider-neutral Agent profile, prompt, context, and reasoning policy |
| `provider` | Gateway, request, message, event, state, identity, capability, and prepared-request contracts |
| `runtime` | Immutable Agent construction, durable Host lifecycle, and identity-bound handles |
| `storage` | Third-party Backend SPI plus official memory and SQLite sources |
| `tools` | Local tool definitions, permissions, resources, effects, and results |
| `observation` | Sanitized events and durable host-facing projections |
| `florettest` | Scripted gateways and public conformance suites for tests only |

Downstream code must not import `internal/*`.

## Composition Boundary

`runtime.Host` belongs only in the composition root. It owns one opened
`storage.Backend` and closes it after active runtime work has joined. The
composition root immediately issues the narrow handle required by each local
service:

| Handle | Bound authority |
| --- | --- |
| `ThreadCreator` | one `ThreadID` and create intent |
| `ThreadReader` | one root thread |
| `ThreadTitleEditor` | one root thread |
| `ThreadForker` | one source root thread |
| `ThreadDeleter` | one root thread tree |
| `TurnRunner` | one root thread and immutable Agent |
| `ThreadCompactor` | one root thread and immutable Agent |
| `SubAgentManager` | one parent thread and immutable Agent |
| `SubAgentReader` | one parent thread |
| `PendingToolRecovery` | one exact settlement target |
| `InterruptedTurnRecovery` | one exact interrupted turn target |

Requests do not repeat identities already bound by a handle. Services should
declare local minimal interfaces and must not retain `*runtime.Host` or recover
it through type assertions.

`ThreadReader` is the complete provider-free read surface for its bound root:
overview, exact and paged turns, detail events, approvals, todos, context,
projections, artifacts, and pending settlement targets. `TurnRunner` owns the
matching provider-backed retry, approval, todo, and active pending-work writes.
`SubAgentReader` and `SubAgentManager` expose the same child facts and effects
only through their bound parent authority.

## Agent Immutability

`runtime.NewAgent` requires a non-empty `config.AgentProfile`, system prompt,
context policy, and `provider.Gateway`. It snapshots configuration and static
tools. Options configure effect authorization, event observation, dynamic tool
surfaces, loop limits, title ownership, capabilities, and SubAgent timeout at
construction time; an Agent has no mutating API afterward.

Provider API keys, base URLs, provider names, and fake responses are not Agent
configuration. They belong to the gateway constructed by the host.

Runtime event sinks receive provider-neutral stream observations. The public
`runtime.ToolCallStream` identifies in-progress tool calls without exposing
arguments, so downstream hosts can render and test start/delta/end events
without depending on provider or internal runtime types.

## Storage

`storage.Source` and `storage.Backend` are the complete third-party persistence
SPI. A Backend exposes only snapshot `View`, serializable `Update`, namespaced
`Get` and bounded lexicographic `Scan`, `Put`, `Delete`, and `Close`. Floret owns
the versioned tuple keys, JSON envelopes, domain indexes, and authority rules.

Backend implementations must:

- invoke each `Update` callback exactly once and never retry it implicitly;
- roll back the complete write on callback error or panic;
- return caller-owned bytes;
- preserve snapshot reads and serializable writes;
- classify missing records, conflicts, closed backends, and expired
  transactions with the public storage errors.

Use the public conformance suite:

```go
func TestBackend(t *testing.T) {
	florettest.RunBackendContract(t, myBackendSource)
}
```

The official sources run the same domain kernel:

```go
runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
runtime.Open(ctx, runtime.Options{Storage: storage.SQLite("agent.db")})
```

SQLite v2 contains only backend metadata and opaque namespaced records. It does
not implement a second domain state machine.

## v1 Migration

Normal v2 startup accepts only an empty backend or the exact v2 logical schema.
An exact v1 schema-v16 SQLite store returns
`runtime.MigrationRequiredError`. No startup path migrates, repairs, or reads a
legacy shape.

Before migration, move every non-empty v1 `metadata_records` row to the host's
product store. Then run the explicit offline command:

```bash
go run github.com/floegence/floret/v2/cmd/floret-store@v2.0.0 \
  migrate-v2 --path /absolute/path/agent.db --operation-id deploy-2026-07-29
```

The migrator accepts only the exact schema-v16 fingerprint. It converts all
Floret-owned lifecycle state in one SQLite write transaction, validates counts,
authority graph, opaque provider state, and a content hash, then removes the v1
tables. Error, cancellation, or panic rolls back the transaction. Reusing the
same operation ID on the exact migrated content returns replay success; another
operation ID or corrupted content is rejected.

Versions v3-v15, unversioned, unknown, future, fingerprint-mismatched, and
corrupt databases are not guessed or repaired by v2.

## Source Of Truth

Floret's journal, canonical turn pages and projections, approval queue, Agent
todo state, artifacts, pending settlement records, SubAgent tree, provider
state, and prompt cache are the sole source of truth for admitted Agent
lifecycle. Hosts may persist product authorization, security audit, routing,
unadmitted commands, resources, and transport diagnostics, but must not persist
or rebuild a second queryable Agent lifecycle.

Canonical user references are opaque durable facts. Rich current-turn-only
host context belongs in `SupplementalContext` and never becomes conversation
history or provider continuation state.

## Development

```bash
GOWORK=off go test ./...
GOWORK=off go vet ./...
scripts/check_candidate_release_adoption.sh
```

Repository workflow and compatibility rules are defined in [AGENTS.md](AGENTS.md).
Architecture and maintenance knowledge lives in [okf/](okf/).
