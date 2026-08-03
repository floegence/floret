# Floret

Floret is a reusable Go engine for interactive AI agents. It owns the model
loop and the complete admitted Agent lifecycle: canonical messages, threads,
turns, runs, tools, approvals, todos, artifacts, context, SubAgents, recovery,
provider state, prompt cache, and observable execution facts.

The host application owns product UI, credentials, provider profiles, resource
authorization, routing, read state, uploads before admission, and transport
diagnostics. It must not persist or rebuild a second queryable Agent lifecycle.

Floret is not a graph workflow framework, a multi-agent orchestrator, or a
product persistence layer.

## Install

```bash
go get github.com/floegence/floret/v3@v3.2.6
```

Production integrations must resolve the published module. Do not use a local
`replace`, `go.work`, or sibling repository path. v1 and v2 remain available
only from their published tags; v3 has no legacy facade or runtime decoder.

## Quick Start

Every Agent uses one explicit `provider.Gateway`. Floret allocates durable
thread, turn, and run identities; an application supplies only a stable
`identity.LogicalRequestID` for each logical mutation. This production example
uses the official OpenAI-compatible Gateway and SQLite:

```go
package main

import (
    "context"
    "os"

    "github.com/floegence/floret/v3/config"
    "github.com/floegence/floret/v3/provider"
    "github.com/floegence/floret/v3/runtime"
    "github.com/floegence/floret/v3/storage"
)

func main() {
    ctx := context.Background()
    gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
        Provider: "openai", Model: "gpt-4.1-mini",
        BaseURL: "https://api.openai.com/v1", APIKey: os.Getenv("OPENAI_API_KEY"),
        StateCompatibilityKey: "openai:gpt-4.1-mini:chat-completions:v1",
        Capabilities: provider.Capabilities{
            Reasoning: provider.ReasoningUnsupported,
            AttachmentPayload: provider.AttachmentDescriptors,
        },
    })
    if err != nil { panic(err) }
    agent, err := runtime.NewAgent(config.AgentConfig{
        Profile:      config.AgentProfile{ID: "assistant", Name: "Assistant"},
        SystemPrompt: "Answer clearly and concisely.",
        Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
    }, gateway)
    if err != nil {
        panic(err)
    }

    host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite("floret.db")})
    if err != nil {
        panic(err)
    }
    defer func() {
        if err := host.Shutdown(context.Background()); err != nil {
            panic(err)
        }
    }()

    created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
        LogicalRequestID: "create-conversation-42",
    })
    if err != nil {
        panic(err)
    }
    thread, err := host.Thread(ctx, created.ThreadID)
    if err != nil {
        panic(err)
    }
    executor, err := thread.TurnExecutor(agent)
    if err != nil {
        panic(err)
    }
    command := runtime.StartTurnCommand{
        LogicalRequestID: "send-message-42",
        UserMessage:      runtime.TurnInput{Text: "Hello"},
    }
    _, err = executor.StartTurn(ctx, command)
    if err != nil {
        panic(err)
    }
}
```

Run it with `OPENAI_API_KEY=... go run ./cmd/examples/openai-sqlite`. The complete
example also reads the authoritative assistant projection. `florettest` remains
test-only; use it for deterministic provider scripts and `florettest.NewIDSource`
for deterministic lifecycle identities.

## Public Packages

| Package | Responsibility |
| --- | --- |
| `identity` | Thread, turn, run, prompt-scope, trace, logical-request, and artifact identities |
| `config` | Provider-neutral Agent profile, prompt, context, and reasoning policy |
| `runtime` | Immutable Agent construction, durable Host lifecycle, commands, queries, and subscriptions |
| `observation` | Sanitized runtime events and host-facing projections |
| `tools` | Local tool definitions, permissions, resources, effects, and results |
| `provider` | Model Gateway contract and official provider constructors |
| `storage` | Opaque storage values and official memory and SQLite constructors |
| `storage/spi` | Advanced physical storage implementation contract |
| `florettest` | Scripted gateways and public conformance suites for tests only |

Ordinary applications use `identity`, `config`, `runtime`, `observation`,
`tools`, the official `provider` constructors, and opaque `storage.Source`
values. Custom provider transports and physical storage implementations are
advanced integration surfaces with separate conformance suites. Downstream
code must never import `internal/*`.

## Runtime Boundary

`runtime.Host` belongs in the composition root. Its public entry points are
`Open`, `Host.Threads`, `Host.Thread`, and `Host.Shutdown(ctx)`. A `Thread`
binds one exact `identity.ThreadID`. The composition root grants
`Thread.Reader`, `Thread.Lifecycle`, `Thread.TurnExecutor`, `Thread.Compactor`,
or `Thread.SubAgentManager`; application services accept only the narrow
interface they need.

`ThreadReader.Child` binds direct-child read authority for canonical detail and
pending tool targets. `ThreadReader.Descendant` binds one validated descendant
at any depth for turn pages, exact turn reads, and artifacts. Neither handle
accepts another thread identity after binding, and unrelated, deleted, or
corrupt ancestry fails closed.

Root canonical reads are issued through `ThreadReader`: bootstrap, overview,
exact turn, turn pages, todos, context, approval queue, authoritative
projections, pending-tool targets, and direct-child inventory. `Child` provides
the corresponding exact turn and turn-page authority for one direct child.
Title updates use `ThreadLifecycle.SetTitle`. Interrupted-turn and
provider-free pending-tool recovery use `ThreadLifecycle` to issue one-time
handles bound to the exact current proof or target.

Commands use stable logical request identities and explicit names:
`CreateThread`, `StartTurn`, `AdmitTurn`, `ExecuteAdmission`, `RetryTurn`,
`Fork`, `Delete`,
`Compact`, `ContinuePendingTool`, `RecordPendingToolOutcome`,
`ResolveApproval`, `UpdateTodos`, `SpawnSubAgent`, `SendSubAgentMessage`,
`InterruptSubAgent`, `WaitSubAgents`, and `CloseSubAgent`. Floret allocates all
lifecycle identities. Replaying the same logical request under the same
operation and bound authority returns the original identities; changing durable
input returns a typed request conflict.
Turn execution begins through `TurnExecutor.StartTurn` or the receipt-first
`TurnExecutor.AdmitTurn`/`TurnExecutor.ExecuteAdmission` pair.

Hosts that must bind product coordination after canonical admission can use
`TurnExecutor.AdmitTurn` to persist the user message and immutable execution
plan, then receive a `TurnAdmissionReceipt` before any provider request is sent.
They then call `TurnExecutor.ExecuteAdmission` with that receipt and an
`ExecutionContext` containing only current-turn supplemental context or an
executable signal binding, or
use `AdmitTurnResult.Execute` as a same-process convenience. The receipt is
the durable handoff point; hosts do not maintain a second queryable turn
lifecycle or persist a duplicate `StartTurnCommand`.

`runtime.NewAgent` snapshots the resolved Agent profile, system prompt,
Gateway, tools, capabilities, reasoning policy, and execution policy. The
effective snapshot and continuation state used by each run are Floret-owned
durable facts. Provider credentials and editable profile sources remain in the
host.

`ThreadReader.ReadAuthoritativeProjection` returns a canonical projection with
its revision and provenance. `DeriveThreadTurn` is only a validated offline
calculation from caller-supplied detail events and must not be stored as Agent
lifecycle authority. `Thread` exposes only capability issuers and identity; it
does not expose direct read, mutation, execution, compaction, or SubAgent
methods. Production hosts leave `runtime.Options.IDSource` nil; deterministic
identity injection belongs to `florettest.NewIDSource`.

## Consistent Reads

Every exact thread has one monotonic `runtime.ThreadRevision`. A reconnecting
consumer uses one fixed handshake through the read-only capability:

1. Call `ThreadReader.Bootstrap` to atomically load thread, initial turn page,
   approvals, todos, context, pending work, and direct SubAgents at revision R.
2. Render that complete read model and retain its page cursors.
3. Call `ThreadReader.Subscribe` with `AfterRevision=R`.

Page cursors bind thread, revision, direction, and position. Page limits are
1 through 200. If a revision is no longer readable, Floret returns
`ErrRevisionUnavailable`; the consumer obtains a new snapshot instead of
silently switching to current state.

`ThreadReader.Subscribe` observes only the exact bound thread. It is a linearized
pull protocol through `Subscription.Next(ctx)`, not a callback or bare channel.
Durable revision events tell consumers which canonical domain to query.
Provider, token, and tool progress is transient. On queue overflow the
subscription yields one Gap with its last delivered and resync revisions, then
returns `ErrSubscriptionStale` until the consumer repeats the snapshot/query/
subscribe handshake. Parent streams contain child publication and close facts,
not child execution events.

## Storage

For ordinary hosts, `storage.Source` is an opaque value consumed exclusively by
`runtime.Open`:

```go
runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
runtime.Open(ctx, runtime.Options{Storage: storage.SQLite("agent.db")})
```

Applications cannot use a Source as a lifecycle query path. Teams implementing
a physical backend use the advanced `storage/spi` contracts and their
conformance suite. SPI records remain opaque Floret data; a backend must not
decode them into a second Agent model. Memory, SQLite, and third-party backends
all run the same Floret-owned domain kernel.

## Source Of Truth

Floret exclusively owns admitted messages and references, thread/turn/run
lifecycle, titles, approvals, todos, tool invocation and outcome, pending-work
settlement, artifacts, control signals, context and compaction, provider
ledgers and state, prompt cache, SubAgent hierarchy, and Activity projections.

Hosts may persist product authorization and audit, routing, credentials,
editable persona sources, resource catalogs, read state, unadmitted commands,
upload staging, and transport diagnostics. Those records must not contain a
serialized Floret DTO or support reconstruction of Agent state. Canonical
message references are opaque durable facts; rich material needed only for the
current provider turn belongs in `SupplementalContext` and never becomes
conversation history or continuation state.

## Shutdown

`Host.Shutdown(ctx)` stops admission, cancels Host-managed provider and tool
execution, waits for it to finish, and then closes storage. If the context
expires, Shutdown returns `ctx.Err()` and Host remains closing; a later call
continues waiting. After completion, every retained handle returns
`ErrHostClosed`.

## Development

```bash
GOWORK=off go test ./...
GOWORK=off go vet ./...
GOWORK=off go test -race ./...
scripts/check_candidate_release_adoption.sh
```

Repository workflow and compatibility rules are defined in [AGENTS.md](AGENTS.md).
Architecture and maintenance knowledge lives in [okf/](okf/).
