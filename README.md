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
go get github.com/floegence/floret/v7@v7.0.5
```

Production integrations must resolve the published module. Do not use a local
`replace`, `go.work`, or sibling repository path. Earlier major versions remain
available only from their published tags; v7 does not restore retired facades.

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

    "github.com/floegence/floret/v7/config"
    "github.com/floegence/floret/v7/provider"
    "github.com/floegence/floret/v7/runtime"
    "github.com/floegence/floret/v7/storage"
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

    service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
        return agent, nil
    }))
    if err != nil {
        panic(err)
    }
    created, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "create-conversation-42"})
    if err != nil {
        panic(err)
    }
    _, err = service.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "send-message-42", Input: runtime.UserInput{Text: "Hello"}})
    if err != nil {
        panic(err)
    }
}
```

Hosts may also set `runtime.SendInput.SupplementalContext` for material that is
needed only by the current provider turn. Floret validates and renders that
context for the provider without adding a second canonical conversation
message. The pre-dispatch checkpoint records only that an ephemeral overlay was
present; it never stores the overlay, its payload hash, provider continuation,
or overlay-derived request measurements.

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
| `tools/webfetch` | Secure public-text HTTP/HTTPS fetch tool with fixed network and output limits |
| `provider` | Model Gateway contract and official provider constructors |
| `storage` | Opaque storage values and official memory and SQLite constructors |
| `storage/spi` | Advanced physical storage implementation contract |
| `florettest` | Scripted gateways and public conformance suites for tests only |

Ordinary applications use `identity`, `config`, `runtime`, `observation`,
`tools`, the official `provider` constructors, and opaque `storage.Source`
values. Custom provider transports and physical storage implementations are
advanced integration surfaces with separate conformance suites. Downstream
code must never import `internal/*`.

Tool definitions may provide `InvalidActivity` when a host needs to preserve a
safe display label from a parseable JSON object that fails the input schema.
Floret uses that callback only for presentation: the invalid invocation still
fails closed before permission, effect dispatch, and handler execution.

Question activity may include host-authored answer summaries for completed
prompts. A secret answer is represented only by `Redacted: true`; Floret
rejects a redacted answer that also carries values. These fields are display
data, not an alternate input-response or durable message authority.

Structured activity may include bounded, ordered `Rows` containing
host-sanitized text, Markdown, or code. Floret validates and preserves these
display rows without interpreting product tools or accepting arbitrary JSON
payloads.

Activity presentation is cumulative for one tool invocation. A result may add
terminal status and output, while non-empty display facts from the matching
tool call remain available in events, canonical views, and reopened threads.

SubAgent management activity uses a dedicated operation payload that preserves
the exact action, ordered child targets, and bounded outcome counts. The
existing single-SubAgent payload remains the durable child-thread fact; hosts
do not need to infer management actions from labels or collapse multi-child
results.

`tools/webfetch.New` supplies the product-neutral `web_fetch` implementation.
It performs GET-only public HTTP/HTTPS reads, revalidates redirects, DNS, and
dial targets, rejects non-text bodies, and returns Markdown or text under fixed
limits. Its typed Activity carries the requested URL, response metadata, a
2,000-character preview; complete content remains in the tool result and
artifact. It does not discover or request page icons. Hosts own static tool
iconography, visibility, current product permission policy, and UI.
Authentication, custom headers, non-GET requests, binary downloads, and browser
rendering remain separate host capabilities.

## Runtime Boundary

`runtime.Host` belongs in the composition root. `Host.ThreadService` returns the
single typed lifecycle boundary. Its `Create`, `Fork`, `Delete`, `SetTitle`,
`List`, `View`, `History`, `Send`, `Respond`, `Cancel`, `Retry`,
queue, import, and `Subscribe` methods all operate on stable thread and request
identities. Child agents are ordinary child threads with explicit parent
identity, so they use the same current-view and command contracts.

Each thread has one in-memory runtime owner. `Send` first commits canonical turn
acceptance, then publishes and returns the user item and active current view
before provider dispatch. The canonical journal is the only durable fact
source. Provider and tool I/O execute outside the thread lock.
`Cancel` is idempotent for every known thread. It commits the terminal turn
before returning, releases pending interactions, and fences late provider or
tool output without waiting for those goroutines to exit. If an irreversible
effect outcome cannot be confirmed, Floret atomically fails the turn with
`effect_outcome_unknown`, closes every unfinished tool and interaction, clears
provider continuation, and never replays the effect.
`Respond` resolves the matching approval or input interaction in place.
Public Ask User answers become one canonical user message and remain in every
later provider request. Secret answers are sent only to the current continuation;
the journal and later context retain a redacted marker, never the secret value.

`runtime.NewAgent` resolves the Agent profile, system prompt, Gateway, tools,
capabilities, reasoning policy, and execution policy. The first provider
checkpoint freezes that complete execution surface for one Turn. Ask User,
ordinary tool loops, retries, and restart recovery reuse it exactly. Provider
natural stop completes the Turn; there is no second completion protocol.
Provider credentials and editable profile sources remain in the host.

An `AgentFactory` may select a new system prompt, tool surface, provider, model,
or reasoning policy for each new Turn. The canonical conversation remains
append-only. Floret maintains a content-addressed render lineage for each exact
execution surface, clears opaque continuation state when the surface changes,
and compacts only when explicitly requested or when the selected model's
context window needs it. A waiting interaction, tool continuation, retry, or
restart remains frozen to the surface recorded for that Turn. Historical tool
calls remain readable when their definitions are removed; a new call to an
unavailable tool returns a safe ordinary tool result and the model loop
continues.

Current views contain one Floret-ordered sequence of directly renderable user,
thinking, assistant, tool, and interaction items, plus pending interactions and
the accepted queue. Each item has a stable ID and ordinal; live deltas grow the
same item in place, and tool approval, dispatch, and result state update the
original tool item. Canonical reload derives the same sequence without a
presentation ledger or draft mirror fields.
Every item and interaction carries its exact `TurnID` and `RunID`, so multi-turn
history and same-turn continuation never borrow identity from the current run.
`ThreadView.RunID` identifies only the current execution; hosts must never use
it to fill historical items. `RunProgress` is the actor-owned, process-local phase
for an advancing run and is absent while awaiting interaction or after the run
settles.
`ViewVersion` is process-local notification ordering, not a durable replay
cursor. Production hosts leave `runtime.Options.IDSource` nil; deterministic
identity injection belongs to `florettest.NewIDSource`.

## Consistent Reads

`ThreadService.View` returns one complete replaceable current view.
The value returned by `Host.ThreadService` also implements
`ThreadContextReader`; `Context` returns Floret's canonical context policy,
usage, and one latest lifecycle record per compaction operation, including
terminal state restored after runtime restart. The snapshot also exposes
conversation-wide disjoint input, output, cache-read, and cache-write totals
folded from canonical final provider usage records.
Each successfully committed final `provider_usage` runtime event also carries
`ThreadUsageTotals`. It is the live form of the same canonical fold; projected
requests, stream-only usage, rejected attempts, and failed writes omit it.
`ThreadService.Subscribe` publishes workspace summary and current-view updates;
reconnecting clients refresh summaries and the currently visible view. There is
no durable cursor, replay ledger, materialized projection, or second lifecycle
authority.

## Storage

For ordinary hosts, `storage.Source` is an opaque value consumed exclusively by
`runtime.Open`:

```go
runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
runtime.Open(ctx, runtime.Options{Storage: storage.SQLite("agent.db")})
```

Hosts that present startup readiness may observe Floret's product-neutral
storage phases without reading physical records:

```go
host, err := runtime.Open(ctx, runtime.Options{
    Storage: storage.SQLite("agent.db"),
    StartupProgress: runtime.StartupProgressFunc(func(phase runtime.StartupPhase) {
        // Present migrating or verifying without exposing stored content.
    }),
})
```

The callback is synchronous and must return promptly. Legacy stores report
`migrating` followed by `verifying`; fresh and current stores report only
`verifying`. A returned `Host` is ready, and a failed migration remains atomic.

Applications cannot use a Source as a lifecycle query path. Teams implementing
a physical backend use the advanced `storage/spi` contracts and their
conformance suite. SPI records remain opaque Floret data; a backend must not
decode them into a second Agent model. Memory, SQLite, and third-party backends
all run the same Floret-owned domain kernel.

New SQLite stores use incremental auto-vacuum so deleted pages can be reclaimed
without rebuilding the database. A host that owns an older SQLite file may call
`storage.MaintainSQLite` before `runtime.Open`. The maintenance boundary checks
the exact Floret physical schema and database integrity, refuses an open
runtime, and uses SQLite's native `VACUUM` or `incremental_vacuum`; it never
copies records or exposes their contents.

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
