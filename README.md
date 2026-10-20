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

## Request output budgets

Set ContextPolicy.ModelMaxOutputTokens to the model output capability to enable
request-fitted budgets. MaxOutputTokens remains an explicit per-request ceiling.
Automatic output is min(model capability, effective window - projected input -
min(window / 100, 4096)); automatic compaction starts at 90% of that window.
An explicit ceiling is never silently reduced and may require earlier compaction.
The margin is inside the default 10% compaction buffer, not another output reserve.
Policies omitting ModelMaxOutputTokens retain their published v7 output-reservation semantics.
Automatic summary and title requests also respect the opted-in model capability.

Gateways declare provider.OutputControlClient or provider.OutputControlServer.
Client-controlled prepared requests implement PreparedRequestOutputLimiter to
return a new immutable wire snapshot from frozen input. Attachments and managed
facts are not resolved again; network retries replay the final snapshot. A
server-controlled gateway cannot accept an explicit client ceiling. Observation
reports request_output_tokens, output_control and safety_margin_tokens for the
actual request, separately from input usage and composition estimates.

Domain schema 28 retains the atomic 17 -> 18 output-budget edge and
18 -> 19 for canonical parent events, and appends 19 -> 20 for completed-error input, retaining every older migration
and all historical conversation facts without inferring historical capabilities.
The 21 -> 22 edge admits request-bound accounting; 23 -> 24 adds frozen character attribution and input measurement scope without rewriting canonical entries.
Older writers reject the upgraded envelope. Cancellation, bounded length
continuation and unknown-effect recovery retain their existing boundaries.


### Native measured prefixes

The Engine evaluates each complete frozen next request once. Projected input is
qualified native input (or native input plus normalized output at a committed
response boundary) plus an estimate of only the subsequent frozen protocol input.
Without a matching measurement it uses the complete conservative request estimate.
The automatic threshold stays min(90% of the window, request safe limit); output
reservation semantics and the bounded overflow recovery are unchanged.

Gateways may implement provider.PreparedRequestMeasurement and attach
provider.RequestMeasurement to native Usage. Evidence binds the exact attempt,
account/execution identity, full frozen input and successful terminal response.
The provider.RenderedMeasurementPrefix and RenderedMeasurementSuffix helpers hash
ordered protocol items and the fixed input surface without retaining content.
Suffix estimation uses Floret's estimator; adapters do not calculate pressure.
Neither method may read attachments, refresh context, authorize tools or count
through the network. Output refits and retries preserve the frozen input.

InputTokenScope retains its protocol meaning: provider_execution alone never
qualifies. Incomplete visible attribution does not invalidate a proven total.
Encrypted reasoning requires explicit protocol inclusion evidence and a matching
native boundary, never a ciphertext-length estimate. New opaque suffixes invalidate
calibration. Model, account/connection, tool/system surface, estimator, canonical
history and compaction lineage must match. Native cache buckets are counted once;
OutputTokensReported distinguishes explicit zero, and OutputIncludesReasoning
prevents duplicate output counting. Aggregated or failed attempts cannot establish
a response boundary. Input plus output is a conservative projection, not measured
current occupancy, because some generated tokens may not be replayed.

Automatic pressure is evaluated only after context collection and freezing.
Responses update measurements without scheduling a second compaction path.
Manual and overflow compaction retain the same bounded generate/validate/install
lifecycle. A safe soft-threshold no-op permits sending and is suppressed only for
the same frozen input and budget; hard-limit failures never dispatch. Successful
installation clears old measurement and continuation. Existing decision evidence
retains selected boundary and invalidation reason without a second ledger.

Domain schema 27 -> 28 fences these semantics. It validates canonical authority
without rewriting historical messages, usage, summaries or missing evidence.
Historical input anchors remain eligible under their original transparent-input
contract; new response boundaries require new evidence. Invalid calibration falls
back to estimation and cannot prevent historical thread opening.

## Historical conversation compatibility

Upgrades preserve supported historical conversations as a release requirement.
Domain schema 26 -> 27 repairs only compaction references proven by canonical
journal identity or the exact released writer contract. Startup and read-only
inspection share this migration; unresolved authority rolls back the complete
transaction. Historical messages, tool outcomes, attachments, timestamps and
compaction decision evidence remain intact. No task or tool resumes automatically.

The journal owns durable message identity. Engine installs committed canonical
messages and the complete canonical context after compaction; observation events
never persist the same output again. Invalid boundaries fail explicitly instead
of guessing a retained exchange. Provider-context revision v11 establishes an
observable lineage upgrade without rewriting old requests, responses or usage.

Immutable synthetic released-writer databases cover repeated compaction,
interleaved observations, Fork and nested Fork. Ordinary tests verify migration
and rollback; candidate and published adoption gates also continue these threads
through public APIs across restart. Incident qualification uses separate copies
and fake providers, never production execution.

## Canonical output sources

The Engine owns one ordered response fragment collection for each exact Run and
provider attempt. Optional `provider.Event.OutputPart` identifies native items
and zero-based output/content positions; gateways without native items remain
valid and use Engine channel/hosted boundaries. DeepSeek preserves native parts,
including terminal suffixes that update an earlier fragment.

Harness commits those fragments, classified calls and opaque provider state in
one existing provider-context transaction. One memory-only batch handoff installs
the canonical view before tool execution. Observation events do not independently
settle drafts. Current-attempt drafts can survive a refresh; committed canonical
text is authoritative and replaced attempts cannot change it.

Schema 24 -> 25 stores only content/reasoning source references on canonical
messages. It preserves historical public item IDs and entry references, reveals
correction-response reasoning at its original position, and keeps invalid calls
and validation feedback out of actionable tool rows. Technical feedback remains
in model history. Legacy shape interpretation runs only in the atomic startup
migration; current restart does not rewrite records. Hosts render Floret order
and IDs directly without content deduplication or client sorting.

## Thread goals

Opt in with `runtime.WithAgentGoals()` to let the executing model maintain one
clear objective and reflect before naturally stopping. Use the optional
`runtime.ThreadGoalController` for user `SetGoal` / `DeleteGoal` operations;
changes appear in `ThreadView.Goal` and the canonical timeline immediately and
enter the next new model request. Management never starts execution. Reflection
reuses the existing context, provider continuation and bounded stop hook; Goal
completion requires a current evidence-bearing review and successful Turn end.
Goals are independent of todos. The 22 -> 23 schema edge preserves history.
For an explicit user command that also pursues the objective, use the optional
`runtime.ThreadGoalSubmitter.SubmitGoal`. Persist product execution authorization under
`runtime.GoalSubmissionRequestKey` before submission so ordinary execution and restart
resolve the same identity. It atomically saves the Goal and admits
its submitted text as a normal user Turn only when idle. Active and waiting
Turns are updated without queuing work or resolving interactions. Replays retain
the original decision, including after Stop or restart. Execution preparation
failures retain the Goal and use the existing recoverable Turn failure surface.
This additive API reuses schema 24 journal shapes; no migration is needed.
See [the Goal contract](okf/decisions/thread-goals.md).

## Passive child events

Set `runtime.Options.EnableParentEvents` explicitly to enable the optional
`runtime.ThreadParentEventService` returned by `Host.ThreadService`. The default
keeps existing hosts' model inputs unchanged. This additive API decision serves
Flower's child reporting tools without adding methods to `ThreadService`.

`ReportToParent` derives the direct parent from canonical child ownership and
verifies the authorized active tool invocation. Reports require nonempty UTF-8
text up to 8192 bytes. Exact invocation replay returns its stable receipt;
conflicting content or identity fails. Hosts supply invocation authority, never
model-selected sender, recipient or wakeup flags. Reads must be freshly
authorized by the host. Read-only child policy may permit reporting to its own
parent without granting other thread writes or interaction responses.

Completion, failure, cancellation, confirmed interruption, typed execution
request timeout, input and approval facts create references atomically with their
source. Running states and message previews create none. Receipts and bounded,
frozen admissions live in the existing journal. No inbox, consumer table,
periodic scan or second lifecycle is introduced. Only unresolved interactions
are bootstrapped on opt-in; historical terminal facts are not backfilled.

A natural logical provider boundary admits pending events after tool pairs and
before budgeting, compaction and request freezing. Receipt never sends, steers,
wakes an idle thread, answers an interaction or resamples a natural completion.
Retry retains admitted text even when preparation failed. Forks inherit their
cut's receipts and admissions; later events still target the original parent.
Compaction preserves canonical read access and admission coverage.

Automatic event projections include stable source and event references. Reports
have at most a 512-byte UTF-8 preview. Individual projections are bounded by
2 KiB/512 estimated tokens and each new event message by 8 KiB/2048 estimated
tokens, including framing. Unresolved interactions and unknown-effect failures
have priority. Overflow includes a closed event range and requires reading the
omitted events before claiming all delegated work has been reviewed. Normal
whole-request budget failures remain authoritative; no extra sampling drains
notifications. Error previews contain reason codes, not raw failures or approval
commands. Use sufficient untruncated reports directly; read report events only
when their full text is needed. Completion events contain no final report:
hosts expose a separate result read capability for final handoffs and failure
details. Reuse an already available result for the same child turn.

`tools.SubAgentOperationStatus` identifies metadata-only child checks in typed
Activity presentation. It is distinct from `SubAgentOperationInspect`, whose
result may include a complete final report. This additive action serves Flower
without changing existing inspection, waiting, or event-read contracts.

`ListParentEvents` exposes metadata, filters and explicit pagination;
`ReadParentEvents` reads full reports by event ID or a closed immutable range.
`ThreadView.ParentEvents` is a bounded paginated receipt view separate from
streamed item ordinals. Successful event pages always encode events as an array,
including an empty array for an empty page; disabled parent events remain absent. Its `Admitted` field means context placement, never
“model read”. Hosts use existing subscriptions before checking pending events
when implementing event-aware waits; waiting timeouts are not execution timeouts.
`ParentEventPage.ThreadID` (`thread_id`) identifies the queried canonical thread
path on every successful list, read and current view, including empty or filtered
pages. A fork inherits events without rewriting their original `ParentThreadID`;
that recipient can differ from the page identity. Hosts validate page scope
against the requested thread and authorize source navigation independently.

ParentEvent.ParentTurnID (JSON parent_turn_id) identifies the most recently
started ordinary parent turn preceding receipt in canonical Path order. It
remains that turn after termination until another ordinary turn starts; context
compaction does not advance it. No preceding ordinary turn means the field is
omitted. Forks retain inherited ownership and original recipient identity.
List, full read, pagination and restart derive the same field without changing
historical bytes or admission. This projection does not change the domain schema.
Hosts own presentation.

## Install

Build with Go 1.27.2.

```bash
go get github.com/floegence/floret/v7@v7.40.0
```

Production integrations must resolve the published module. Do not use a local
`replace`, `go.work`, or sibling repository path. Earlier major versions remain
available only from their published tags; v7 does not restore retired facades.

Prepared request estimates count visible input, protocol framing, and conservative
media/unknown-content bounds. Encrypted and redacted reasoning stays replayable,
but ciphertext bytes are diagnostics, not token counts. Such estimates retain
partial attribution and cannot calibrate pressure from provider-execution usage.
The Engine's bounded overflow recovery remains authoritative for hidden input.

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

`runtime.UserInput.References` and `runtime.UserInput.Context` are admitted
with the user message and remain model context through follow-ups, Ask User
responses, retries, forks, restart, and compaction. Reference kind, label, text,
and truncation status render in submitted order; opaque `ResourceRef` values
never enter model text. Context items have `Kind`, `Title`, and `Text` and describe
non-secret runtime facts effective from that message. Append a new snapshot when
facts change. Context is included in admission fingerprints and request-prefix
checks; it does not grant tool authorization.

Hosts may also set `runtime.SendInput.SupplementalContext` for material that is
needed only by the current provider turn. Floret validates and renders that
context for the provider without adding a second canonical conversation
message. The pre-dispatch checkpoint records only that an ephemeral overlay was
present; it never stores the overlay, its payload hash, provider continuation,
or overlay-derived request measurements.

Hosts that need immutable execution metadata may construct an Agent with
`runtime.WithAgentRunLabels`. Correlation labels remain observable, while the
opaque Host map reaches provider requests, permission checks, and local tool
invocations without becoming durable conversation state.

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

Structured rows and patch mutations may each carry an optional Target
(ActivityTargetRef) identifying their own observed resource. Targets survive
cloning, result merges, finalization and journal replay. Paths are navigation
facts, never authorization or a guarantee that the resource still exists; hosts
must authorize current access. Omitted targets preserve existing JSON behavior.
Top-level targets and targets on structured inputs, result rows, and patch
mutations retain exact Path and URI values in canonical history and host-facing
live observations. Display text remains sanitized. Diagnostic traces redact
navigation paths at every target location without changing canonical facts.
This boundary does not rewrite or reconstruct previously redacted history.

Structured activity may include bounded, ordered `Rows` containing
host-sanitized text, Markdown, or code. Floret validates and preserves these
display rows without interpreting product tools or accepting arbitrary JSON
payloads. Optional `Inputs` retain safe invocation details independently of result
rows. `RowsProvided` distinguishes a captured empty result from missing historical
output. Code rows support a syntax `Language`, up to 64 KiB of content, and an
explicit `Truncated` preview marker. Other row text remains bounded at 8,000 runes.

Activity presentation is cumulative for one tool invocation. A result may add
terminal status and output, while non-empty display facts from the matching
tool call remain available in events, canonical views, and reopened threads.

`tools.TerminalActivityPayload` adds optional operation, sent-byte count,
output cursors, remaining-output flag, total bytes, public execution location,
and timeout facts. Hosts author the label and description and retain only safe
command display text; raw stdin and credentials are never display fields.
Read cursor snapshots replace their cursors and `has_more` together, so a final
page can clear the flag. Status-only updates preserve the prior snapshot.

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
single typed lifecycle boundary. Title snapshots expose `Title`, `TitleStatus`, and
`TitleGeneration`; hosts order them independently of body updates and activity.
See the [title snapshot contract](okf/api/runtime.md#title-snapshots). Its `Create`, `Fork`, `Delete`, `SetTitle`,
`List`, `View`, `History`, `Send`, `Respond`, `Cancel`, `Retry`,
queue, import, and `Subscribe` methods all operate on stable thread and request
identities. Child agents are ordinary child threads with explicit parent
identity, so they use the same current-view and command contracts.

The optional `runtime.ThreadToolResultReader.LookupToolResult` reads confirmed
local tool arguments and output from the active journal path using exact thread,
turn, run, and tool-call identity. Hosts authorize and interpret that private
output; the query neither runs a tool nor changes history. Live and historical
items retain their creation times, including original occurrence times in forks.
Hosts may pin a committed assistant answer with the optional ForkThreadInput.SourceItemID; the runtime resolves its canonical journal boundary and rejects unknown or conflicting anchors.

The optional `runtime.ThreadQueryReader` implements `Query`, `Read`, and
`Aggregate` using `threadquery` types. It searches canonical active-path threads,
user/assistant messages, and persisted local/hosted tool facts through the
canonical owner's transactional derived index. Hosts provide an exact readable ThreadID set
and an authority key on **every** call; reference snapshots never grant access.
Filters combine with AND (values within one field use OR), support literal
Unicode substring/keyword/phrase search, and use UTC half-open time ranges.
All sorts use stable identity ties. Queries default to 50 records (maximum 200);
`Read` explicitly continues UTF-8 chunks up to 16 KiB in responses up to 64 KiB.
Counts and grouping use the same authorized filter. Cursor snapshots last up to
five minutes within a 64 MiB global budget, survive append/completion/rename,
and fail explicitly after scope changes, deletion, expiration or restart.
See the [thread query contract](okf/api/thread-query.md) for keys and limits.

`runtime.MessageReferenceThread` adds a lightweight admitted snapshot using
`ReferencedThreadID`, `Label`, and `Text`. The provider receives the explicit
referenced identity as data; hosts retain read authorization. Existing reference
rendering and input fingerprints stay byte-identical when this field is absent.

The optional `runtime.ThreadSendReader.LookupSend` returns original admitted
input for a request key, including after queue edits, promotion, removal, or
restart. Hosts can validate transport retries against canonical input without
refreshing context or resolving attachments again.

`Host.MediaInventory(ctx)` exposes opaque media descriptors retained by every
canonical journal, including archived threads, inactive paths, and queue facts,
and reports whether every retained execution is terminal.
Hosts use this read-only inventory to retain their immutable media bytes instead
of inferring absence from a current view. It grants no resource access. Hosts
must hold their tool-media write gate before reading this snapshot and collect
only when `Idle` is true, or before deferred execution is activated. Other
not-yet-admitted uploads require their existing owner protection. Floret owns no
media byte store.

The optional `runtime.ThreadQueueController` supports `EditQueued` with a text
precondition and `SendQueuedNow` to stop gracefully and start one chosen queued
input. Text editing preserves attachments, references and context. Complete editing
uses paired `ExpectedInput` and `Input` to replace text, attachments, references
and context atomically while preserving identity and position.
Already admitted or concurrently edited input is rejected without replacement.
`ThreadQueueEditReader.LookupQueuedEdit` reads immutable before/after content
from those canonical facts to validate retries after promotion or restart.
`DeleteQueuedInput.ExpectedInputSHA256` optionally requires the complete pending input
to match the lowercase SHA-256 hex digest of its `encoding/json` representation
before removal, returning `ErrRequestConflict` for missing, started or
changed items. Hosts can use this boundary to take a queued message back into an
ordinary draft without racing automatic promotion. Exact committed retries succeed.
See the [queue control contract](okf/api/runtime.md#queued-message-controls).

Each thread has one in-memory runtime owner. `Send` first commits canonical turn
acceptance, then publishes and returns the user item and active current view
before provider dispatch. The canonical journal is the only durable fact
source. Provider and tool I/O execute outside the thread lock.
`Cancel` is idempotent for every known thread. Its default `immediate` mode
preserves synchronous terminal settlement. Interactive hosts can opt into
`CancelInput.Mode: runtime.CancelModeGraceful`: Floret commits the stop request,
fences new execution, cancels the execution context, and returns acceptance.
In-flight tools share one five-second window to commit confirmed results through
the normal output and artifact path. The terminal is cancelled when all effects
are confirmed; real tool errors and completed effects remain visible. A stop does
not roll back earlier changes. Queued input remains queued until a later explicit
submission or promotion. `CancelInput.IncludeDescendants` also stops the existing
child subtree, including children of an idle parent, while preserving independent
threads. Child creation and stop admission share one boundary; canceled child
creation cannot run after waiting for that boundary.

Retrying a failed turn uses its latest committed save point and retains completed
tool results. Repeated failures and restart preserve that context without adding
another user message. A completed-turn retry retains its regeneration behavior.
Thread-level queue facts can interleave a waiting turn without invalidating its
canonical history or preventing a response after restart.

`ThreadView.Cancellation` and `ThreadSummary.Cancellation` carry the canonical
source, exact execution identity, mode, and request time. Active views with that
fact are stopping; hosts must keep that indication until the authoritative view
becomes terminal, independently of the HTTP request. A dispatched effect with
unconfirmed outcome fails the turn with `effect_outcome_unknown`; only unresolved
dispatched tools become unknown, while undispatched calls are cancelled. Floret
never automatically replays unknown effects or permits retrying that turn. Late
results cannot change the terminal or a later turn. Use `tools.CanceledResult`
only when execution has actually ended; a bare `context.Canceled` is insufficient.
Schema v11 appends the v10 to v11 migration without rewriting historical records
or inventing missing stop provenance.

A completed tool may return `tools.Result.InputRequired` with a validated
`tools.InputRequest`. Completed success and error results may request input; errors retain their status.
Unconfirmed, canceled and declined results cannot request input. Floret commits the result and pauses before the next model
request, after every tool in the batch settles. The existing input interaction
survives restart; `Respond` must answer every question, and all batch interactions
must be resolved before continuation. The original tool is never replayed.
Prompts, options, and answers are durable, non-secret data. Use acknowledgment
choices for external user actions; collect passwords and tokens outside the
agent. `Pending` remains asynchronous host work and cannot be combined with input.
Domain schema v21 appends v20 to v21 while preserving the original v11 to v12
contract and all historical facts.

OutputPolicy.ArchiveAboveBytes independently archives complete Text above
a positive byte threshold when PreserveFull is enabled, even without visible
truncation. Zero retains archive-on-truncation behavior. Archival uses the existing
atomic result/artifact settlement and never marks uncut Text as truncated.

Ask User `runtime.InputPresentation` exposes optional `ReasonCode`,
`RequiredFromUser`, and `EvidenceRefs` from the validated control signal in current,
summary, and historical views. Evidence associates facts; it grants no access or
permission. Absent historical fields remain absent.

Ask User `runtime.InputQuestion` preserves optional `Header`, `WritePlaceholder`,
`ChoicesExhaustive`, and rich `Choices` from the original validated call across
live updates, reads, and restart. Each `InputChoice` has an original `ChoiceID`,
`Label`, optional `Description`, and the established answer `Value`. Submit
`Value` through `Respond`; the original identifier is display identity, not a
new answer encoding. Existing v7 `Options` strings remain supported and match
rich choice values in order. Historical and tool-origin input without rich
metadata remains valid; hosts must not invent missing presentation content.

`Respond` resolves the matching approval or input interaction in place.
Public Ask User answers become one canonical user message and remain in every
later provider request. Secret answers are sent only to the current continuation;
the journal and later context retain a redacted marker, never the secret value.

`runtime.NewAgent` resolves the Agent profile, system prompt, Gateway, tools,
capabilities, reasoning policy, and execution policy. The first provider
checkpoint freezes that complete execution surface for one Turn. Ask User,
ordinary tool loops, retries, and restart recovery reuse it exactly. Provider
natural stop completes the Turn; there is no second completion protocol.
Assistant citations are canonical message facts. `provider.Source` keeps optional
`StartIndex` and `EndIndex` as end-exclusive Unicode code-point positions in the
complete response text. `runtime.ThreadItem.Sources` exposes detached, message-relative
ranges in current/history views. Unsafe URLs are dropped; malformed or cross-segment
ranges become unlocated sources. Hosts render exact citations and list unlocated
sources without inferring URLs from provider markers. Sources events remain live
observations; they are not a second durable source store. Existing schema-16 bytes
without sources remain unchanged.

Provider credentials and editable profile sources remain in the host.

An `AgentFactory` may select a new system prompt, tool surface, provider, model,
or reasoning policy for each new Turn. The canonical conversation remains
append-only. Floret maintains a content-addressed render lineage for each exact
execution surface, invalidates exact-surface continuation when that surface changes,
and compacts only when explicitly requested or when the selected model's
context window needs it. A waiting interaction, tool continuation, retry, or
restart remains frozen to the surface recorded for that Turn. Historical tool
calls remain readable when their definitions are removed; a new call to an
unavailable tool returns a safe ordinary tool result and the model loop
continues.

Hosts that need live capability changes can explicitly return
`ToolSurface.RefreshProviderSurface: true` from `WithAgentDynamicToolSurface`.
Each subsequent provider request then uses current system text and local/hosted
tool definitions. Provider, model, reasoning, context policy and compatibility
identity remain fixed by the first Turn checkpoint. Changed envelopes establish
a new render lineage without compaction or rewriting earlier checkpoints.
The zero-valued `provider.State.ReusePolicy` requires an exact surface. Adapters
may declare `provider.StateReuseCanonicalHistory` when they validate the complete
canonical prefix before replaying native history across tool or permission changes.
Unknown policies fail explicitly. Model/account identity changes, canonical
lineage resets, installed compaction and supplemental secret input still fence state.
Completed response state commits atomically with its canonical assistant and
tool-call output before tool dispatch or another provider request. Failure, Stop
and restart retain the last committed compatible state and its source Run/Turn;
terminal settlement never reinstalls a pre-compaction result. The default remains the frozen Turn surface.
Dispatch-time permission resolution and existing approvals remain independent:
refreshing model-visible tools does not authorize an invocation or settle a wait.

Gateways may declare `Capabilities.ReasoningHistory` as
`provider.ReasoningHistoryCurrentUser` to omit assistant reasoning before the
latest canonical user message from transport. Visible history and tool pairs
remain intact; tool loops and truncated output after that input retain their
reasoning. Ephemeral supplemental answers do not create a replay boundary.
Canonical storage and earlier checkpoints are unchanged. The default
`ReasoningHistoryAll` preserves all reasoning. Include the selected policy in
the gateway's state compatibility identity when changing an existing adapter.

Automatic provider recovery belongs to Floret. Agents default to four additional
attempts, with at most three timeout retries, jittered 2/4/8/10-second backoff and
a five-minute window from the first failure. `WithAgentProviderRetryPolicy`
accepts an explicit validated policy; use `ProviderRetryPolicy{Disabled: true}`
to disable recovery. SDK transport retries must be disabled.

`WithAgentProviderRequestTimeoutPolicy` configures one physical request clock,
shared by ordinary requests, compaction and titles. Zero fields use
`DefaultProviderRequestTimeoutPolicy`: five minutes without first semantic output,
two minutes without further output, five minutes while a provider-hosted tool is
active, and fifteen minutes total per attempt. Negative durations are invalid.
Text, reasoning, argument fragments and accepted tool transitions advance the
clock; heartbeats, empty deltas and usage do not. Connection opening and reading
are bounded; local tools, approvals and user input are outside this boundary.

A deadline cancels only that attempt and enters the same recovery budget.
`provider.RequestTimeoutError` matches `provider.ErrRequestTimeout` and
`context.DeadlineExceeded`; it does not establish execution cancellation.
`ThreadTurnFailure.ProviderTimeout` optionally retains the stage, timing and
attempt identity through canonical terminal metadata and restart. It is an
additive diagnostic contract; schema 17 and all historical migrations remain
unchanged. `ProviderRequestProgress.LastProgressAt` lets hosts display stalled
output using their own presentation clock without inventing lifecycle timers.

Gateways expose structured `provider.RetryAdvice` through `RetryAdviser` and
`WithRetryAdvice`, preserving the original error cause. `HTTPRetryAdvice`
parses response-time server deadlines, exact machine codes and HTTP status.
At provider transport boundaries, `provider.RetryAdviceForError` shares the
Engine classifier, including ordinary and wrapped EOF as stream interruption.
The outermost explicit adviser wins even with empty advice. This helper does
not schedule recovery and must not classify storage, decoding or tool errors.
The exact `basispoints_tool_arguments_stalled` code, including HTTP 200 stream
errors, uses `RetryStreamInterrupted` and the same bounded recovery policy.
Unknown codes and error prose do not establish retry permission; permanent
statuses and explicit server retry prohibitions retain priority.
Authentication, billing, invalid contracts, unknown errors and unsafe hosted
requests are never automatically replayed. Declare `HostedToolDefinition.ReplaySafe`
only for verified read-only provider capabilities. Expanded prepared gateways
implement `PreparedRequestReplay` to create fresh single-use attempts from the
original immutable rendering without reopening attachments.

Recovery retains the same turn, run and agent step. It replaces only the failed
streaming draft, never confirmed tool effects. `Event.Retry` and
`ThreadRunProgress.Retry` expose count, limit, absolute next-attempt time, reason
and replaced attempt identity for a shared host countdown and working Stop.
Retry state is memory-only; restart uses existing interrupted/manual Retry rules.

`provider.Request.ReusePolicy` carries the Engine's effective cache namespace,
native-state permission, and continuation preference through direct and prepared
gateways. Gateways must not broaden this policy. Supplemental context disables
application reuse and native-state retention; nil preserves defaults for callers
outside the Engine. Queue admission and replay share one validated canonical
base, so queue-only edits preserve native history without ancestor fallback.

`ToolSurfaceRequest.InitialProviderSurface` supplies a detached copy of the first
checkpoint's system text and tool definitions, including after restart; it is nil
before the first request. Hosts can preserve unrelated capabilities from this
snapshot while refreshing only their live policy. It contains no executable
handlers and does not authorize tools or create a second persistence contract.

Current views contain one Floret-ordered sequence of directly renderable user,
thinking, assistant, tool, and interaction items, plus pending interactions and
the accepted queue. Each item has a stable ID and ordinal; live deltas grow the
same item in place, and tool approval, dispatch, and result state update the
original tool item. Canonical reload derives the same sequence without a
presentation ledger or draft mirror fields.
Every item and interaction carries its exact `TurnID` and `RunID`, so multi-turn
history and same-turn continuation never borrow identity from the current run.
Local-tool Activity execution starts at its recorded dispatch and ends at its
canonical result. These existing timestamps survive terminal refresh, history
reads, restart and forks; admission and approval waits do not become execution
time. A missing dispatch leaves the execution interval unavailable. SubAgent
management Activity describes the invocation's outcome and elapsed execution,
while a target's saved status describes the child at that operation's boundary.
`ThreadView.Turns` and `HistoryPage.Turns` expose canonical per-turn outcome,
start/end times, elapsed milliseconds, waiting milliseconds, and work duration.
Waiting deducts the union of user-input and approval intervals; queue time is
excluded. `FinalBoundaryComplete` distinguishes recorded accepted final-response
item IDs from unavailable legacy boundaries. Render every `FinalItemIDs` segment
in order; never infer final answers from text or the latest assistant item.

`ThreadItem.CanonicalOrdinal` and `ParentEvent.CanonicalOrdinal` (JSON
`canonical_ordinal`) are one-based source positions on the queried canonical
Path. Merge public items and parent events by this position, preserving item
order at ties; occurrence timestamps are display facts only. Tool updates retain
their first position. Zero means an uncommitted live item, whose tail order stays
unchanged. These positions are neither durable cursors nor execution identities.
Forks derive positions from their own Path and preserve all copied occurrence
timestamps; inherited event recipient identities remain unchanged.

`ThreadTurn.IntermediateItemIDs` (JSON `intermediate_item_ids`) contains ordered,
unique segmented assistant items superseded by a later local tool call, hosted
activity or semantic continuation in the same Turn/Run. Accepted final references
are disjoint. Length continuation alone does not supersede text. Trailing partial
answers and unsegmented legacy text remain unclassified. Hosts may fold these
confirmed intermediate items independently of terminal outcome; unknown prose
must remain visible. These are read projections, with no journal rewrite or
schema change (current domain schema 28).


`ThreadView.RunID` identifies only the current execution; hosts must never use
it to fill historical items. `RunProgress` is the actor-owned, process-local phase
for an advancing run and is absent while awaiting interaction or after the run
settles.
`ThreadRunProgress.Request` reports the current provider attempt's start, first
output and estimated received output tokens (text, reasoning and local tool
arguments). Estimates are incremental ASCII/non-ASCII character heuristics, not
billable usage. Hosts can render waiting duration from the start timestamp without
asking the runtime to publish timer ticks.

`ThreadView.ToolCallPreviews` contains memory-only, unvalidated argument previews.
Tools may opt in through `tools.Definition.NewArgumentPreview`, a per-call bounded,
side-effect-free parser returning a safe display name and optional line counts.
Unknown counts remain absent; known zero remains explicit. Preview errors only
remove the preview. They never authorize, dispatch, read resources, reserve item
ordinals or enter the journal. Match admitted calls by exact execution identity
and CallID; actual results replace preview counts, including zero. Both request
progress and previews are attempt-fenced and discarded on replacement or finish.

`ViewVersion` is process-local notification ordering, not a durable replay
cursor. Production hosts leave `runtime.Options.IDSource` nil; deterministic
identity injection belongs to `florettest.NewIDSource`.

## Independent Context Compaction

The service returned by `Host.ThreadService` implements the optional
`ThreadContextCompactor`. Call `CompactContext` with only `ThreadID` and
`RequestKey`; keep slash-command text, drafts, attachments, and transient host
context outside this operation. It accepts an idle thread with no queued input
or unresolved interaction and returns after durable admission, before resolving
its Agent or calling the provider. Busy threads return `ErrThreadBusy`.

`TurnKindContextCompaction` identifies this control turn in `AgentRequest`,
`ThreadView`, and `ThreadSummary`; the zero kind remains ordinary execution.
Factories resolve the thread's configured Agent for this kind without requiring
a user message. Manual compaction has real Turn and Run identities, uses the
same summary generation and validation as automatic compaction, and finishes
without an ordinary assistant answer or tool dispatch. Automatic compaction
continues the admitted task after installing the summary.

Summary generation reads the complete eligible provider-history projection,
including message formatting, tool arguments, references, and admitted runtime
facts. It never replaces ordinary messages with fixed-length previews. One
ordered batching loop carries only the latest candidate summary; oversized
messages use lossless UTF-8 fragments with their message/tool metadata. Rendered
request estimates and bounded overflow recovery determine batch size. All
batches must succeed before installation, and incomplete or over-budget summaries
fail without truncating or changing the effective context. Intermediate summaries
are in memory only. The default summary ceiling remains 20,000 tokens; the
50,000-token compacted-context target is not a minimum summary length.

Manual compaction need not reach the automatic pressure threshold. Context below
the configured target (50,000 tokens by default), a missing safe cut point, or
insufficient savings produces an explicit noop. Failed, cancelled, and noop
operations preserve the effective context and its provider continuation.
Installation atomically commits the summary, context lifecycle, new estimate,
and manual terminal; a committed installation wins later cancellation.
Success clears old continuation and confirmed usage. The next ordinary request
supplies a new native measurement.

Replaying a request key returns the latest canonical view without executing or
installing again; reusing the key for another operation kind conflicts. Retry a
failed or cancelled compaction with a new compact request key. Ordinary `Retry`
returns `ErrNoRetryTarget` for a context-only turn. Context snapshots expose one
result per operation and its canonical `AfterItemID` presentation anchor; an
empty history has no artificial anchor. Stop, shutdown, restart, and fork use
the existing thread lifecycle. Domain schema v17 retains the automatic v12 -> v13
typed-turn edge and v13 -> v14 canonical run-identity edge without
reclassifying historical slash-command conversations or using observation
metadata as runtime authority. The v14 -> v15 edge adds typed thread references
without rewriting historical messages or request fingerprints.

## Visual Compaction And Historical Images

Compaction sends canonical user images and confirmed tool-result images to the
selected summary model as native image attachments, beside source labels. One
ordered batch cursor covers full text, UTF-8 fragments and indivisible images.
The complete prepared request, including resolved image cost, must fit; an image
that cannot fit fails explicitly. Transport retries replay frozen input and
output refits never reopen attachments. No partial summary is installed.

`runtime.MessageAttachment.Locator()` returns a stable `media:v1:<sha256>` identifier
of the complete descriptor, including optional text statistics. It is independent
of thread and entry identity and grants no access. Model labels expose the
locator, name, MIME and size, never the opaque `ResourceRef`. Hosts authorize
current canonical membership before resolving original bytes. `ThreadItem.Attachments`
contains both user attachments and confirmed tool-result attachments; scoped
thread search includes their names/MIME and chunked reads return safe labels.

Checkpoints preserve recent user attachment labels and explain that summaries
are lossy: exact text, numbers and positions require rereading the original by
locator. Recent tail images retain normal native input. Older images remain
discoverable through canonical history, without an unbounded checkpoint catalog.
The context projection revision advances to v10 with an explicit lineage reset.
Schema 19, original attachment descriptors, stored bytes and old checkpoints are
unchanged; old summaries gain no invented visual facts.

## Used Context Composition

The public accounting contract describes one frozen request. Use
`observation.ReconcileContextStatus` for canonical and live observations. Keep
the last confirmed native input total, visible character distribution and recent
cache usage together until model, policy or successful compaction invalidates
that sample. Current request estimates remain Engine pressure inputs.

`TokenEstimate.Characters` and `RequestEstimate.Characters` use
`prepared_characters_v1` and `unicode_code_points`. The existing collector counts
decoded text without trimming or Unicode normalization, and deterministic JSON
for tool definitions, arguments and response schemas. Categories and optional
details add exactly to `TotalCharacters`; transport envelopes, protocol framing,
controls, ciphertext and binary/media proxies contribute no characters. Visible
media labels count normally. Hidden content records coverage limitations only.
Characters have no arithmetic relationship with provider tokens.

Reconciliation validates character units, counts and frozen request identity,
returns detached character data, and preserves trustworthy native totals when
optional attribution is invalid. Missing historical characters stay unavailable;
there is no reconstruction or token conversion. Render measured tokens with `=`,
visible content with `chars` and shares of visible characters. A measured zero is
available; output-only usage does not establish an input measurement.

Legacy token breakdown and remainder fields remain compatible budget diagnostics,
but are deprecated for content distribution. They are not a character fallback.
`TokenEstimate.Coverage=complete_request` still means complete preparation, not
complete attribution. Unknown models retain explicitly approximate vocabulary
estimates. Opaque proxies, margins and output reserves serve budgets only.

Adapters declare `Usage.InputTokensReported` and `InputTokenScope` from the
protocol (`request_input`, `provider_execution`, or `unknown`). Only qualified
request input with complete visible coverage can calibrate pressure. Historical
anchors without qualification, hosted/opaque execution usage and unknown scopes
cannot establish exact window occupancy. Existing estimates, bounded compaction
and explicit provider overflow recovery continue to govern execution. No online
count is added before sending.

`ContextStatus.PayloadFingerprint` identifies final sent bytes after output
refitting. Frozen retries preserve input and character statistics.
`Usage.OutputIncludesReasoning` and `TotalTokensReported` preserve protocol
relationships and native totals, including explicit zero, without double-counting
reasoning or cached input. Published token field meanings are unchanged.

## Managed Runtime Facts

`runtime.ModelContextSnapshot` contains complete `ModelContextSection` values
with stable `Key`, display `Title`, and full `Text`. Supply an optional snapshot
in `TurnInput.ModelContextSnapshot` for atomic user-input admission, and in
`ToolSurface.ModelContextSnapshot` at `provider_request` or `compact` boundaries
through `WithAgentDynamicToolSurface`. Use one host collector with the same
resolved permissions as the tool surface. Network retries replay frozen input.

Nil supplies no update; a non-nil empty collection clears all managed sections.
Missing keys remove prior facts. Floret copies, sorts unique keys, and rejects
more than 128 sections, 64,000 characters per section or 256 KiB of encoded data.
Facts cannot contain secrets or grant resource access. Existing `UserInput.Context`
keeps its message-attached semantics; managed snapshots are a separate contract.

One canonical owner appends changed full snapshots and projects a full baseline,
then changed sections and explicit removals as marked user-role context messages.
They never become user bubbles, title inputs or retry anchors. System text keeps
stable behavior rules. Each logical request applies steering, resolves the live
surface, admits changes, budgets/compacts and freezes rendering. Storage failure
prevents dispatch; physical retries replay frozen requests. Ordinary updates take
effect at the next request without interrupting generation.

Compaction stores the exact snapshot at its cut and replays retained updates in
order. That baseline consumes fixed input capacity and cannot be summarized or
dropped. Retry, fork and restart use the same projection. Domain migration
16 -> 17 preserves historical bytes and never infers sections from old prose;
the next live request admits a typed baseline. Old writers reject the upgraded
envelope in the existing physical namespace.

`ThreadContextSnapshot.ModelContext` exposes admitted facts, canonical position,
changed/removed keys and the latest request using that exact record. Hosts read
it through the existing authorized boundary and normal thread subscriptions.
Wrap required failures with `runtime.ErrModelContextUnavailable`; the terminal
`model_context_unavailable` failure releases execution and permits safe explicit
retry after repair. Confirmed effects remain committed and unknown effects remain
non-retryable.

## Consistent Reads

`ThreadService.View` returns one complete replaceable current view.
The thread runtime installs ThreadView.Context with canonical items before
publishing ViewVersion. Views and legacy subscriptions include a complete snapshot,
even when no context facts exist. Committed context changes publish while a Turn
is active; text deltas reuse the installed snapshot. Failed assembly returns an
error and the next explicit read retries, without blocking execution settlement
or cancellation. ThreadContextReader.Context uses that same projection.
ThreadContextCompaction.CanonicalOrdinal is the first lifecycle entry's one-based
position on the queried Path, independent of timestamps and terminal updates.
AfterItemID remains the insertion boundary; explicit Turn/Run identity defines
ownership. Forks reproject canonical order on the destination Path.

Compaction lifecycle records optionally expose a typed observation.CompactionDecision.
It records the actual frozen trigger candidate (even when never sent), the decision
pressure, and the validated replacement. Budget estimates, calibrated pressure and
message-context before/after values remain distinct measurements. Missing snapshots
are unavailable; manual preflight does not fabricate a request. The snapshot carries
only budget diagnostics and fingerprints, never raw prompts or source distributions.
The same canonical lifecycle supplies View, Context, reconnect and restart. Domain
schema 25 -> 26 preserves historical payloads and rejects older writers.


The value returned by `Host.ThreadService` also implements
`ThreadContextReader`; `Context` returns Floret's canonical context policy,
usage, and one latest lifecycle record per compaction operation, including
terminal state restored after runtime restart. The snapshot also exposes
conversation-wide disjoint input, output, cache-read, and cache-write totals
folded from canonical final provider usage records.
Forked snapshots retain historical Turn and Run identity while reporting the
destination Thread identity. Floret owns this projection and automatically
migrates affected v8 stores; hosts do not patch context records.
Each successfully committed final `provider_usage` runtime event also carries
`ThreadUsageTotals`. It is the live form of the same canonical fold; projected
requests, stream-only usage, rejected attempts, and failed writes omit it.
`ThreadContextSnapshot.ContextUsage` and `runtime.Event.ContextUsage` expose the
same committed `ThreadContextUsage`: `Confirmed` holds the latest native usage,
while `Estimate` holds the current prediction. Hosts can keep measurements stable
while separately showing request pressure. Matching model and budget policies
retain the measurement across Turns and restart; changed policies and successful
compaction clear it. Failed, cancelled, and noop compactions do not clear it.
Missing native usage updates only the estimate. Existing `Usage` and
`ContextStatus` fields retain their latest-status semantics.
The optional `ThreadWaiter.WaitThreads` observes a frozen scoped target set with
one positive timeout. Results distinguish `missing`, `attention_required`,
`completed` and `timed_out`, in that priority order. Timeout is a normal snapshot;
caller cancellation, read failure and Host shutdown remain errors. Opt-in pending
parent-event observation also returns canonical receipts with an `events` reason
without resuming idle parents. Subscriptions
wake a fresh authoritative read, including after deletion; they are not replay.
A tool may return `tools.BoundedWait` only for a full normal timeout with
unfinished, nonempty targets. The dispatcher measures that handler, excluding
approval and batch delay. Engine's single batch guard permits repetition only
when every call demonstrates a full bounded wait or changed polling progress.
Ordinary effects, early returns and errors retain the existing repeat limit.
`ThreadTurnFailure.Reason` derives safe loop-blocking classifications from the
canonical code and exact fixed Engine reason, including historical reads and
restart. Unknown failures have no guessed reason. No durable schema changes are
required; codes, retry savepoints and unknown-effect restrictions stay intact.

`ThreadService.Subscribe` publishes workspace summary and current-view updates;
reconnecting clients refresh summaries and the currently visible view. There is
no durable cursor, replay ledger, materialized projection, or second lifecycle
authority.

For streaming hosts, the same service implements optional
`runtime.ThreadUpdateSource` without adding methods to `ThreadService`.
Subscribe before reading `View`, discard updates through the snapshot version,
then apply only continuous `BaseVersion -> ViewVersion` batches. Item mutations
are explicit add, replace, remove, or UTF-8-byte-offset text append operations;
component wrappers distinguish unchanged from cleared values. Ordinary growth
is coalesced for at most 50ms, with immediate semantic boundaries and bounded
buffering. Snapshots flush pending growth to establish an exact baseline.
Overflow, canonical invalidation, or a version gap requires a fresh subscription
and authoritative view, never replay or inferred missing events. The old
subscription materializes complete isolated views only for existing legacy
consumers. See [native thread updates](okf/api/thread-updates.md).

Context failures carry distinct `ThreadTurnFailure` codes: `context_budget_invalid`
when output headroom leaves no input capacity, `context_fixed_overhead` when
non-compressible input cannot fit, and `context_compaction_limit` when a rebuilt
request remains too large. Invalid budgets and known fixed overhead fail before
automatic compression. Hosts should offer budget/model changes before explicit
retry; the same failure survives restart. These messages contain numeric budget
diagnostics, never prompt bodies or credentials.

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
        // Present migrating, verifying or indexing without exposing stored content.
    }),
})
```

The callback is synchronous and must return promptly. Legacy stores report
`migrating` followed by `verifying`. Missing derived indexes also report
`indexing` before the Host is exposed; current indexes are reused. A returned `Host` is ready, and a failed migration remains atomic.

Applications cannot use a Source as a lifecycle query path. Teams implementing
a physical backend use the advanced `storage/spi` contracts and their
conformance suite. SPI records remain opaque Floret data; a backend must not
decode them into a second Agent model. Memory, SQLite, and third-party backends
all run the same Floret-owned domain kernel.

Prompt-cache history is stored as append-ordered records per prompt scope and
category. Dispatch checkpoints commit only new segments, toolsets, requests,
and pending responses for the affected scopes; repeated request attempts retain
their original order. Turn settlement and shutdown flush only pending facts.
Compaction changes the effective model context independently of storage history.

SQLite physical format 3 adds source-locator metadata and contentless FTS5
postings. The contiguous 2 -> 3 edge runs in the same startup transaction.
Physical format 2 separates large record values from the primary-key
index using a rowid records table. `runtime.Open` converts format 1, splits the
legacy global prompt snapshot, migrates the session tree, updates logical
storage version 9, and verifies the result in one transaction. The session-tree
schema remains 20. Inspection reports physical upgrades as migration work and
reuses each merged namespace across pages without writing the source.

New SQLite stores use incremental auto-vacuum so deleted pages can be reclaimed
without rebuilding the database. A host that owns an older SQLite file may call
`storage.MaintainSQLite` before `runtime.Open`. The maintenance boundary checks
the exact Floret physical schema and database integrity, refuses an open
runtime, and uses SQLite's native `VACUUM` or `incremental_vacuum`; it never
copies records or exposes their contents.

### Coordinated storage startup and restore

Hosts with multiple stores can call `runtime.InspectSQLite` before any writable
open and `storage.BackupSQLite` while all writers are stopped. Inspection checks
the supported migration path in memory without creating a runtime Host. A backup
includes committed WAL records and never overwrites its destination.

Inspection retains complete canonical and projection validation while omitting
disposable text-search postings. `runtime.Open` verifies and reuses persistent derived postings before exposing
the Host, rebuilding only missing or supported derived state. Canonical mutations
and index updates share one transaction. SQLite uses the optional storage SPI
index capability; other backends use the generic KV adapter. Search bodies stay
canonical, and pagination snapshots stay bounded in memory.

Open with `runtime.Options{Storage: source, DeferExecution: true}` when product
stores or authorization must be prepared first. Reads and pending-input import
cannot start execution until `host.Activate(ctx)` succeeds. Default startup
remains automatic. `ErrExecutionDeferred` identifies commands attempted before
activation; `ErrStoreTooNew` identifies a newer logical storage contract.

For a restored snapshot, call `host.PrepareRestore(ctx)` on the staged, deferred
Host before activating or publishing the storage set. It stops unfinished turns,
resolves old interactions, and removes queued work from execution while exposing
its original input through `ThreadView.RestoredInputs`. `ErrRestoredTurn` prevents
retrying a stopped turn. Users must submit a new request under current authority.
Restore does not undo external side effects. The application owns consistent
multi-store snapshots, confirmation, retention, and recoverable file replacement.

## Source Of Truth

Floret exclusively owns admitted messages and references, thread/turn/run
lifecycle, titles, approvals, todos, tool invocation and outcome, pending-work
settlement, artifacts, control signals, context and compaction, provider
ledgers and state, prompt cache, SubAgent hierarchy, and Activity projections.

Hosts may persist product authorization and audit, routing, credentials,
editable persona sources, resource catalogs, read state, unadmitted commands,
upload staging, and transport diagnostics. Those records must not contain a
serialized Floret DTO or support reconstruction of Agent state. Canonical
message references and admitted runtime snapshots are durable model history.
Only material explicitly scoped to the current turn, such as secrets, belongs
in `SupplementalContext`; it never becomes history or continuation state.

## Shutdown

`Host.Shutdown(ctx)` stops admission, cancels Host-managed provider and tool
execution, waits for it to finish, and then closes storage. If the context
expires, Shutdown returns `ctx.Err()` and Host remains closing; a later call
continues waiting. After completion, every retained handle returns
`ErrHostClosed`.

## DeepSeek Responses

Use `provider.NewDeepSeek(provider.DeepSeekOptions{...})` for `deepseek-flash`
(DeepSeek V4.1 Flash 1017, text and images) and `deepseek-v4-pro` (text only).
The official `deepseek-v4-flash` and `deepseek-v4-flash-vision-exp` aliases
remain accepted with Flash capabilities; Floret preserves the selected ID and
opaque state identity. All support `off`, `low`, `high`, and `max` reasoning
with `high` as the default. Catalog ceilings are 1,000,000 context tokens and
393,216 output tokens; callers still choose their request budget.

Set `Model`, `BaseURL` (normally `https://api.deepseek.com`), `APIKey`, and an
explicit `StateCompatibilityKey`. The gateway calls `/responses` and streams
text and reasoning. Pro supports hosted `web_search`; Flash ignores hosted
search and uses the independent Search API. `provider.DeepSeekModelCapabilities`
returns the reviewed exact-model capability; unknown models are unavailable.
The gateway retains the published hosted-input and opaque replay contracts.

`tools/websearch` provides `NewDeepSeekClient`, `NewBraveClient` and `NewTool`.
Clients bind one host-selected endpoint and key; tools cannot select credentials
or sources. DeepSeek search validates 1–128 Unicode query characters and 1–50
results, defaults to 10, sends `need_content=false`, preserves model summaries
separately from UI snippets, and treats an empty result list as success.
`tools/webfetch.NewDeepSeek` is an explicit paid Markdown fetch alternative to
`webfetch.New` (Direct); it never falls back or invents page response metadata.
Both remote APIs use the connection key, bounded responses, cancellation, no
API redirects, and typed tool-service errors independently of model failures.
Search and Fetch currently cost $0.75 per 1,000 successful nonempty requests;
hosts must make activation explicit and must not infer billed totals from tokens.
See the [official Search/Fetch contract](https://api-docs.deepseek.com/guides/search/)
and the [web tools API guide](okf/api/websearch.md).

Web Activity preserves `Operation` (`search`, `open_page`, `find_in_page`),
`Query` (ordered queries separated by newlines), `URL`, `Pattern`, and source
`Title`, `URL`, and `Snippet`. Each source requires a URL; title and snippet are
optional provider facts and are never synthesized. `ResultsProvided` distinguishes an explicitly
returned empty list from unavailable details. An empty operation remains
unknown. Hosts render these facts from canonical Activity, including after
restart; answer citations are not per-call search results. These additive v7.4
contracts serve Redeven's shared Flower UI and other public Activity consumers.

Image support is a property of the selected model. Set the optional
`DeepSeekOptions.ResolveAttachment` callback to resolve an authorized opaque
`provider.Attachment` into image bytes. The gateway validates the model's image
capability and MIME type, freezes the expanded payload during `Prepare`, and
includes it in request estimates. Pure text models reject image input. The host
owns resource authorization; Floret never reads files or fetches attachment URLs.
Opaque state retains image references and content digests, never image bytes.
Subsequent requests resolve the content again under current host authorization.
This additive v7 option preserves existing descriptor-only callers.

Non-successful DeepSeek HTTP responses return `provider.ProviderHTTPError` with
the status code and sanitized provider error details. Context-overflow responses
continue to return `provider.ErrContextOverflow`.

DeepSeek is stateless. Floret retains provider-native response items in opaque
state, validates them against the canonical conversation, and returns the full
input history on each call, including search receipts and reasoning. Hosts must
not inspect or rebuild this state. Supplemental-context Turns retain their
existing no-continuation-state privacy boundary. Prepared estimates include the complete wire
payload using the offline official DeepSeek V4 tokenizer, 10% text headroom,
and 1024 tokens per actual image. These are conservative predictions, not native
usage. Canonical history fingerprints validate native calibration across Turns;
message ID reassignment does not invalidate it. See the
[estimator provenance and limits](scripts/deepseek-tokenizer/README.md).
Missing terminal events fail; truncated responses continue through the
normal runtime limit policy. The existing `NewOpenAICompatible` constructor
continues to select Chat Completions explicitly.


## Tool schema rendering order

Local tool input schemas render root properties in their existing `required` array
order, followed by remaining properties alphabetically. `tools.InputPropertyOrder`
is the single ordering rule; it does not mutate schemas or constrain model argument
order. `StrictObject` retains its existing alphabetically sorted required array.
Hosts that need a declaration order can supply that array directly.

Custom transports call `provider.OrderToolSchemas` after their final protocol encoding,
before estimation, fingerprinting and request freezing. It covers Chat Completions,
Responses (including namespace functions), Anthropic Messages and generic model-input
JSON. Hosted tools, nested schemas, historical messages and argument strings retain
their content. Invalid ordering declarations fail explicitly. Official adapters use
the same renderer for final requests and raw tool segments. Network retries preserve
frozen bytes; output-budget refits retain tool declarations.

The internal tool-schema rendering revision advances render lineage through the
existing provider-surface boundary. It stops old exact-envelope continuation reuse
without changing provider identity, canonical history, valid native history, cache
namespaces or domain schema 28. Historical rendered facts remain immutable.

## Model-aware request estimates

`provider.EstimateRenderedRequest` lets custom transports estimate the exact frozen
Chat Completions, Responses, or Anthropic Messages JSON they send. It counts model
input rather than HTTP controls or host metadata, including tool definitions and
structured-output schemas. Recognized OpenAI models use embedded official
cl100k/o200k vocabularies. Unknown models use an explicitly identified multilingual
BPE proxy; OpenAI-compatible transport alone does not select an OpenAI tokenizer.
Images use separate dimension/model budgets, while opaque files remain conservative.
`RequestFormatJSON` supports opaque host transports without claiming exact server
rendering. These offline predictions never replace native measurements. See
[provenance, coverage, and limitations](scripts/openai-tokenizer/README.md).

## Model metadata

Ollama hosts can use `provider.ParseOllamaReasoningCapability` with `/api/show`
metadata and `provider.OllamaReasoningEffort` for validated compatible requests.
Explicit `config.ReasoningLevelOn` is distinct from the omitted Default and from
effort levels; only declared toggle capabilities accept it.

The engine uses a generated, offline models.dev snapshot with official protocol
corrections. Hosts own credentials, enabled models, and discovery. Updating the
snapshot does not select a model for an existing conversation or add a public
catalog API. See [catalog maintenance](scripts/model-catalog/README.md).

## Development

Public API compatibility starts at the published **v7.16.2** release. That
release and compatible additions published afterward are protected within v7;
source/API compatibility with earlier releases is no longer promised. Consumers
upgrading from an older release must adopt the v7.16.2 API. Existing conversation
data continues through the permanent automatic domain-schema migration lineage.

```bash
GOWORK=off go test ./...
GOWORK=off go vet ./...
GOWORK=off go test -race ./...
scripts/check_v7_api_compatibility.sh
scripts/check_candidate_release_adoption.sh
```

Repository workflow and compatibility rules are defined in [AGENTS.md](AGENTS.md).
Architecture and maintenance knowledge lives in [okf/](okf/).

## Model input validation

Invalid tool arguments remain strict errors. The Engine records each rejected
call and its validation result and allows at most two regeneration opportunities.
Rejected calls cannot execute tools, request authorization, or open an Ask User
interaction. Technical feedback stays in canonical history and diagnostics while
Activity omits it. Exhausted control correction retains `control_error`; failures
inside control or Activity projectors and storage fail immediately.

Schema v10 adds durable context without rewriting existing journal entries or
provider request records. The automatic v9 to v10 edge preserves older migration
edges. Provider projection v8 establishes an explicit boundary for existing
threads: already saved references become visible; missing historical ephemeral
material is never reconstructed. Ordinary requests retain their canonical prefix;
compaction, canonical retry, projection upgrades, and execution configuration
changes use the existing observable request boundary.

### Immediate thread steering

Hosts may opt into `runtime.ThreadSteeringController`. `Steer` durably accepts an instruction and interrupts the current provider attempt or cancellable tool batch while preserving the same task and execution identity. `SteerQueued` atomically consumes an exact queued item. Cancelled steering carries a reason code (`stopped`, `effect_outcome_unknown`, `task_ended`, or `fork`); hosts may offer explicit resubmission after `stopped`, and must not automatically replay uncertain effects. Ordinary `Send` remains queued while a thread is active; steering requests are idempotent by request key and stale queue content returns `ErrRequestConflict`.


## Prepared content distribution

Prompt-cache plans retain source intervals from the current canonical projection,
including cache reuse and frozen network retries. Wire snapshots never own or persist
these transient attribution intervals.

The additive v7.47 API decision serves Redeven Flower. `TokenEstimate.Breakdown`
is produced by the same frozen-request extraction and vocabulary count as the safety
budget. `Message.Attribution` carries optional typed model-context/compaction source
spans; renderers map them to `RenderedRequest.Attribution` JSON Pointer paths.
These fields are observation metadata, excluded from model payloads. UTF-8 spans
partition decoded text; token ownership uses the first byte and never retokenizes
sources. Missing sources remain unattributed. Invalid optional maps suppress only
that distribution and emit the bounded `attribution_mismatch` diagnostic.

`ContextBreakdown.Tokenizer` identifies the actual vocabulary, independently of the
wire protocol and configured provider instance identity. `Quality=token_estimate`
allows approximate token amounts only with complete known text coverage;
`relative_weight` and the empty historical quality allow shares only, including
accessible labels and tooltips. `FramingTokens` is protocol markup, not native-usage
reconciliation. Display shares use the content-weight total, never scaled native
usage. Safety margins/media bounds remain in budgets. No new durable store or
lifecycle is introduced; the existing request identity, history and source-version
calibration invalidation continue to apply.

See [qualification evidence](scripts/context-qualification/REPORT.md) and
[unified counter](internal/requestestimate/content.go).

Optional ThreadSummaryReader reads canonical summaries without full history or
thread actor creation. Options.StartupWorkProgress reports real completed thread
projections and known totals; StartupPhaseIndexing identifies initial index work.

### Returned effect uncertainty

Set tools.Result.EffectOutcome to tools.EffectOutcomeUncertain only when a handler
returns a trustworthy error result whose external effect is unconfirmed. It requires
IsError, no DispatchErr, no Pending and no InputRequired. Floret commits the result
and archived output atomically, then lets the model inspect current state and decide
what to do. Do not blindly repeat an uncertain action. Repeated invocation identity
replays the committed result, and retry preserves the post-result boundary.
Crashes, panics, unsettled cancellation and result persistence failure retain the
terminal ErrEffectOutcomeUnknown contract. When a result commits before its batch
savepoint, retry retains the complete settled terminal path instead of an earlier
savepoint. Incomplete tool exchanges fail closed.

Schema 20 -> 21 preserves historical
facts and requires current writers.

Search and DeepSeek Fetch clients may use `ResolveAPIKey` to resolve live credentials immediately before dispatch. The callback must enforce current host authorization; its errors remain explicit and do not switch services. Credentials are never part of a tool definition or checkpoint.
