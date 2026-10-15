---
type: decision
---

# Provider request recovery

The Engine owns temporary model failure recovery through one bounded in-memory
retry loop, also used by compaction batches and automatic titles. Hosts classify
protocol facts and render [runtime progress](../api/runtime.md); they must disable
SDK retries and must not simulate recovery with canonical manual Retry.

Each logical request keeps its Step, ThreadID, TurnID, RunID, PromptScopeID and
LogicalRequestID. Physical attempts have new AttemptID/Epoch values, admission,
request observations and resource cleanup. Backoff holds no admission slot or
storage transaction. Confirmed local tools are outside this retry boundary.

Defaults allow four extra attempts (at most three timeout retries), jittered
2/4/8/10-second delays and a five-minute window beginning at the first failure.
An absolute server deadline is never shortened. Server waits over one minute,
authentication, billing, contract, unknown-effect and unclassified errors end
recovery. Parent cancellation and deadlines interrupt requests and waits.

The Engine status owns the execution outcome. A Provider request timeout or
recovery-window deadline remains a Provider failure while its execution context
is active, even when the error chain matches a context sentinel. The Harness
maps that typed outcome once; runtime presentation and cancellation diagnostics
must not reclassify it from the error text or chain. Before Engine execution,
preparation cancellation requires the actual execution context to be canceled.
Persistence settlement uses its own bounded context and reports storage failure
without replacing it with user cancellation. Unknown effects remain terminal
failures, and a committed terminal always retains authority.

Failed streaming drafts are retired immediately when recovery is scheduled.
The canonical projection and current view reject late events from that attempt;
committed conversation and confirmed tools remain. Exhaustion retains only the
last incomplete attempt under the failed turn. Retry counters and deadlines are
live memory, never a durable retry queue or separate lifecycle.

[Provider contracts](../api/provider.md) carry typed advice and optional frozen
prepared-request replay. Hosted tools require explicit ReplaySafe permission.
Schema 16 and its complete migration lineage are unchanged.
