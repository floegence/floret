---
type: decision
---

# Managed model context

Floret owns managed runtime facts. A host supplies complete snapshots at admission
and logical request boundaries through [runtime contracts](../api/runtime.md)
and [runtime/model_context.go](../../runtime/model_context.go). Nil supplies no
update; empty clears facts; stable keys define complete replacement and removal.
Validation rejects malformed or oversized input.

Canonical records store full snapshots only. The
[session projector](../../internal/session/provider_history.go) derives user-role
baseline/change messages. They are not user requests, title inputs, retry anchors
or authorization. [Engine admission](../../internal/engine/model_context.go)
precedes budgets and request freezing. Physical retries replay frozen input.

[Compaction](../../internal/session/compaction/compaction.go) stores the exact
snapshot at its cut, then applies retained updates in order. Live and reopened
projections must match. Baselines consume fixed capacity. Retry retains admitted
facts without replaying superseded execution; forks inherit only their selected
path. Confirmed effects and unknown-effect terminal rules remain unchanged.

The [16 -> 17 migration](../../internal/sessiontree/backend_domain_v17_migration.go)
validates source authority without rewriting facts or inferring sections from
prose. A new envelope in the existing namespace makes old writers reject an
upgraded store. Every prior migration remains supported.

Hosts collect once with resolved permissions. They do not keep another current
state, admission ledger or provider history. Authorized context reads expose the
canonical position and existing request ledger usage. Required failures settle
normally with `model_context_unavailable` and allow safe explicit retry.
