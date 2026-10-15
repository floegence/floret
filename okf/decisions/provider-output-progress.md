---
type: Decision
title: Provider Output Progress
description: Received output estimates and unvalidated argument previews are transient observation.
resource: /runtime/provider_progress.go
tags: [runtime, tools, observation]
---

# Provider Output Progress

The Engine counts only accepted received text, reasoning and local tool argument
fragments. ASCII characters contribute one quarter token and non-ASCII Unicode
characters contribute one and a half. UTF-8 tails are isolated by stream; final
call arguments are counted only when no argument bytes were streamed. These
estimates never enter provider Usage, budgets, prompt cache accounting or storage.

The thread actor owns `ThreadRunProgress.Request` timing and the current
`ThreadView.ToolCallPreviews`. Existing exact request-attempt fences discard late
updates. Request progress resets on the next request; previews survive a matching
admission or approval until actual results, and disappear on replacement or turn
settlement. Hosts interpolate elapsed time locally; no backend timer publishes
views merely because another second elapsed.

Tools optionally supply `tools.Definition.NewArgumentPreview`, creating a bounded
memory-only parser for one call. Its append-only input remains unvalidated and
must not authorize, read resources, invoke handlers or produce irreversible
side effects. Only safe display names and optional nonnegative line counts cross
the observation boundary. Parser errors and panics clear display data without
changing validation or dispatch. Existing sanitization applies to display names.

Previews never become canonical Items or reserve Ordinal values. A host may reuse
one row for a preview and its exact admitted CallID. Authoritative result counts
replace the preview, including zero; failure must not present an unconfirmed
preview as a completed edit. Canonical refresh preserves still-active previews;
restart rebuilds only durable facts and never replays these observations.

See [the public runtime boundary](/README.md#runtime-boundary),
[the API change workflow](../workflows/change-public-api.md) and
[the tool contracts](/tools/argument_preview.go).

`ProviderRequestProgress.LastProgressAt` is the detached timestamp authored by the
same attempt owner that enforces request deadlines. Hosts may show waiting time
while retaining received token estimates; display timers never cancel requests
or decide retries. Native hosted completion restores the waiting-response phase.
