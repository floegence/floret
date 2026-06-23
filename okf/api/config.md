---
type: Public API
title: config Package
description: The config package resolves provider, model, prompt identity, skill configuration, loop defaults, and context policy.
resource: /config/config.go
tags: [api, config]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

`config` is the public package for building or loading Floret runtime
configuration. Hosts may construct `config.Config` directly or use `config.Load`
to read environment-backed defaults.

# Responsibilities

* Resolve provider and model defaults.
* Load environment values when requested.
* Resolve agent profile and prompt identity.
* Normalize prompt-cache retention.
* Normalize context policy defaults.
* Normalize provider-neutral reasoning selection from `Config.Reasoning`,
  `FLORET_REASONING_LEVEL`, and `FLORET_REASONING_BUDGET_TOKENS`.
* Validate required provider settings.

# Reasoning Selection

`Config.Reasoning` is the default reasoning request intent for Floret-managed
runs. It carries a provider-neutral level and optional budget tokens. Provider
adapters validate that intent against the selected model capability before
rendering provider-specific request fields.

# Use With

* [runtime](runtime.md) when constructing `runtime.Host` or calling
  `runtime.RunProjectedTurn`.
* [Observation](observation.md) when interpreting context pressure DTOs.

# Key Source Files

* [Config](/config/config.go)
* [Context Policy](/config/context.go)
