---
type: Architecture Concept
title: Observation and Events
description: Floret emits presentation-neutral runtime events and exposes sanitized observation DTOs for hosts.
resource: /observation/doc.go
tags: [architecture, observation, events]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Events are the engine and harness fact stream. Important decisions, retries,
compactions, approvals, provider requests, tool outcomes, and control signals
must be observable through events or testable state.

# Sanitized Observation

Public sinks and host-facing projections use sanitized events. Observation DTOs
are intentionally narrower than raw debug records and omit provider payloads,
reasoning, tool arguments, tool results, and local paths.

# Main Projections

* Activity timelines summarize tool, hosted-tool, approval, control, and budget
  state.
* Runtime stream observations expose provider-neutral model output facts,
  including text deltas, reasoning deltas, retry/finish signals, and model
  tool-call stream start/delta/end facts. Model tool-call stream facts identify
  the call but do not expose argument text; local tool execution remains a
  separate activity timeline concern.
* Context statuses show projected and provider-reported context pressure.
* Compaction events expose context compaction lifecycle.

# Key Source Files

* [Observation Package](/observation/doc.go)
* [Activity Timeline](/observation/activity.go)
* [Context Observation](/observation/context.go)
* [Event Sanitization](/internal/event/event.go)
