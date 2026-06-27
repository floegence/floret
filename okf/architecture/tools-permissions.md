---
type: Architecture Concept
title: Tools and Permissions
description: Floret separates provider-visible tool definitions, local tool dispatch, effects, resources, approvals, and hosted provider tools.
resource: /tools/doc.go
tags: [architecture, tools, permissions]
timestamp: 2026-06-20T00:00:00Z
---

# Summary

Local tools are registered through `tools.Registry`. Registration validates
names, schemas, effects, permission modes, and parallel-safety claims before
tools are exposed to a provider.

# Local Tool Contracts

Each local tool has:

* a provider-visible `ToolDefinition`;
* strict input schema validation;
* typed invocation data;
* declared effects such as read, write, shell, or network;
* resource extraction for approval and observation;
* an output policy for visible and preserved output.

Read-only safe tools may default to allow. Riskier tools must explicitly ask,
allow, or deny. Open-world and destructive tools require careful permission
declarations.

# Hosted Tool Boundary

Hosted provider tools are provider-native capabilities. They are not dispatched
by the local tool runtime and must not be treated as ordinary local handlers.

# Dynamic Tool Surfaces

`runtime.ToolSurfaceProvider` is the public host hook for changing the active
tool surface during a run. The hook returns product-neutral data: a registry or
explicit local tool definitions, hosted tool definitions, system prompt text,
host context, and audit metadata. Floret refreshes that surface before provider
requests and again before local tool dispatch, so provider-visible capabilities
and executable local capabilities converge at safe points.

Product permission modes stay in the host. Floret only sees the projected
registry, tool definitions, hosted tools, and prompt/context text. A stale model
tool call cannot bypass a newer host policy because dispatch uses the refreshed
registry and the same resource, effect, permission, and approver lifecycle as
ordinary tool calls.

# Pending Tool Work

A tool may return a pending result when the host starts work whose lifecycle
continues outside Floret. The host later completes that work through the public
runtime facade.

# Key Source Files

* [Tools Package](/tools/tools.go)
* [Tool Invocation](/tools/invocation.go)
* [Permissions](/tools/permission.go)
* [Pending Results](/tools/pending.go)
