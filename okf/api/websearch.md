---
type: Public API
title: Web Search and Remote Fetch
description: Explicit authenticated web services with canonical tool observations.
resource: /tools/websearch/client.go
tags: [api, tools, web, providers]
timestamp: 2026-10-19T00:00:00Z
---

# Ownership

The additive public `tools/websearch` clients and `tools/webfetch.NewDeepSeek`
serve Redeven and other authorized hosts. Floret owns request validation,
transport, bounded output and canonical Activity; hosts own service selection,
credentials, permissions and billing disclosure. No model-selected routing,
source fallback, provider retry classification or new durable lifecycle exists.

# Search

`NewDeepSeekClient` and `NewBraveClient` produce one immutable client binding.
`NewTool` wraps that client with host permission policy and optional tool name.
DeepSeek validates Unicode query length (1–128), count (1–50, default 10), optional
original question and IP localization. Tools default to no page content.
Summary goes to model text; snippet goes to Activity. Source title, URL and
optional metadata stay factual. Empty results are successful. All external
content is untrusted. Brave uses its native fields and the same HTTP implementation
as the existing internal builtin; no separate HTTP owner remains.

# Remote Fetch

`webfetch.NewDeepSeek` exposes `web_fetch` with URL and Markdown-only format.
It validates public URLs using the existing admission policy before sending them
to the remote service. Remote redirects are the supplier's boundary, not a local
validation claim. Only requested URL, title, Markdown content and format are
returned; no target status, final URL, MIME or bytes are invented. Direct's
constructor and security contract remain unchanged.

# Failure and Recovery

Search times out after 20 seconds, Fetch after 30, both respecting shorter context
deadlines. Responses over 5 MiB and malformed JSON fail. HTTP client redirects are
disabled even for supplied clients. HTTPError is a sanitized tool-service error,
not provider.ProviderHTTPError. There is no automatic retry or source switch.
Tools settle through the existing result/Activity path; Stop cancels HTTP work.

Hosts can store non-secret service-binding annotations in canonical first Turn
checkpoints and recover from InitialProviderSurface, independently of live
permission changes. Secrets are never annotations, prompts or durable results.

See [Search client](../../tools/websearch/client.go),
[tool](../../tools/websearch/tool.go), [Fetch](../../tools/webfetch/deepseek.go),
[capabilities](../../provider/deepseek_capability.go) and
[official contract](https://api-docs.deepseek.com/guides/search/).
