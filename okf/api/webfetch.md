---
type: Public API
title: tools/webfetch Package
description: The webfetch package provides one secure public-text HTTP and HTTPS tool.
resource: /tools/webfetch/webfetch.go
tags: [api, tools, web, security]
timestamp: 2026-08-27T00:00:00Z
---

# Summary

`tools/webfetch.New` returns the product-neutral `web_fetch` local tool. Its
provider arguments are only `url` and optional `format` (`markdown` or `text`).

# Fixed Behavior

The tool performs GET requests to HTTP or HTTPS on ports 80 and 443. It disables
environment proxies and automatic redirects, then revalidates the URL, every
redirect, every DNS answer, and the actual dial target. Local, private,
link-local, metadata, benchmark, encoded, reserved, and mixed public/private
targets fail closed. `AllowResolvedBenchmarkIPs` admits `198.18.0.0/15` only
when a domain resolves there; literal benchmark addresses remain blocked.

Requests time out after 30 seconds, follow at most five redirects, retain at
most 5 MiB of source data, and expose at most 64 KiB to the model. Supported
text MIME types are charset-decoded. Sanitized HTML becomes Markdown or plain
text; relative links use the final URL as their base. Binary data, JavaScript
rendering, authentication, custom headers, and non-GET requests are outside the
contract.

The Activity projection includes at most 2,000 Unicode characters of sanitized
content. For HTML, the same fetch path may make one best-effort icon retrieval:
the first same-origin `rel=icon` target or same-origin `/favicon.ico`. The icon
has a two-second sub-timeout and 8 KiB limit, follows only same-origin validated
redirects, and accepts PNG, JPEG, WebP, or ICO bytes. SVG and other active or
unsupported content fail closed without failing the page read.

# Host Boundary

Hosts bind `Options.Permission` and `Options.PermissionFor` to current product
authorization. They decide visibility and render `WebFetchActivityPayload`.
That payload contains response metadata, errors, the lightweight preview, and
optional passive icon. Hosts render those bytes directly and never refetch a
remote icon URL. Complete content remains in the tool result and artifact.

# Key Source Files

* [Tool Contract](/tools/webfetch/webfetch.go)
* [Network Policy](/tools/webfetch/network.go)
* [Content Processing](/tools/webfetch/content.go)
