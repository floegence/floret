# Security Policy

## Report a Vulnerability

Please do not report suspected vulnerabilities in a public issue, discussion,
or pull request. Use GitHub's private vulnerability reporting for this
repository:

<https://github.com/floegence/floret/security/advisories/new>

If private reporting is unavailable, open a minimal issue asking the
maintainers for a private contact channel. Do not include vulnerability details
in that issue.

Include the affected Floret version or commit, the security impact, and the
smallest reproducible case you can provide. Remove credentials, prompts,
conversation content, provider state, tool output, and SQLite data from all
diagnostics. The maintainers will coordinate validation, remediation, and
disclosure through the private report.

## Project Boundary

Reports about Floret's agent runtime, public integration contracts, or owned
durable state belong here. Product credentials, deployment policy, concrete
tools, resource authorization, routing, and user interface behavior belong to
the downstream host and should be reported to that project instead.
