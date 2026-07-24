# Maintainer Workflows

## Downstream Adoption

* [Compose a Durable Host](compose-durable-host.md) - Build the composition root and distribute only authority-bound capabilities.
* [Integrate a Model Gateway](integrate-model-gateway.md) - Keep transport and credentials in the host while Floret owns the model loop.
* [Authorize a Tool Effect](authorize-tool-effect.md) - Connect product approval policy to Floret's generic effect lifecycle.
* [Maintain a SQLite Store](maintain-sqlite-store.md) - Inspect, verify, plan, and explicitly apply Store migration.
* [Recover an Interrupted Turn](recover-interrupted-turn.md) - Reconcile one exact interrupted authority without heuristic repair.
* [Render a Turn Projection](render-turn-projection.md) - Combine transient live updates with canonical durable reloads.

## Repository Changes

* [Change Public API](change-public-api.md) - Update public packages without leaking implementation contracts.
* [Add a Tool](add-tool.md) - Add local tool capability while preserving schema, permission, and observation boundaries.
* [Add a Provider](add-provider.md) - Add provider support without provider-name special cases in capability selection.
* [Quality Gate](quality-gate.md) - Required checks before integration.
