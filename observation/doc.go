// Package observation defines host-facing runtime observation DTOs.
//
// Runtime events are presentation-neutral facts, and those facts may still be
// too low-level for a host UI or API response. Package observation projects
// selected events and narrow observation DTOs into stable, UI-neutral status
// records that a host can render without parsing assistant text, exposing
// prompt-cache storage records, or depending on internal inspection types.
//
// Observation values are not raw debug records. They intentionally omit provider
// payloads, model deltas, reasoning, and raw tool arguments and results. Display
// text and metadata omit local paths. Explicit ActivityTargetRef navigation
// paths and URIs remain resource facts; hosts must authorize access before
// opening them. Diagnostic trace sinks redact navigation paths separately.
package observation
