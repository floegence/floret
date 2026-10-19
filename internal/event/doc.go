// Package event defines presentation-neutral runtime events.
//
// Event values produced by the engine may contain raw provider text, reasoning,
// tool arguments, tool results, artifact paths, and diagnostics. Diagnostic
// sinks and trace files use Sanitize or SerialSink with the default policy.
// Canonical tool presentation and host UI observations preserve explicit
// ActivityTargetRef navigation facts separately from sanitized display text.
// Hosts authorize access to those resources. PreserveActivityNavigation never
// enables raw event text; AllowRaw remains a separate local debug capability
// that must not be reused for default/public sinks in the same run.
//
// Recorder is safe for concurrent Emit calls and is intended for tests. Custom
// sinks should either be internally synchronized or wrapped in SerialSink.
package event
