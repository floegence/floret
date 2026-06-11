// Package event defines presentation-neutral runtime events.
//
// Event values produced by the engine may contain raw provider text, reasoning,
// tool arguments, tool results, artifact paths, and diagnostics. Public sinks,
// trace files, SSE streams, API responses, and host snapshots should pass events
// through Sanitize or SerialSink with the default policy. Raw inspection is a
// local debug capability: enabling SinkPolicy.AllowRaw on one sink must not be
// reused for default/public sinks in the same run.
//
// Recorder is safe for concurrent Emit calls and is intended for tests. Custom
// sinks should either be internally synchronized or wrapped in SerialSink.
package event
