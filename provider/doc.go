// Package provider defines the normalized model streaming contract.
//
// Provider implementations must make Stream safe for concurrent calls. Each
// returned stream must emit zero or more non-terminal events followed by exactly
// one terminal event: Done, Empty, or Truncated. Streams must not emit events
// after the terminal event, must close promptly when the context is cancelled,
// and must return complete unique tool call IDs. Hosted tool result events must
// match a prior hosted tool call in the same stream. Unknown event types should
// be treated as provider contract failures.
//
// StreamValidator captures these invariants for adapters and engine tests.
package provider
