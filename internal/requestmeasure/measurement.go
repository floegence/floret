// Package requestmeasure defines frozen protocol boundaries, never token policy.
package requestmeasure

// Prefix identifies an ordered prefix of complete frozen protocol input. Its
// fingerprint includes the fixed input surface and every covered protocol item.
// ExecutionIdentity binds the provider account/connection; it must contain no secrets.
type Prefix struct {
	ExecutionIdentity string `json:"execution_identity"`
	Format            string `json:"format"`
	Fingerprint       string `json:"fingerprint"`
	ItemCount         int    `json:"item_count"`
	Boundary          string `json:"boundary"`
}

// Evidence is native protocol evidence for exactly one successful attempt.
// CompleteInput covers the entire submitted input, including cached input.
// OpaqueInput includes encrypted reasoning only with explicit protocol evidence.
// ResponseEnd requires a complete terminal output matching ResponseID and the
// native items that will actually be replayed. It is a conservative output budget.
type Evidence struct {
	AttemptID     string `json:"attempt_id"`
	CompleteInput bool   `json:"complete_input"`
	OpaqueInput   bool   `json:"opaque_input,omitempty"`
	Input         Prefix `json:"input"`
	ResponseID    string `json:"response_id,omitempty"`
	ResponseEnd   Prefix `json:"response_end,omitzero"`
}

// Prepared reads frozen bytes only. Suffix returns complete protocol input
// containing only items after the matching prefix, without fixed input already
// measured. A mismatch must return false; it must never reread external resources.
type Prepared interface {
	InputMeasurementPrefix() Prefix
	MeasurementSuffix(Prefix) ([]byte, bool)
}

func (p Prefix) Valid() bool {
	return p.ExecutionIdentity != "" && p.Format != "" && p.Fingerprint != "" && p.ItemCount >= 0 && (p.Boundary == "input" || p.Boundary == "response_end")
}
