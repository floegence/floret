package tools

// ArgumentPreview is display-only information derived from unvalidated model
// arguments. Nil counts are unknown; a non-nil zero is an observed zero. It must
// never be used as permission, resource, effect, or execution authority.
type ArgumentPreview struct {
	DisplayName string `json:"display_name,omitempty"`
	Additions   *int   `json:"additions,omitempty"`
	Deletions   *int   `json:"deletions,omitempty"`
}

// ArgumentPreviewer consumes append-only argument fragments for one call. The
// final notification may have an empty delta and does not imply schema validity.
// Implementations must be bounded, memory-only and free of side effects. An error
// or panic discards this preview without affecting validation or execution.
type ArgumentPreviewer func(delta string, complete bool) (*ArgumentPreview, error)

// Clone returns detached preview data.
func (p *ArgumentPreview) Clone() *ArgumentPreview {
	if p == nil {
		return nil
	}
	out := *p
	if p.Additions != nil {
		n := *p.Additions
		out.Additions = &n
	}
	if p.Deletions != nil {
		n := *p.Deletions
		out.Deletions = &n
	}
	return &out
}
