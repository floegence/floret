package contextpolicy

// RequestBudget is the sole calculation of request capacity and compaction pressure.
// Legacy policies retain their published reservation semantics at this boundary.
type RequestBudget struct {
	OutputTokens, SafetyMarginTokens, ThresholdTokens, RequestSafeLimit, HeadroomTokens int64
	CompactionNeeded, HardLimitExceeded, Invalid                                        bool
}

// SafetyMargin is shared by ordinary and summary requests.
func SafetyMargin(window int64) int64 { return max(0, min(window/100, 4096)) }

func ResolveRequestBudget(policy Policy, input int64, minimumOutput ...int64) RequestBudget {
	p := Normalize(policy)
	b := RequestBudget{OutputTokens: p.MaxOutputTokens}
	minimum := int64(1)
	if len(minimumOutput) > 0 {
		minimum = max(minimum, minimumOutput[0])
	}
	if p.ModelMaxOutputTokens > 0 {
		b.SafetyMarginTokens = SafetyMargin(p.ContextWindowTokens)
		required := minimum
		if p.MaxOutputTokens > 0 {
			required = p.MaxOutputTokens
		}
		b.Invalid = p.MaxOutputTokens < 0 || required < minimum || required > p.ModelMaxOutputTokens
		b.RequestSafeLimit = p.ContextWindowTokens - b.SafetyMarginTokens - required
		b.HeadroomTokens = b.SafetyMarginTokens + required
		if p.MaxOutputTokens == 0 {
			b.OutputTokens = max(0, min(p.ModelMaxOutputTokens, p.ContextWindowTokens-input-b.SafetyMarginTokens))
		}
		b.HardLimitExceeded = b.Invalid || input > b.RequestSafeLimit
	} else {
		b.HeadroomTokens = p.MaxOutputTokens
		if b.HeadroomTokens <= 0 {
			b.HeadroomTokens = p.ReservedOutputTokens
		}
		b.RequestSafeLimit = p.ContextWindowTokens - b.HeadroomTokens
		b.HardLimitExceeded = input >= b.RequestSafeLimit
	}
	b.Invalid = b.Invalid || b.RequestSafeLimit <= 0
	b.ThresholdTokens = max(1, min(RatioLimitTokens(p), b.RequestSafeLimit))
	b.CompactionNeeded = input >= b.ThresholdTokens
	return b
}
