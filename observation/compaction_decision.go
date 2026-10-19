package observation

import (
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/config"
)

// CompactionRequestSnapshot describes a frozen candidate, which need not have
// been sent. Estimate and Pressure have different scopes when calibration applies.
// It contains budget diagnostics, never request content or character distribution.
type CompactionRequestSnapshot struct {
	PayloadFingerprint   string                 `json:"payload_fingerprint"`
	Provider             string                 `json:"provider"`
	Model                string                 `json:"model"`
	Estimate             config.RequestEstimate `json:"estimate"`
	Pressure             config.ContextPressure `json:"pressure"`
	CalibrationRequestID string                 `json:"calibration_request_id,omitempty"`
}

// CompactionDecision retains the trigger candidate and validated replacement in
// the canonical lifecycle. Missing candidates are unavailable, including
// historical records and manual preflight without a prepared request.
type CompactionDecision struct {
	Pressure config.ContextPressure     `json:"pressure"`
	Before   *CompactionRequestSnapshot `json:"before,omitempty"`
	After    *CompactionRequestSnapshot `json:"after,omitempty"`
}

// Clone returns an independently mutable copy.
func (d *CompactionDecision) Clone() *CompactionDecision {
	if d == nil {
		return nil
	}
	out := *d
	clone := func(in *CompactionRequestSnapshot) *CompactionRequestSnapshot {
		if in == nil {
			return nil
		}
		v := *in
		v.Estimate.Characters = in.Estimate.Characters.Clone()
		if in.Estimate.Breakdown.Details != nil {
			details := *in.Estimate.Breakdown.Details
			details.Items = append([]config.ContextTokenDetail(nil), details.Items...)
			v.Estimate.Breakdown.Details = &details
		}
		return &v
	}
	out.Before, out.After = clone(d.Before), clone(d.After)
	return &out
}

// Validate checks recorded facts without recalculating their policy or scope.
func (d *CompactionDecision) Validate() error {
	if d == nil {
		return nil
	}
	if d.Before == nil && d.After == nil {
		return fmt.Errorf("compaction decision requires a candidate")
	}
	if err := validateCompactionPressure(d.Pressure); err != nil {
		return err
	}
	for _, s := range []*CompactionRequestSnapshot{d.Before, d.After} {
		if s == nil {
			continue
		}
		if strings.TrimSpace(s.PayloadFingerprint) == "" || len(s.PayloadFingerprint) > 512 || strings.ContainsAny(s.PayloadFingerprint, "\r\n\x00") {
			return fmt.Errorf("compaction candidate requires a valid fingerprint")
		}
		if s.Provider == "" || s.Model == "" || s.Estimate.Source == "" || s.Estimate.Method == "" {
			return fmt.Errorf("compaction candidate requires model and estimator identity")
		}
		if err := validateCompactionPressure(s.Pressure); err != nil {
			return err
		}
		e, p := s.Estimate, s.Pressure
		if e.EstimatedInputTokens < 0 || e.PrefixTokens < 0 || e.MessageTokens < 0 || e.ToolDefinitionTokens < 0 {
			return fmt.Errorf("invalid compaction candidate budget")
		}
		if e.Characters != nil || e.Breakdown.Details != nil {
			return fmt.Errorf("compaction decision excludes content distribution")
		}
		if (p.Source == config.PressureSourceUsageAnchoredDelta) != (s.CalibrationRequestID != "") {
			return fmt.Errorf("compaction calibration requires its measured request")
		}
	}
	return nil
}

func validateCompactionPressure(p config.ContextPressure) error {
	if p.ProjectedInputTokens < 0 || p.WindowInputTokens < 0 || p.RequestOutputTokens < 0 || p.SafetyMarginTokens < 0 || p.ContextWindowTokens <= 0 || p.ThresholdTokens <= 0 || p.RequestSafeLimit <= 0 {
		return fmt.Errorf("invalid compaction pressure budget")
	}
	return nil
}
