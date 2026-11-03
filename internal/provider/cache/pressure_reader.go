package cache

import "context"

// PressureEvidence contains the original records for the engine's measurement
// validator; it does not introduce another durable measurement authority.
type PressureEvidence struct {
	Anchor   PressureAnchorState
	Request  ProviderRequestRecord
	Response *ProviderResponseRecord
}

type PressureEvidenceReader interface {
	ReadPressureEvidence(context.Context, string, string, string) (PressureEvidence, bool, error)
}

func ReadPressureEvidence(ctx context.Context, store Store, scope, provider, model string) (PressureEvidence, bool, error) {
	if err := ctx.Err(); err != nil {
		return PressureEvidence{}, false, err
	}
	if reader, ok := store.(PressureEvidenceReader); ok {
		return reader.ReadPressureEvidence(ctx, scope, provider, model)
	}
	anchor, found, err := store.LatestPressureAnchor(ctx, scope, provider, model)
	if err != nil || !found {
		return PressureEvidence{}, false, err
	}
	requests, err := store.ProviderRequests(ctx, scope)
	if err != nil {
		return PressureEvidence{}, false, err
	}
	var response *ProviderResponseRecord
	if anchor.MeasurementPrefix != (PressureAnchorState{}).MeasurementPrefix {
		responses, err := store.ProviderResponses(ctx, scope)
		if err != nil {
			return PressureEvidence{}, false, err
		}
		for i := len(responses) - 1; i >= 0; i-- {
			if responses[i].PressureAnchor == anchor {
				response = &responses[i]
				break
			}
		}
		if response == nil {
			return PressureEvidence{}, false, nil
		}
	}
	for i := len(requests) - 1; i >= 0; i-- {
		if requests[i].ID == anchor.RequestID && (response == nil || requests[i].AttemptID == response.AttemptID) {
			return PressureEvidence{Anchor: anchor, Request: requests[i], Response: response}, true, nil
		}
	}
	return PressureEvidence{}, false, nil
}

func (s *PreparedStore) ReadPressureEvidence(ctx context.Context, scope, provider, model string) (PressureEvidence, bool, error) {
	return ReadPressureEvidence(ctx, s.base, scope, provider, model)
}
func (s *executionStore) ReadPressureEvidence(ctx context.Context, scope, provider, model string) (PressureEvidence, bool, error) {
	return ReadPressureEvidence(ctx, s.Store, scope, provider, model)
}
