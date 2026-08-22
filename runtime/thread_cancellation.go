package runtime

import (
	"context"
	"time"

	"github.com/floegence/floret/v4/identity"
)

// threadCancellationDiagnostic is intentionally internal. It records the
// owner of an execution cancellation without widening the public runtime API.
type threadCancellationDiagnostic struct {
	ThreadID             identity.ThreadID
	TurnID               identity.TurnID
	RunID                identity.RunID
	Source               string
	Reason               string
	At                   time.Time
	EntryCancelRequested bool
}

func (service *threadRuntimeService) recordCancellation(threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, source, reason string) {
	if service == nil {
		return
	}
	entries, err := service.host.store.repo.Entries(context.Background(), threadID.String())
	record := threadCancellationDiagnostic{
		ThreadID: threadID,
		TurnID:   turnID,
		RunID:    runID,
		Source:   source,
		Reason:   reason,
		At:       time.Now().UTC(),
	}
	if err == nil {
		record.EntryCancelRequested = turnHasCancelRequest(entries, turnID)
	}
	service.cancellationMu.Lock()
	service.cancellations = append(service.cancellations, record)
	service.cancellationMu.Unlock()
}

func (service *threadRuntimeService) cancellationDiagnostics() []threadCancellationDiagnostic {
	if service == nil {
		return nil
	}
	service.cancellationMu.Lock()
	defer service.cancellationMu.Unlock()
	return append([]threadCancellationDiagnostic(nil), service.cancellations...)
}
