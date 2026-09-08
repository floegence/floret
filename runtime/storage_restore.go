package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

const restoredFromBackupMetadata = "restored_from_backup"

// ErrRestoredTurn requires the caller to submit a new user request instead of
// retrying execution whose side effects may have occurred after the snapshot.
var ErrRestoredTurn = errors.New("restored turn cannot be retried; submit a new request")

// RestoredInput preserves a cancelled queue input as a read-only canonical
// projection. It is never an executable queue item or authorization evidence.
type RestoredInput struct {
	ID    string    `json:"id"`
	Input UserInput `json:"input"`
}

// RestorePreparation reports changes made while preparing an offline restore.
type RestorePreparation struct {
	StoppedThreads     int `json:"stopped_threads"`
	StoppedQueueInputs int `json:"stopped_queue_inputs"`
}

// PrepareRestore ends unfinished execution and queued work in a restored
// snapshot. It requires a deferred Host and is idempotent across interruption.
// Use it on the staged snapshot, before publishing any restored storage set.
// Historical messages remain unchanged and deleted queue inputs remain visible
// in ThreadView.RestoredInputs. Existing approval decisions cannot resume work.
func (host *Host) PrepareRestore(ctx context.Context) (RestorePreparation, error) {
	var result RestorePreparation
	if ctx == nil || host == nil {
		return result, errors.New("restore preparation requires a Host and context")
	}
	host.maintenanceMu.Lock()
	defer host.maintenanceMu.Unlock()
	if !errors.Is(host.requireExecution(), ErrExecutionDeferred) {
		return result, errors.New("restore preparation requires deferred execution")
	}
	if err := host.available(); err != nil {
		return result, err
	}
	service := host.threadRuntimeForFence()
	threads, err := sessiontree.ListThreads(ctx, host.store.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return result, err
	}
	for _, meta := range threads {
		threadID := identity.ThreadID(meta.ID)
		view, err := service.View(ctx, threadID)
		if err != nil {
			return result, err
		}
		if view.Activity == ThreadActivityActive {
			key := "restore-cancel:" + view.TurnID.String()
			fingerprint, _ := stableFingerprint(view.TurnID)
			if _, err = service.settleCancellation(ctx, service.runtime(threadID), threadID, view.TurnID, view.RunID, cancellationRequest{EntryID: key, RequestKey: key, RequestFingerprint: fingerprint}, "snapshot_restore", "execution stopped after snapshot restore"); err != nil {
				return result, err
			}
			result.StoppedThreads++
		}
		for _, queued := range view.Queue {
			payload, err := json.Marshal(queued.ID)
			if err != nil {
				return result, err
			}
			writer, ok := host.store.repo.(sessiontree.RuntimeJournalRepo)
			if !ok {
				return result, ErrUnsupportedStoreCapability
			}
			_, err = writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{{ID: "restore-queue-delete:" + queued.ID, ThreadID: threadID.String(), Type: sessiontree.EntryQueueDeleted, Payload: payload, Metadata: map[string]string{restoredFromBackupMetadata: "true"}, CreatedAt: time.Now().UTC()}})
			if err != nil {
				return result, runtimeHostError(err)
			}
			result.StoppedQueueInputs++
		}
		actor := service.runtime(threadID)
		actor.mu.Lock()
		actor.state.view = ThreadView{}
		actor.mu.Unlock()
	}
	return result, ctx.Err()
}

func restoredQueueInputs(ctx context.Context, repo sessiontree.JournalRepo, threadID identity.ThreadID) ([]RestoredInput, error) {
	entries, err := repo.Entries(ctx, threadID.String())
	if err != nil {
		return nil, runtimeHostError(err)
	}
	added := map[string]UserInput{}
	var result []RestoredInput
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryQueueAdded {
			var queued QueuedInput
			if err := json.Unmarshal(entry.Payload, &queued); err != nil {
				return nil, ErrAuthorityCorrupt
			}
			added[queued.ID] = queued.Input
		}
		if entry.Type == sessiontree.EntryQueueDeleted && entry.Metadata[restoredFromBackupMetadata] == "true" {
			var id string
			if err := json.Unmarshal(entry.Payload, &id); err != nil {
				return nil, ErrAuthorityCorrupt
			}
			input, ok := added[id]
			if !ok {
				return nil, ErrAuthorityCorrupt
			}
			result = append(result, RestoredInput{ID: id, Input: input})
		}
	}
	return result, nil
}
