package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
)

func TestMemoryStateV4FreshAndCurrentRoundTrip(t *testing.T) {
	repo := NewMemoryRepo()
	encoded, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(encoded, &header); err != nil {
		t.Fatal(err)
	}
	if header.Version != 4 {
		t.Fatalf("fresh state version=%d, want 4", header.Version)
	}
	decoded, err := DecodeMemoryState(encoded, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := decoded.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	if string(reencoded) != string(encoded) {
		t.Fatal("current state round trip changed canonical bytes")
	}
}

func TestMemoryStateV2MigratesActiveSubAgentAdmission(t *testing.T) {
	repo, request, admitted := newLegacySubAgentAdmissionState(t, false)
	key := turnAdmissionKey(request.ChildThreadID, request.TurnID)
	encoded := encodePublishedV323MemoryState(t, repo, key)

	migrated, err := DecodeMemoryState(encoded, func() time.Time { return admitted.Lease.ExpiresAt.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	ledger, ok := migrated.turnAdmissions[key]
	if !ok {
		t.Fatal("v2 active SubAgent admission was not reconstructed")
	}
	if !SameTurnLease(ledger.Lease, admitted.Lease) || ledger.TurnStartedID != admitted.TurnStarted.ID ||
		ledger.UserMessageID != admitted.UserMessage.ID || ledger.BaseLeafID != admitted.TurnStarted.ParentID {
		t.Fatalf("migrated active admission=%#v, want exact published authority", ledger)
	}
	if len(migrated.entries[request.ChildThreadID]) != len(repo.entries[request.ChildThreadID]) ||
		migrated.subAgentInputs[request.ChildThreadID][0].SubAgentInputID != repo.subAgentInputs[request.ChildThreadID][0].SubAgentInputID {
		t.Fatal("v2 migration did not preserve the SubAgent journal and input")
	}

	replayed, err := migrated.AdmitSubAgentInput(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || !SameTurnLease(replayed.Lease, admitted.Lease) {
		t.Fatalf("migrated admission replay=%#v, want exact replay", replayed)
	}
}

func TestMemoryStateV2MigratesTerminalSubAgentAdmissionWithoutExecutableLease(t *testing.T) {
	repo, request, _ := newLegacySubAgentAdmissionState(t, true)
	key := turnAdmissionKey(request.ChildThreadID, request.TurnID)
	encoded := encodePublishedV323MemoryState(t, repo, key)

	migrated, err := DecodeMemoryState(encoded, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	ledger, ok := migrated.turnAdmissions[key]
	if !ok {
		t.Fatal("v2 terminal SubAgent admission was not reconstructed")
	}
	if ledger.Lease != (TurnLease{}) || ledger.LegacyTerminalProof == nil {
		t.Fatalf("migrated terminal admission=%#v, want read-only proof without lease", ledger)
	}
	if _, active := migrated.leases[request.ChildThreadID]; active {
		t.Fatal("terminal migration manufactured an active lease")
	}

	first, err := migrated.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := DecodeMemoryState(first, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := reopened.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != string(first) {
		t.Fatal("migrated terminal state is not restart-idempotent")
	}
}

func TestMemoryStateMigrationRejectsDriftAndFutureVersions(t *testing.T) {
	t.Run("active input without exact lease", func(t *testing.T) {
		repo, request, _ := newLegacySubAgentAdmissionState(t, false)
		key := turnAdmissionKey(request.ChildThreadID, request.TurnID)
		lease := repo.leases[request.ChildThreadID]
		lease.TurnID = "other-turn"
		repo.leases[request.ChildThreadID] = lease
		if _, err := DecodeMemoryState(encodePublishedV323MemoryState(t, repo, key), time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("drifted active migration err=%v, want ErrAuthorityCorrupt", err)
		}
	})

	t.Run("terminal input with drifted finish", func(t *testing.T) {
		repo, request, _ := newLegacySubAgentAdmissionState(t, true)
		key := turnAdmissionKey(request.ChildThreadID, request.TurnID)
		finish := repo.turnFinishes[key]
		finish.RunID = "other-run"
		repo.turnFinishes[key] = finish
		if _, err := DecodeMemoryState(encodePublishedV323MemoryState(t, repo, key), time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("drifted terminal migration err=%v, want ErrAuthorityCorrupt", err)
		}
	})

	t.Run("current state missing admission", func(t *testing.T) {
		repo, request, _ := newLegacySubAgentAdmissionState(t, false)
		key := turnAdmissionKey(request.ChildThreadID, request.TurnID)
		state := repo.memoryStateLocked()
		state.Version = 4
		delete(state.TurnAdmissions, key)
		encoded, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeMemoryState(encoded, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("invalid v4 state err=%v, want ErrAuthorityCorrupt", err)
		}
	})

	t.Run("future version", func(t *testing.T) {
		repo := NewMemoryRepo()
		state := repo.memoryStateLocked()
		state.Version = 5
		encoded, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeMemoryState(encoded, time.Now); err == nil {
			t.Fatal("future session-tree state was accepted")
		}
	})
}

func newLegacySubAgentAdmissionState(t *testing.T, terminal bool) (*MemoryRepo, AdmitSubAgentInputRequest, AdmitSubAgentInputResult) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{
		ID: "child", ParentThreadID: "parent", TaskName: "worker", AgentPath: "/root/worker",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.PublishSubAgentInput(ctx, PublishSubAgentInputRequest{
		InputRequestID: "input", RequestFingerprint: "publish-fingerprint",
		ParentThreadID: "parent", ChildThreadID: "child",
		Message: session.Message{Role: session.User, Content: "continue the delegated task"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	request := AdmitSubAgentInputRequest{
		ParentThreadID: "parent", ChildThreadID: "child", TurnID: "turn", RunID: "run", OwnerID: "owner", Now: now,
	}
	admitted, err := repo.AdmitSubAgentInput(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if terminal {
		if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
			Lease: admitted.Lease, RunID: request.RunID, TerminalEntryID: "terminal", Status: TurnCompleted,
			OutcomeFingerprint: "completed", Now: now.Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return repo, request, admitted
}

// encodePublishedV323MemoryState reproduces the domain shape emitted by the
// published v3.2.3 AdmitSubAgentInput implementation.
func encodePublishedV323MemoryState(t *testing.T, repo *MemoryRepo, missingAdmissionKey string) []byte {
	t.Helper()
	state := repo.memoryStateLocked()
	state.Version = 2
	delete(state.TurnAdmissions, missingAdmissionKey)
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
