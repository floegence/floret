package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestBackendDomainV7ToV8ClassifiesContextContinueWithoutChangingCanonicalInput(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	memory, continuationID := v7ContextContinueFixture(t, now)

	backend := newMigrationTestBackend()
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomainV7(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	repo, err := NewBackendRepo(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	read, err := repo.ReadCanonicalTurn(ctx, "thread", "turn")
	if err != nil {
		t.Fatal(err)
	}
	if canonicalTurnUserEntryCount(read.Turn.Entries) != 1 {
		t.Fatalf("canonical input count = %d, want 1", canonicalTurnUserEntryCount(read.Turn.Entries))
	}
	entry, err := repo.Entry(ctx, "thread", continuationID)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Message.Kind != session.MessageKindControlSignal || entry.Raw != rawForEntry(entry) || entry.RawHash != stableHash(entry.Raw) {
		t.Fatalf("migrated continuation = %#v", entry)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		if records, err := scanBackendDomainV7(ctx, tx); err != nil || len(records) != 0 {
			return errors.Join(err, errors.New("v7 records remain after migration"))
		}
		_, found, err := loadBackendDomainV9(ctx, tx, time.Now)
		if err != nil || !found {
			return errors.Join(err, errors.New("v9 records are missing"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackendDomainV7ToV8MigrationIsAtomicAndRestartIdempotent(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 15, 0, 0, time.UTC)
	for _, test := range []struct {
		name string
		run  func(context.Context, spi.Backend, time.Time) error
		want error
	}{
		{
			name: "write failure",
			run: func(ctx context.Context, backend spi.Backend, now time.Time) error {
				_, err := NewBackendRepo(ctx, migrationFailingBackend{Backend: backend, err: errors.New("v8 write failed")}, func() time.Time { return now })
				return err
			},
			want: errors.New("v8 write failed"),
		},
		{
			name: "cancellation",
			run: func(_ context.Context, backend spi.Backend, now time.Time) error {
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := NewBackendRepo(cancelled, backend, func() time.Time { return now })
				return err
			},
			want: context.Canceled,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			memory, _ := v7ContextContinueFixture(t, now)
			backend := newMigrationTestBackend()
			if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomainV7(tx, memory) }); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			err := test.run(ctx, backend, now)
			if err == nil || !errors.Is(err, test.want) && test.want.Error() != err.Error() {
				t.Fatalf("migration error = %v, want %v", err, test.want)
			}
			if !equalMigrationRecords(before, backend.records) {
				t.Fatal("failed migration changed durable records")
			}
		})
	}

	memory, _ := v7ContextContinueFixture(t, now)
	backend := newMigrationTestBackend()
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomainV7(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBackendRepo(ctx, backend, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	current := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(ctx, backend, func() time.Time { return now.Add(time.Second) }); err != nil {
		t.Fatal(err)
	}
	if !equalMigrationRecords(current, backend.records) {
		t.Fatal("current v9 restart rewrote durable records")
	}
}

func TestBackendDomainV7ToV8FinalVerificationRollsBackCorruptWrite(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 18, 0, 0, time.UTC)
	memory, _ := v7ContextContinueFixture(t, now)
	backend := newMigrationTestBackend()
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomainV7(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(ctx, migrationCorruptingBackend{Backend: backend}, func() time.Time { return now }); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt write error = %v, want authority corruption", err)
	}
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("final verification failure committed partial migration")
	}
}

func TestBackendDomainV7ToV8ReportsMigrationThenVerification(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 20, 0, 0, time.UTC)
	memory, _ := v7ContextContinueFixture(t, now)
	backend := newMigrationTestBackend()
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomainV7(tx, memory) }); err != nil {
		t.Fatal(err)
	}

	var phases []StartupPhase
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		repo, err := NewBackendRepoInTransaction(ctx, backend, tx, func() time.Time { return now }, func(phase StartupPhase) {
			phases = append(phases, phase)
		})
		if err != nil {
			return err
		}
		return repo.VerifyCurrentStateInTransaction(ctx, tx)
	}); err != nil {
		t.Fatal(err)
	}
	if len(phases) != 2 || phases[0] != StartupPhaseMigrating || phases[1] != StartupPhaseVerifying {
		t.Fatalf("startup phases = %v, want [%s %s]", phases, StartupPhaseMigrating, StartupPhaseVerifying)
	}
}

func TestBackendDomainDetectionRejectsMixedFormatsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 25, 0, 0, time.UTC)
	memory, _ := v7ContextContinueFixture(t, now)
	backend := newMigrationTestBackend()
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := saveCompleteBackendDomainV7(tx, memory); err != nil {
			return err
		}
		return putBackendDomainV8Record(
			tx,
			backendDomainV8Key(backendDomainRecordManifest),
			backendDomainRecordManifest,
			"",
			"",
			0,
			backendDomainV8Manifest{Version: backendDomainV8Version, Sequence: memory.seq},
		)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(ctx, backend, func() time.Time { return now }); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("mixed domain error = %v, want authority corruption", err)
	}
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("mixed domain rejection mutated durable records")
	}
}

func TestBackendDomainV7ToV8RejectsMalformedContextContinueAtomically(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)
	memory := newMemoryRepo(func() time.Time { return now })
	if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	entry := Entry{
		ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnSavePoint,
		Metadata: map[string]string{"reason": "context_continue", "continuation_reason": "hook", "run_id": "run"},
	}
	if _, err := memory.Append(ctx, entry, AppendOptions{ID: "orphan-save-point", Now: now}); err != nil {
		t.Fatal(err)
	}
	backend := newMigrationTestBackend()
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomainV7(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(ctx, backend, func() time.Time { return now }); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("migration error = %v, want authority corruption", err)
	}
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("failed v7 to v8 migration changed durable records")
	}
}

func v7ContextContinueFixture(t *testing.T, now time.Time) (*MemoryRepo, string) {
	t.Helper()
	ctx := context.Background()
	memory := newMemoryRepo(func() time.Time { return now })
	if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	accepted, err := memory.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "request-fingerprint", InputRequestFingerprint: "input-fingerprint",
		Input: session.Message{Role: session.User, Content: "original user input"}, Now: now,
	})
	if err != nil || accepted.UserMessage.ID == "" {
		t.Fatalf("admit fixture canonical input: result=%#v err=%v", accepted, err)
	}
	if _, err := AppendRunMessage(ctx, memory, "thread", "turn", "run", session.Message{Role: session.Assistant, Content: "draft"}); err != nil {
		t.Fatal(err)
	}
	continuation, err := AppendRunMessage(ctx, memory, "thread", "turn", "run", session.Message{Role: session.User, Content: "Continue and finish the remaining work."})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendTurnMarker(ctx, memory, "thread", "turn", TurnSavePoint, map[string]string{
		"reason": "context_continue", "continuation_reason": "hook", "run_id": "run",
	}); err != nil {
		t.Fatal(err)
	}
	// Historical removed-control fixture: v8 must preserve it as a durable fact
	// without requiring a current tool definition.
	if _, err := AppendRunMessage(ctx, memory, "thread", "turn", "run", session.Message{
		Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call",
		ToolCallID: "legacy-complete", ToolName: "task_complete", ToolArgs: `{"output":"done"}`,
		ControlSignal: &session.ControlSignalView{Name: "task_complete", CallID: "legacy-complete", Disposition: "terminal", OutputText: "done"},
	}); err != nil {
		t.Fatal(err)
	}
	return memory, continuation.ID
}

type migrationCorruptingBackend struct {
	spi.Backend
}

func (backend migrationCorruptingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(migrationCorruptingWriteTx{WriteTx: tx})
	})
}

type migrationCorruptingWriteTx struct {
	spi.WriteTx
}

func (tx migrationCorruptingWriteTx) Put(namespace string, key, value []byte) error {
	if namespace == backendDomainV9Namespace && string(key) == string(backendDomainV9Key(backendDomainRecordRootIndex)) {
		value = []byte("corrupt")
	}
	return tx.WriteTx.Put(namespace, key, value)
}
