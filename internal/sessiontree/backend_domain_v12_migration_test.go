package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func installV11CancellationStore(t *testing.T, backend *migrationTestBackend) *MemoryRepo {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "accept", InputRequestFingerprint: "input",
		Input: session.Message{Role: session.User, References: []session.MessageReference{{ReferenceID: "device", Kind: session.MessageReferenceText, Label: "udesk26", Text: "Selected device: ssh:host:udesk26"}}}, Now: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "done", Status: TurnCompleted, OutcomeFingerprint: "done", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV11(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	return memory
}

func TestBackendDomainV11ToV12PreservesCanonicalBytes(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "references_and_fork"}[populated], func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := newMemoryRepo(time.Now)
			if populated {
				memory = installV11CancellationStore(t, backend)
			} else if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV11(tx, memory) }); err != nil {
				t.Fatal(err)
			}
			before, err := memory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			after, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatal("migration changed existing canonical facts")
			}
			if len(migrationTestNamespaceRecords(t, backend, backendDomainV11Namespace)) != 0 {
				t.Fatal("source records survived")
			}
			current := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(backend.records, current) {
				t.Fatal("current restart rewrote bytes")
			}
		})
	}
}

func TestBackendDomainV11ToV12RollsBackFailures(t *testing.T) {
	for _, failure := range []string{"write", "cancel", "panic", "source_drift", "future"} {
		t.Run(failure, func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := installV11CancellationStore(t, backend)
			ctx := t.Context()
			var target spi.Backend = backend
			switch failure {
			case "write":
				target = migrationFailingBackend{Backend: backend, err: errors.New("injected write failure")}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "panic":
				target = migrationPanickingBackend{Backend: backend}
			case "source_drift":
				for id, entries := range memory.entries {
					for i := range entries {
						if entries[i].Type == EntryUserMessage {
							entries[i].Message.ToolResult = &session.ToolResultView{InputRequired: &tools.InputRequest{Summary: "Unexpected new contract"}}
							entries[i] = PrepareEntry(entries[i])
						}
					}
					memory.entries[id] = entries
				}
				format := backendDomainV11Format
				format.validate = func(*MemoryRepo) error { return nil }
				if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomain(tx, memory, format) }); err != nil {
					t.Fatal(err)
				}
			case "future":
				format := backendDomainV11Format
				format.version++
				raw, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: format.version})
				if err != nil {
					t.Fatal(err)
				}
				if err := backend.Update(ctx, func(tx spi.WriteTx) error {
					return tx.Put(format.namespace, backendDomainV11Key(backendDomainRecordManifest), raw)
				}); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneMigrationRecords(backend.records)
			func() {
				if failure == "panic" {
					defer func() {
						if recover() == nil {
							t.Error("expected injected panic")
						}
					}()
				}
				if _, err := NewBackendRepo(ctx, target, time.Now); err == nil {
					t.Error("invalid migration succeeded")
				}
			}()
			if !reflect.DeepEqual(backend.records, before) {
				t.Fatal("failed migration mutated the backend")
			}
		})
	}
}
