package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func legacyPromptFixture() legacyPromptSnapshot {
	return legacyPromptSnapshot{Version: 1,
		Segments:  []cache.Segment{{ID: "first", PromptScopeID: "a", Raw: "raw rendered fragment"}, {ID: "other", PromptScopeID: "b"}, {ID: "last", PromptScopeID: "a"}},
		Toolsets:  []cache.ToolsetSnapshot{{ID: "old", PromptScopeID: "a", Epoch: 1}, {ID: "new", PromptScopeID: "a", Epoch: 2}},
		Requests:  []cache.ProviderRequestRecord{{ID: "same", PromptScopeID: "a", Attempt: 1, PreviousResponseID: "continuation"}, {ID: "same", PromptScopeID: "a", Attempt: 2, CompactionGeneration: 2}},
		Responses: []cache.ProviderResponseRecord{{RequestID: "same", PromptScopeID: "a", InputTokens: 1234, UsageAvailable: true, PressureAnchor: cache.PressureAnchorState{PromptScopeID: "a", WindowInputTokens: 1234}}},
	}
}

func seedLegacyPrompt(t *testing.T, backend spi.Backend, state legacyPromptSnapshot) []byte {
	t.Helper()
	payload, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := storagecodec.EncodeEnvelope("prompt", payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainNamespace, promptStateKey, encoded) }); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestPromptMigrationPreservesContentOrderAttemptsAndRestart(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	state := legacyPromptFixture()
	seedLegacyPrompt(t, backend, state)
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	segments, err := kernel.Segments(t.Context(), "a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(segments, []cache.Segment{state.Segments[0], state.Segments[2]}) {
		t.Fatal("segments changed")
	}
	requests, err := kernel.ProviderRequests(t.Context(), "a")
	if err != nil || !reflect.DeepEqual(requests, state.Requests) {
		t.Fatalf("request attempts changed: %v", err)
	}
	responses, err := kernel.ProviderResponses(t.Context(), "a")
	if err != nil || !reflect.DeepEqual(responses, state.Responses) {
		t.Fatalf("responses changed: %v", err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		if _, err := tx.Get(backendDomainNamespace, promptStateKey); !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("legacy authority retained: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	counting := &promptCountingBackend{Backend: backend}
	if _, err := NewBackendKernel(t.Context(), counting, time.Now); err != nil {
		t.Fatal(err)
	}
	if counting.bytes != 0 {
		t.Fatalf("restart rewrote %d bytes", counting.bytes)
	}
}

func TestPromptMigrationRollbackAndRetry(t *testing.T) {
	for _, mode := range []string{"write", "commit", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			original := seedLegacyPrompt(t, backend, legacyPromptFixture())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("injected migration failure")
			func() {
				defer func() {
					if mode == "panic" && recover() == nil {
						t.Fatal("missing panic")
					}
				}()
				err = backend.Update(ctx, func(tx spi.WriteTx) error {
					if mode == "write" {
						tx = promptFailTx{WriteTx: tx, err: injected}
					}
					if err := preparePromptRecords(ctx, tx, true, nil); err != nil {
						return err
					}
					switch mode {
					case "commit":
						return injected
					case "cancel":
						cancel()
						return ctx.Err()
					case "panic":
						panic("injected")
					}
					return nil
				})
				if mode != "panic" && err == nil {
					t.Fatal("failure succeeded")
				}
			}()
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				got, err := tx.Get(backendDomainNamespace, promptStateKey)
				if err != nil || !bytes.Equal(got, original) {
					t.Fatal("legacy changed after rollback")
				}
				page, err := tx.Scan(spi.ScanRequest{Namespace: promptRecordsNamespace, Limit: 1})
				if err != nil || len(page.Records) != 0 {
					t.Fatal("partial records survived")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := NewBackendKernel(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type promptFailTx struct {
	spi.WriteTx
	err error
}

func (tx promptFailTx) Put(namespace string, key, value []byte) error {
	if namespace == promptRecordsNamespace {
		return tx.err
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestPromptMigrationRejectsUnknownTrailingFutureAndMixedState(t *testing.T) {
	for _, payload := range []string{`{"version":1,"unknown":true}`, `{"version":99}`, `{"version":1,"segments":[{"prompt_scope_id":""}]}`} {
		backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := storagecodec.EncodeEnvelope("prompt", []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainNamespace, promptStateKey, encoded) }); err != nil {
			t.Fatal(err)
		}
		if _, err := NewBackendKernel(t.Context(), backend, time.Now); err == nil {
			t.Fatalf("accepted %s", payload)
		}
		backend.Close()
	}
	if err := decodePromptJSON([]byte(`{"version":1} {}`), &legacyPromptSnapshot{}); err == nil {
		t.Fatal("accepted trailing legacy data")
	}
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if _, err := NewBackendKernel(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
	seedLegacyPrompt(t, backend, legacyPromptFixture())
	if _, err := NewBackendKernel(t.Context(), backend, time.Now); err == nil {
		t.Fatal("mixed state accepted")
	}
}
