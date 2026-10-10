package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func BenchmarkPromptCheckpointWith128MiBHistory(b *testing.B) {
	backend, err := storagebridge.Open(b.Context(), storagebridge.Source(publicstorage.SQLite(b.TempDir()+"/large.sqlite")))
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()
	kernel, err := NewBackendKernel(b.Context(), backend, time.Now)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		if err := kernel.CheckpointProviderRequest(b.Context(), []cache.Segment{{ID: fmt.Sprint(index), PromptScopeID: "unrelated", Raw: strings.Repeat("x", 1<<20)}}, nil, cache.ProviderRequestRecord{ID: fmt.Sprint(index), PromptScopeID: "unrelated"}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := kernel.AppendProviderRequest(b.Context(), cache.ProviderRequestRecord{ID: "same-attempt", PromptScopeID: "active"}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPromptLegacySplitWith15000RecordsAnd128MiBCache(b *testing.B) {
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		backend, err := storagebridge.Open(b.Context(), storagebridge.Source(publicstorage.SQLite(b.TempDir()+"/large.sqlite")))
		if err != nil {
			b.Fatal(err)
		}
		repo, err := sessiontree.NewBackendRepo(b.Context(), backend, time.Now)
		if err != nil {
			b.Fatal(err)
		}
		if err := repo.UpdateDomain(b.Context(), func(memory *sessiontree.MemoryRepo, _ spi.WriteTx) error {
			if _, err := memory.CreateThread(b.Context(), sessiontree.ThreadMeta{ID: "history"}); err != nil {
				return err
			}
			for row := 0; row < 15000; row++ {
				if _, err := memory.Append(b.Context(), sessiontree.Entry{ThreadID: "history", Type: sessiontree.EntryCustom, Payload: json.RawMessage(`"synthetic history fact"`)}, sessiontree.AppendOptions{ID: fmt.Sprintf("history-%08d", row), Now: time.Now()}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		state := legacyPromptSnapshot{Version: 1}
		for row := 0; row < 128; row++ {
			state.Segments = append(state.Segments, cache.Segment{ID: fmt.Sprint(row), PromptScopeID: fmt.Sprint(row % 8), Raw: strings.Repeat("x", 1<<20)})
		}
		payload, err := json.Marshal(state)
		if err != nil {
			b.Fatal(err)
		}
		encoded, err := storagecodec.EncodeEnvelope("prompt", payload)
		if err != nil {
			b.Fatal(err)
		}
		if err := backend.Update(b.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainNamespace, promptStateKey, encoded) }); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		b.ReportAllocs()
		if _, err := NewBackendKernel(b.Context(), backend, time.Now); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		backend.Close()
	}
}
