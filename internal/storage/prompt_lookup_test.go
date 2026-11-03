package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTurnPreparationDoesNotReadUnrelatedRequests(t *testing.T) {
	inner, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &requestReadBackend{Backend: inner}
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		if err := kernel.AppendProviderRequest(t.Context(), cache.ProviderRequestRecord{ID: fmt.Sprint(i), PromptScopeID: "scope", TurnID: fmt.Sprint(i), Provider: "test", Model: "model"}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		backend.gets, backend.scans = 0, 0
		if _, found, err := cache.InitialTurnSurface(t.Context(), kernel, "scope", "new-turn"); err != nil || found {
			t.Fatalf("missing turn: %v %v", found, err)
		}
		if err := cache.ValidateTurnModel(t.Context(), kernel, "scope", "999", "test", "model"); err != nil {
			t.Fatal(err)
		}
		if backend.scans != 0 || backend.gets > 12 {
			t.Fatalf("unrelated history read: gets=%d scans=%d", backend.gets, backend.scans)
		}
		kernel, err = NewBackendKernel(t.Context(), backend, time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPromptLookupEvidenceReadsStayBoundedAfterRestart(t *testing.T) {
	inner, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &requestReadBackend{Backend: inner}
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		id := fmt.Sprint(i)
		if err := kernel.CheckpointProviderRequest(t.Context(), nil, []cache.ToolsetSnapshot{{ID: id, PromptScopeID: "scope", Provider: "p", Model: "m"}}, cache.ProviderRequestRecord{ID: id, AttemptID: id, TurnID: id, PromptScopeID: "scope", Provider: "p", Model: "m"}); err != nil {
			t.Fatal(err)
		}
		if err := kernel.AppendProviderResponse(t.Context(), cache.ProviderResponseRecord{PromptScopeID: "scope", RequestID: id, AttemptID: id, PressureAnchor: cache.PressureAnchorState{PromptScopeID: "scope", Provider: "p", Model: "m", RequestID: id, WindowInputTokens: 5, MeasurementPrefix: requestmeasure.Prefix{Fingerprint: id}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := kernel.Checkpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		backend.gets, backend.scans = 0, 0
		tool, found, err := kernel.ActiveToolset(t.Context(), "scope", "p", "m")
		if err != nil || !found || tool.ID != "999" {
			t.Fatalf("toolset: %+v %v %v", tool, found, err)
		}
		evidence, found, err := kernel.ReadPressureEvidence(t.Context(), "scope", "p", "m")
		if err != nil || !found || evidence.Request.ID != "999" || evidence.Response == nil {
			t.Fatalf("evidence: %+v %v %v", evidence, found, err)
		}
		if backend.scans != 0 || backend.gets > 12 {
			t.Fatalf("unbounded reads: %d gets, %d scans", backend.gets, backend.scans)
		}
		kernel, err = NewBackendKernel(t.Context(), backend, time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPromptLookupPreparationYieldsAfterLargeRecord(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := kernel.AppendProviderRequest(t.Context(), cache.ProviderRequestRecord{PromptScopeID: "scope", TurnID: fmt.Sprint(i), TurnSurface: cache.TurnSurfaceSnapshot{SystemPrompt: strings.Repeat("x", promptLookupPageBytes)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deletePromptLookups(tx, "scope") }); err != nil {
		t.Fatal(err)
	}
	if err := kernel.preparePromptLookupPage(t.Context(), "scope", promptRequest); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		through, _, err := readLookup[uint64](tx, promptLookupCoverageKey("scope", promptRequest))
		if through != 1 {
			t.Fatalf("prepared %d large records without yielding", through)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// A later append must not claim that the historical gap is prepared.
	if err := kernel.AppendProviderRequest(t.Context(), cache.ProviderRequestRecord{PromptScopeID: "scope", TurnID: "tail", Model: "tail"}); err != nil {
		t.Fatal(err)
	}
	facts, err := kernel.ReadTurnRequestFacts(t.Context(), "scope", "tail")
	if err != nil || facts.Model != "tail" {
		t.Fatalf("gap preparation: %+v %v", facts, err)
	}
}

func BenchmarkPromptLookupCheckpoint(b *testing.B) {
	for _, sameTurn := range []bool{false, true} {
		b.Run(fmt.Sprint(sameTurn), func(b *testing.B) {
			backend, err := storagebridge.Open(b.Context(), storagebridge.Source(publicstorage.SQLite(b.TempDir()+"/prompt.sqlite")))
			if err != nil {
				b.Fatal(err)
			}
			defer backend.Close()
			kernel, err := NewBackendKernel(b.Context(), backend, time.Now)
			if err != nil {
				b.Fatal(err)
			}
			r := cache.ProviderRequestRecord{PromptScopeID: "scope", TurnID: "turn", Provider: "p", Model: "m", TurnSurface: cache.TurnSurfaceSnapshot{SystemPrompt: strings.Repeat("x", 16384)}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				r.ID = fmt.Sprint(i)
				if !sameTurn {
					r.TurnID = r.ID
				}
				if err := kernel.AppendProviderRequest(b.Context(), r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPromptLookupMatchesCanonicalAdapters(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/prompt.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	oracle := cache.NewMemoryStore()
	requests := []cache.ProviderRequestRecord{
		{ID: "one", TurnID: "turn", RunID: "run", Provider: "p", Model: "m", AttemptID: "a"},
		{ID: "two", TurnID: "turn", Provider: "p", Model: "drift"},
		{ID: "three", TurnID: " padded ", Provider: "p", Model: "m"},
		{ID: "four", RunID: " fallback ", Provider: "p", Model: "m"},
		{ID: "one", TurnID: "retry", Provider: "p", Model: "m", AttemptID: "b"},
	}
	for i := range requests {
		r := &requests[i]
		r.PromptScopeID = "scope"
		r.TurnSurface, _, err = cache.ResolveTurnSurface(t.Context(), cache.NewMemoryStore(), "scope", "surface", cache.TurnSurfaceSnapshot{Provider: r.Provider, Model: r.Model, SystemPrompt: fmt.Sprint(i), AdapterVersion: cache.Version, StateCompatibilityKey: "stable"}, false)
		if err != nil {
			t.Fatal(err)
		}
		if err = kernel.AppendProviderRequest(t.Context(), *r); err != nil {
			t.Fatal(err)
		}
		if err = oracle.AppendProviderRequest(t.Context(), *r); err != nil {
			t.Fatal(err)
		}
	}
	check := func() {
		t.Helper()
		for _, turn := range []string{"turn", "padded", " padded ", "fallback", "retry", "missing"} {
			got, gf, ge := cache.InitialTurnSurface(t.Context(), kernel, "scope", turn)
			want, wf, we := cache.InitialTurnSurface(t.Context(), oracle, "scope", turn)
			if ge != nil || we != nil || gf != wf || !reflect.DeepEqual(got, want) {
				t.Fatalf("surface %q: got=%+v/%v/%v want=%+v/%v/%v", turn, got, gf, ge, want, wf, we)
			}
			ge = cache.ValidateTurnModel(t.Context(), kernel, "scope", turn, "p", "m")
			we = cache.ValidateTurnModel(t.Context(), oracle, "scope", turn, "p", "m")
			if !errors.Is(ge, we) {
				t.Fatalf("model %q: %v / %v", turn, ge, we)
			}
		}
		for _, provider := range []string{"", "p", "other", "missing"} {
			for _, model := range []string{"", "m", "other"} {
				got, gf, ge := kernel.ActiveToolset(t.Context(), "scope", provider, model)
				want, wf, we := oracle.ActiveToolset(t.Context(), "scope", provider, model)
				if ge != nil || we != nil || gf != wf || !reflect.DeepEqual(got, want) {
					t.Fatalf("toolset %q/%q: %v/%v %v/%v", provider, model, gf, ge, wf, we)
				}
				a, af, ae := kernel.LatestPressureAnchor(t.Context(), "scope", provider, model)
				b, bf, be := oracle.LatestPressureAnchor(t.Context(), "scope", provider, model)
				if ae != nil || be != nil || af != bf || a != b {
					t.Fatalf("anchor %q/%q: %+v/%v/%v %+v/%v/%v", provider, model, a, af, ae, b, bf, be)
				}
				e, ef, ee := cache.ReadPressureEvidence(t.Context(), kernel, "scope", provider, model)
				f, ff, fe := cache.ReadPressureEvidence(t.Context(), oracle, "scope", provider, model)
				if ee != nil || fe != nil || ef != ff || !reflect.DeepEqual(e, f) {
					t.Fatalf("evidence %q/%q: %+v/%v/%v %+v/%v/%v", provider, model, e, ef, ee, f, ff, fe)
				}
			}
		}
	}
	for i := range 4 {
		toolset := cache.ToolsetSnapshot{ID: fmt.Sprint(i), PromptScopeID: "scope", Provider: "p", Model: "m", Tools: []cache.ToolDefinition{{Name: fmt.Sprint(i)}}}
		if i == 2 {
			toolset.Provider = "other"
		}
		if err := kernel.AppendToolset(t.Context(), toolset); err != nil {
			t.Fatal(err)
		}
		if err := oracle.AppendToolset(t.Context(), toolset); err != nil {
			t.Fatal(err)
		}
		anchor := cache.PressureAnchorState{PromptScopeID: "scope", Provider: toolset.Provider, Model: "m", RequestID: "one", WindowInputTokens: int64(10 + i)}
		if i == 1 || i == 3 {
			anchor.MeasurementPrefix = requestmeasure.Prefix{Fingerprint: "prefix"}
		}
		if i == 3 {
			anchor.WindowInputTokens = 0
			anchor.InputMeasurementQualified = true
		}
		response := cache.ProviderResponseRecord{PromptScopeID: "scope", RequestID: "one", AttemptID: "a", PressureAnchor: anchor}
		if err := kernel.AppendProviderResponse(t.Context(), response); err != nil {
			t.Fatal(err)
		}
		if err := oracle.AppendProviderResponse(t.Context(), response); err != nil {
			t.Fatal(err)
		}
		check() // Includes uncheckpointed observations and dynamic tools.
	}
	if err := kernel.Checkpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	check()
	kernel, err = NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	check()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deletePromptLookups(tx, "scope") }); err != nil {
		t.Fatal(err)
	}
	check() // Released stores prepare source locators on demand.
	if err := kernel.DeletePromptScopes(t.Context(), "scope"); err != nil {
		t.Fatal(err)
	}
	if err := oracle.DeletePromptScopes(t.Context(), "scope"); err != nil {
		t.Fatal(err)
	}
	check()
}

type lookupFaultBackend struct {
	spi.Backend
	mode   string
	cancel context.CancelFunc
}

func (b *lookupFaultBackend) Update(ctx context.Context, f func(spi.WriteTx) error) error {
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := f(tx); err != nil {
			return err
		}
		switch b.mode {
		case "fail":
			return errors.New("injected failure")
		case "cancel":
			b.cancel()
			return ctx.Err()
		case "panic":
			panic("injected panic")
		}
		return nil
	})
}

func TestPromptLookupPreparationRollsBackAndResumes(t *testing.T) {
	for _, mode := range []string{"fail", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			inner, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer inner.Close()
			backend := &lookupFaultBackend{Backend: inner}
			kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 40 {
				if err := kernel.AppendProviderRequest(t.Context(), cache.ProviderRequestRecord{ID: fmt.Sprint(i), PromptScopeID: "scope", TurnID: fmt.Sprint(i), Provider: "p", Model: "m"}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := kernel.ProviderRequests(t.Context(), "scope")
			if err != nil {
				t.Fatal(err)
			}
			if err := inner.Update(t.Context(), func(tx spi.WriteTx) error { return deletePromptLookups(tx, "scope") }); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backend.mode, backend.cancel = mode, cancel
			func() {
				defer func() {
					if p := recover(); p != nil && mode != "panic" {
						t.Fatalf("unexpected panic: %v", p)
					}
				}()
				if _, err := kernel.ReadTurnRequestFacts(ctx, "scope", "39"); err == nil {
					t.Fatal("injected failure succeeded")
				}
			}()
			backend.mode = ""
			if err := inner.View(t.Context(), func(tx spi.ReadTx) error {
				through, _, err := readLookup[uint64](tx, promptLookupCoverageKey("scope", promptRequest))
				if err != nil {
					return err
				}
				if through != 0 {
					t.Fatalf("partial coverage published: %d", through)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			kernel, err = NewBackendKernel(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			facts, err := kernel.ReadTurnRequestFacts(t.Context(), "scope", "39")
			if err != nil || !facts.ModelFound || facts.Model != "m" {
				t.Fatalf("resume: %+v %v", facts, err)
			}
			after, err := kernel.ProviderRequests(t.Context(), "scope")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("canonical sources changed: %v", err)
			}
		})
	}
}

func TestPromptLookupRejectsMismatchedSourcesAndCoverage(t *testing.T) {
	for _, mode := range []string{"out-of-range", "wrong-turn", "missing-source", "future-coverage", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			for _, turn := range []string{"wanted", "other"} {
				if err := kernel.AppendProviderRequest(t.Context(), cache.ProviderRequestRecord{PromptScopeID: "scope", TurnID: turn}); err != nil {
					t.Fatal(err)
				}
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				switch mode {
				case "out-of-range":
					return putLookup(tx, promptLookupKey("scope", "turn", "wanted"), turnSourceOrdinals{Surface: 3})
				case "wrong-turn":
					return putLookup(tx, promptLookupKey("scope", "turn", "wanted"), turnSourceOrdinals{Surface: 2})
				case "missing-source":
					return tx.Delete(promptRecordsNamespace, promptRecordKey(promptRecord{ScopeID: "scope", Category: promptRequest, Ordinal: 1}))
				case "future-coverage":
					return putLookup(tx, promptLookupCoverageKey("scope", promptRequest), uint64(3))
				default:
					return tx.Put(promptLookupNamespace, promptLookupKey("scope", "turn", "wanted"), []byte("{"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := kernel.ReadTurnRequestFacts(t.Context(), "scope", "wanted"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("corrupt source accepted: %v", err)
			}
		})
	}
}
