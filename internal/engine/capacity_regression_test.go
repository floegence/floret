package engine_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/observation"
	publicprovider "github.com/floegence/floret/v7/provider"
)

func TestPreparedByteOverflowKeepsCapacityCause(t *testing.T) {
	cause := &publicprovider.RequestByteLimitError{ActualBytes: 101, LimitBytes: 100}
	p := newPreparedLifecycleProvider(preparedBehavior{prepareErr: errors.Join(cause, provider.ErrContextOverflow), events: preparedDone("after compact")})
	store := session.NewMemoryStore()
	if err := store.AppendTranscript("run", session.Message{Role: session.User, Content: "older", EntryID: "u1"}, session.Message{Role: session.User, Content: "newer", EntryID: "u2"}); err != nil {
		t.Fatal(err)
	}
	recorder := &event.Recorder{}
	e := newTestEngine(p, recorder)
	e.Store = store
	e.Options.ProviderName = "fixture"
	e.Options.Model = "fixture-model"
	e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
	got := e.Run(t.Context(), "")
	if got.Status != engine.Completed {
		t.Fatalf("%+v", got)
	}
	for _, ev := range recorder.Events {
		if ev.Type == event.ContextCompact {
			meta := ev.Metadata.(map[string]any)
			decision, ok := meta["decision"].(*observation.CompactionDecision)
			if !ok || decision.Capacity == nil || decision.Capacity.ActualBytes != 101 || decision.Capacity.LimitBytes != 100 || decision.Validate() != nil {
				t.Fatalf("lost capacity evidence: %+v error=%v", decision, decision.Validate())
			}
			if decision.Pressure.HardLimitExceeded || decision.Pressure.WindowInputTokens != 0 {
				t.Fatal("byte overflow fabricated native token pressure")
			}
			if fmt.Sprint(meta["reason"]) != "request_bytes" {
				t.Fatalf("byte capacity was flattened: %v", meta["reason"])
			}
		}
	}
}

func TestServerByteOverflowKeepsUnknownCounts(t *testing.T) {
	cause := errors.Join(&publicprovider.ProviderHTTPError{Provider: "fixture", StatusCode: 413, Code: "request_too_large"}, provider.ErrContextOverflow)
	p := newPreparedLifecycleProvider(preparedBehavior{streamErr: cause}, preparedBehavior{events: preparedDone("after compact")})
	store := session.NewMemoryStore()
	if err := store.AppendTranscript("run", session.Message{Role: session.User, Content: "older", EntryID: "u1"}, session.Message{Role: session.User, Content: "newer", EntryID: "u2"}); err != nil {
		t.Fatal(err)
	}
	recorder := &event.Recorder{}
	e := newTestEngine(p, recorder)
	e.Store = store
	e.Options.ProviderName = "fixture"
	e.Options.Model = "fixture-model"
	e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
	got := e.Run(t.Context(), "")
	if got.Status != engine.Completed {
		t.Fatalf("%+v", got)
	}
	found := false
	for _, ev := range recorder.Events {
		if ev.Type != event.ContextCompact {
			continue
		}
		found = true
		d := ev.Metadata.(map[string]any)["decision"].(*observation.CompactionDecision)
		if d.Validate() != nil || d.Capacity == nil || d.Capacity.Source != "provider" || d.Capacity.Stage != "response" || d.Capacity.ActualBytes != 0 || d.Capacity.LimitBytes != 0 || d.Pressure.HardLimitExceeded {
			t.Fatalf("invented server capacity: %+v", d)
		}
	}
	if !found {
		t.Fatal("missing capacity event")
	}
}

func TestTemporaryReferenceRenewalIsBoundedAndDoesNotCompact(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		t.Run(fmt.Sprint(repeated), func(t *testing.T) {
			err := &publicprovider.RequestPreparationError{Cause: errors.New("file expired")}
			second := preparedBehavior{events: preparedDone("renewed")}
			if repeated {
				second = preparedBehavior{streamErr: err}
			}
			p := newPreparedLifecycleProvider(preparedBehavior{streamErr: err}, second)
			recorder := &event.Recorder{}
			e := newTestEngine(p, recorder)
			got := e.Run(t.Context(), "inspect image")
			if (got.Status == engine.Completed) == repeated {
				t.Fatalf("bounded renewal status %+v", got)
			}
			for _, ev := range recorder.Events {
				if ev.Type == event.ContextCompact {
					t.Fatal("reference refresh compacted history")
				}
			}
		})
	}
}
