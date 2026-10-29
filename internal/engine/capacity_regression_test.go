package engine_test

import (
	"context"
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

func TestPreparedMediaCapacityRecoversWithoutTokenOverflow(t *testing.T) {
	for _, kind := range []string{"count", "total_bytes"} {
		t.Run(kind, func(t *testing.T) {
			cause := &publicprovider.MediaLimitError{Kind: kind, Actual: 3, Limit: 2}
			p := newPreparedLifecycleProvider(preparedBehavior{prepareErr: cause, events: preparedDone("after compact")})
			store := session.NewMemoryStore()
			if err := store.AppendTranscript("run", session.Message{Role: session.User, Content: "older", EntryID: "u1"}, session.Message{Role: session.User, Content: "newer", EntryID: "u2"}); err != nil {
				t.Fatal(err)
			}
			e := newTestEngine(p, &event.Recorder{})
			e.Store = store
			e.Options.ProviderName, e.Options.Model = "fixture", "fixture-model"
			e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
			got := e.Run(t.Context(), "")
			if got.Status != engine.Completed {
				t.Fatalf("media capacity should recover: %+v", got)
			}
			if errors.Is(cause, publicprovider.ErrContextOverflow) {
				t.Fatal("media must retain its independent error identity")
			}
		})
	}
}

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

func TestMediaCapacityServerRecoveryIsBounded(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		t.Run(fmt.Sprint(repeated), func(t *testing.T) {
			cause := &publicprovider.MediaLimitError{Kind: "count", Actual: 3, Limit: 2}
			second := preparedBehavior{events: preparedDone("recovered")}
			if repeated {
				second = preparedBehavior{streamErr: cause}
			}
			p := newPreparedLifecycleProvider(preparedBehavior{streamErr: cause}, second)
			e := newTestEngine(p, &event.Recorder{})
			e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
			got := e.Run(t.Context(), "inspect images")
			if (got.Status == engine.Completed) == repeated || len(p.snapshotHandles()) != 2 {
				t.Fatalf("status=%s handles=%d err=%v", got.Status, len(p.snapshotHandles()), got.Err)
			}
			if repeated && errors.Is(got.Err, provider.ErrContextOverflow) {
				t.Fatal("image failure became token overflow")
			}
		})
	}
}
func TestIndividualImageFailureDoesNotCompact(t *testing.T) {
	for _, kind := range []string{"dimension", "bytes", "animated_dimension"} {
		p := newPreparedLifecycleProvider(preparedBehavior{prepareErr: &publicprovider.MediaLimitError{Kind: kind, Actual: 3, Limit: 2}})
		recorder := &event.Recorder{}
		e := newTestEngine(p, recorder)
		e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
		got := e.Run(t.Context(), "current image")
		if got.Status != engine.Failed || got.Metrics.Compactions != 0 {
			t.Fatalf("%s: %+v", kind, got)
		}
		for _, ev := range recorder.Events {
			if ev.Type == event.ContextCompact {
				t.Fatal("invalid image entered recovery")
			}
		}
	}
}

type mandatoryMediaProvider struct{ attempts int }

func (p *mandatoryMediaProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	return nil, errors.New("must not dispatch")
}
func (p *mandatoryMediaProvider) PrepareRequest(context.Context, provider.Request) (provider.PreparedRequest, error) {
	p.attempts++
	return nil, &publicprovider.MediaLimitError{Kind: "count", Actual: 3, Limit: 2}
}
func TestMandatoryMediaFailureNeverInstallsCheckpoint(t *testing.T) {
	p := &mandatoryMediaProvider{}
	recorder := &event.Recorder{}
	e := newTestEngine(p, recorder)
	e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
	got := e.RunTurn(t.Context(), engine.RunInput{RunID: "run", ThreadID: "thread", TurnID: "turn", PromptScopeID: "thread", History: []session.Message{{Role: session.User, Content: "current images", EntryID: "current", Attachments: []session.MessageAttachment{{ResourceRef: "one", MIMEType: "image/png"}, {ResourceRef: "two", MIMEType: "image/png"}, {ResourceRef: "three", MIMEType: "image/png"}}}}})
	if got.Status != engine.Failed || p.attempts > 2 || got.Metrics.Compactions != 0 {
		t.Fatalf("attempts=%d result=%+v", p.attempts, got)
	}
	for _, m := range got.Messages {
		if m.Kind == session.MessageKindCompactionSummary {
			t.Fatal("failed replacement installed")
		}
	}
	if len(got.Messages) != 1 || len(got.Messages[0].Attachments) != 3 {
		t.Fatal("current images were dropped")
	}
}
