package runtime

import (
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func TestCompactionDecisionSnapshotIsolation(t *testing.T) {
	decision := &observation.CompactionDecision{Before: &observation.CompactionRequestSnapshot{PayloadFingerprint: "candidate", Provider: "test", Model: "model", Estimate: config.RequestEstimate{EstimatedInputTokens: 900, Source: "fixture", Method: config.EstimateMethodProviderRenderedPayload}, Pressure: config.ContextPressure{ProjectedInputTokens: 900, ContextWindowTokens: 1000, ThresholdTokens: 900, RequestSafeLimit: 950}}}
	decision.Pressure = decision.Before.Pressure
	entry, err := sessiontree.NewThreadContextCompactionEntry(sessiontree.ThreadContextCompaction{ThreadID: "thread", TurnID: "turn", RunID: "run", OperationID: "compact", RequestID: "request", Phase: "start", Status: "running", Decision: decision, ObservedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	decision.Before.Estimate.EstimatedInputTokens = 1
	decoded, err := sessiontree.DecodeThreadContextCompactionEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &ThreadContextSnapshot{Compactions: []ThreadContextCompaction{{Decision: decoded.Decision}}}
	copied := cloneThreadContextSnapshot(snapshot)
	copied.Compactions[0].Decision.Before.Estimate.EstimatedInputTokens = 2
	if snapshot.Compactions[0].Decision.Before.Estimate.EstimatedInputTokens != 900 {
		t.Fatal("snapshot aliases consumer mutation")
	}
	again, err := sessiontree.DecodeThreadContextCompactionEntry(entry)
	if err != nil || !reflect.DeepEqual(again.Decision, snapshot.Compactions[0].Decision) {
		t.Fatalf("canonical evidence changed: %v", err)
	}
}

func TestCapacityDecisionRoundTripAndIsolation(t *testing.T) {
	for _, capacity := range []*observation.CompactionCapacity{
		{Kind: "request_bytes", Stage: "prepare", Source: "host_policy", ActualBytes: 101, LimitBytes: 100},
		{Kind: "request_bytes", Stage: "response", Source: "provider"},
	} {
		decision := &observation.CompactionDecision{Capacity: capacity, Pressure: config.ContextPressure{Signal: "request_bytes", Source: "request_capacity", ContextWindowTokens: 1000, ThresholdTokens: 900, RequestSafeLimit: 950}}
		entry, err := sessiontree.NewThreadContextCompactionEntry(sessiontree.ThreadContextCompaction{ThreadID: "thread", TurnID: "turn", RunID: "run", OperationID: "capacity", RequestID: "request", Phase: "start", Status: "running", Decision: decision, ObservedAt: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := sessiontree.DecodeThreadContextCompactionEntry(entry)
		if err != nil || !reflect.DeepEqual(decoded.Decision, decision) {
			t.Fatalf("capacity round trip: %+v %v", decoded, err)
		}
		snapshot := &ThreadContextSnapshot{Compactions: []ThreadContextCompaction{{Decision: decoded.Decision}}}
		clone := cloneThreadContextSnapshot(snapshot)
		clone.Compactions[0].Decision.Capacity.Source = "changed"
		if decision.Capacity.Source == "changed" || snapshot.Compactions[0].Decision.Capacity.Source == "changed" {
			t.Fatal("capacity aliases mutable observation")
		}
		again, err := sessiontree.DecodeThreadContextCompactionEntry(entry)
		if err != nil || !reflect.DeepEqual(again.Decision, snapshot.Compactions[0].Decision) {
			t.Fatalf("canonical capacity evidence changed: %v", err)
		}
	}
}
