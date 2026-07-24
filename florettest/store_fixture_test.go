package florettest_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/florettest"
	"github.com/floegence/floret/runtime"
)

func TestPopulateStoreFixtureUsesPublicInputs(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) *runtime.Store
	}{
		{name: "memory", open: func(*testing.T) *runtime.Store { return runtime.NewMemoryStore() }},
		{name: "sqlite", open: func(t *testing.T) *runtime.Store {
			store, err := runtime.OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "fixture.db"), runtime.SQLiteStoreOpenRequest{
				ExpectedState: runtime.SQLiteStoreStateMissing,
			})
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := test.open(t)
			defer store.Close()
			result, err := florettest.PopulateStoreFixture(context.Background(), store, florettest.StoreFixtureInput{
				ThreadID: "fixture-thread", CreateIntentID: "fixture-create",
				Turns: []florettest.StoreFixtureTurn{{
					Request: runtime.RunTurnRequest{TurnID: "fixture-turn", RunID: "fixture-run", Input: runtime.TurnInput{Text: "fixture input"}},
					ModelSteps: []florettest.ModelStep{{Events: []runtime.ModelEvent{
						{Type: runtime.ModelEventDelta, Text: "fixture output"},
						{Type: runtime.ModelEventDone, Reason: "stop"},
					}}},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Thread.ID != "fixture-thread" || len(result.Turns) != 1 || result.Turns[0].Status != runtime.TurnStatusCompleted || result.Turns[0].Output != "fixture output" {
				t.Fatalf("fixture result=%#v", result)
			}
		})
	}
}
