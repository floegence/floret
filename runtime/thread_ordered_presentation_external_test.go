package runtime_test

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v4/runtime"
)

func TestThreadItemOrderedPresentationSurface(t *testing.T) {
	item := runtime.ThreadItem{
		ID: "thinking:turn-example:1", TurnID: "turn-example", Ordinal: 2,
		Kind: runtime.ThreadItemThinking, Text: "checking", Live: true,
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	var decoded runtime.ThreadItem
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != item.ID || decoded.Ordinal != 2 || decoded.Kind != runtime.ThreadItemThinking || !decoded.Live {
		t.Fatalf("ordered thread item round trip=%#v", decoded)
	}
}
