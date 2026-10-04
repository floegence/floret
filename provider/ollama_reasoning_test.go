package provider_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
)

func TestOllamaReasoningUsesDeclaredValues(t *testing.T) {
	for _, tt := range []struct {
		name, raw, kind string
		thinking, off   bool
		levels          []config.ReasoningLevel
	}{
		{"qwen served metadata", `{"values":[false,"low","medium","xhigh"],"default":"medium"}`, config.ReasoningKindEffort, true, true, []config.ReasoningLevel{"low", "medium", "xhigh"}},
		{"boolean toggle", `{"values":[true,false],"default":false}`, config.ReasoningKindToggle, true, true, []config.ReasoningLevel{"on"}},
		{"gpt oss", `{"values":["low","medium","high"],"default":"medium"}`, config.ReasoningKindEffort, true, false, []config.ReasoningLevel{"low", "medium", "high"}},
		{"fixed on", `{"values":[true],"default":true}`, config.ReasoningKindAlwaysOn, true, false, nil},
		{"fixed off", `{"values":[false],"default":false}`, config.ReasoningKindNone, true, false, nil},
		{"legacy thinking", `null`, config.ReasoningKindDynamic, true, false, nil},
		{"legacy ordinary", `null`, config.ReasoningKindNone, false, false, nil},
		{"unknown named level", `{"values":["adaptive"],"default":"adaptive"}`, config.ReasoningKindDynamic, true, false, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cap, err := provider.ParseOllamaReasoningCapability(json.RawMessage(tt.raw), tt.thinking)
			if err != nil {
				t.Fatal(err)
			}
			if cap.Kind != tt.kind || cap.DisableSupported != tt.off || !reflect.DeepEqual(cap.SupportedLevels, tt.levels) {
				t.Fatalf("capability=%+v", cap)
			}
			if err := cap.Validate(); err != nil {
				t.Fatal(err)
			}
			if cap.DefaultLevel != "" && cap.DefaultLevel != config.ReasoningLevelDefault {
				t.Fatal("server default became explicit user intent")
			}
		})
	}
}

func TestOllamaReasoningWireRejectsUnsupportedIntent(t *testing.T) {
	cap, err := provider.ParseOllamaReasoningCapability(json.RawMessage(`{"values":[false,"low","medium","xhigh"],"default":"medium"}`), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		level   config.ReasoningLevel
		wire    string
		invalid bool
	}{{"default", "", false}, {"off", "none", false}, {"xhigh", "xhigh", false}, {"high", "", true}, {"on", "", true}} {
		wire, err := provider.OllamaReasoningEffort(cap, config.ReasoningSelection{Level: tt.level})
		if (err != nil) != tt.invalid || wire != tt.wire {
			t.Fatalf("level=%s wire=%q err=%v", tt.level, wire, err)
		}
	}
	toggle, _ := provider.ParseOllamaReasoningCapability(json.RawMessage(`{"values":[false,true]}`), true)
	if wire, err := provider.OllamaReasoningEffort(toggle, config.ReasoningSelection{Level: config.ReasoningLevelOn}); err != nil || wire != "medium" {
		t.Fatalf("toggle wire=%q err=%v", wire, err)
	}
	if _, err := provider.OllamaReasoningEffort(toggle, config.ReasoningSelection{BudgetTokens: 1024}); err == nil {
		t.Fatal("accepted unsupported budget")
	}
}

func TestOllamaReasoningRejectsMalformedMetadata(t *testing.T) {
	for _, raw := range []string{`{}`, `{"values":[]}`, `{"values":[1]}`, `{"values":[null]}`, `{"values":[true],"default":false}`, `{"values":["high"],"default":"low"}`} {
		if _, err := provider.ParseOllamaReasoningCapability(json.RawMessage(raw), true); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
