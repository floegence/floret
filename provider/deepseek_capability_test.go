package provider

import "testing"

func TestDeepSeekModelSearchCapabilities(t *testing.T) {
	for _, model := range []string{"deepseek-flash", "deepseek-v4-flash", "deepseek-v4-flash-vision-exp", "deepseek-v4-pro"} {
		cap, ok := DeepSeekModelCapabilities(model)
		if !ok {
			t.Fatalf("missing %s", model)
		}
		if (cap.HostedWebSearchWireShape != "") != (model == "deepseek-v4-pro") || cap.IndependentSearch != (model != "deepseek-v4-pro") {
			t.Fatalf("%s: %+v", model, cap)
		}
	}
	if _, ok := DeepSeekModelCapabilities("deepseek-custom"); ok {
		t.Fatal("guessed unknown capability")
	}
}
