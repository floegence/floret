package requestestimate

import "testing"

func TestMediaSubsetIsCountedSeparatelyWithoutChangingInputBudget(t *testing.T) {
	result, err := Count("openai", "gpt-4o", "openai_responses", []byte(`{"input":[{"role":"user","content":[{"type":"input_text","text":"Inspect"},{"type":"input_file","file_data":"encoded"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Media < 4096 || result.Messages < result.Media {
		t.Fatalf("media subset=%+v", result)
	}
	plain, err := Count("openai", "gpt-4o", "openai_responses", []byte(`{"input":[{"role":"user","content":"Inspect"}]}`))
	if err != nil || plain.Media != 0 {
		t.Fatalf("plain input has media: %+v, %v", plain, err)
	}
}
