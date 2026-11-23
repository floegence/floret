package requestestimate

import "testing"

func TestEstimateRequiresExactlyOneJSONObject(t *testing.T) {
	const valid = `{"messages":[{"role":"user","content":"hello"}]}`
	for _, input := range []string{valid, "\n " + valid + " \t\r\n"} {
		if _, err := Count("openai", "gpt-5-mini", "openai_chat", []byte(input)); err != nil {
			t.Fatalf("reject valid input %q: %v", input, err)
		}
	}
	for _, input := range []string{"null", "[]", "{", valid + "{}", valid + " \n null", valid + "\u00a0", valid + "\x00", valid + "\v"} {
		if _, err := Count("openai", "gpt-5-mini", "openai_chat", []byte(input)); err == nil {
			t.Fatalf("accept invalid input %q", input)
		}
	}
}
