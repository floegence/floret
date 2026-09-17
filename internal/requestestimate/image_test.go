package requestestimate

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/png"
	"testing"
)

func TestImageBudgetsByModelAndDetail(t *testing.T) {
	var raw bytes.Buffer
	if err := png.Encode(&raw, image.NewRGBA(image.Rect(0, 0, 1024, 1024))); err != nil {
		t.Fatal(err)
	}
	url := "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw.Bytes())
	for _, tc := range []struct {
		model, detail string
		want          int64
	}{
		{"gpt-4o", "high", 765}, {"gpt-4o", "low", 85}, {"gpt-4o-mini", "high", 25501},
		{"gpt-5.4", "high", 1229}, {"gpt-4.1-mini", "auto", 1659}, {"gpt-6-astra", "high", 1229},
		{"unknown-model", "auto", 5120},
	} {
		c := counter{provider: "openai", model: tc.model}
		got, err := c.image(url, tc.detail)
		if err != nil || got != tc.want {
			t.Errorf("%s %s: %d %v want %d", tc.model, tc.detail, got, err, tc.want)
		}
	}
	c := counter{provider: "openai", model: "gpt-4o-mini"}
	if n, err := c.image("https://never-fetch.invalid/image", "auto"); err != nil || n != 48169 {
		t.Fatalf("remote image bound=%d %v", n, err)
	}
}
