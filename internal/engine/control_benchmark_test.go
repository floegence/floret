package engine

import (
	"fmt"
	"testing"
)

type benchmarkControlPayloadOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

type benchmarkControlPayloadQuestion struct {
	Question string                          `json:"question"`
	Header   string                          `json:"header"`
	ID       string                          `json:"id"`
	Options  []benchmarkControlPayloadOption `json:"options"`
}

func BenchmarkCanonicalControlPayload(b *testing.B) {
	for _, count := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("questions_%d", count), func(b *testing.B) {
			questions := make([]benchmarkControlPayloadQuestion, count)
			for index := range questions {
				questions[index] = benchmarkControlPayloadQuestion{
					Question: "Which option should be used?",
					Header:   "Option",
					ID:       fmt.Sprintf("question-%d", index),
					Options: []benchmarkControlPayloadOption{
						{Label: "first", Description: "Use the first option"},
						{Label: "second", Description: "Use the second option"},
					},
				}
			}
			payload := map[string]any{"questions": questions, "required": true}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := canonicalControlPayload(payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
