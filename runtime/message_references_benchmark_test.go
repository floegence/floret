package runtime

import (
	"fmt"
	"testing"

	"github.com/floegence/floret/internal/session"
)

var benchmarkMessageReferencesSink []session.MessageReference

// BenchmarkMessageReferences measures the runtime admission boundary for
// canonical references: copy and validate the public input, then map it to the
// Floret canonical message contract without resolving host resources.
func BenchmarkMessageReferences(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("references/%d", size), func(b *testing.B) {
			input := TurnInput{Text: "inspect selected context", References: benchmarkMessageReferences(size)}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				normalized, err := normalizeTurnInput(input)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkMessageReferencesSink = sessionMessageReferences(normalized.References)
			}
		})
	}
}

func benchmarkMessageReferences(size int) []MessageReference {
	references := make([]MessageReference, size)
	for index := range references {
		reference := MessageReference{
			ReferenceID: fmt.Sprintf("benchmark:%d", index),
			Label:       fmt.Sprintf("Reference %d", index),
			Text:        fmt.Sprintf("selected context %d", index),
		}
		switch index % 5 {
		case 1:
			reference.Kind = MessageReferenceFile
			reference.Text = fmt.Sprintf("/workspace/file-%d.go", index)
			reference.ResourceRef = fmt.Sprintf("host-resource:file:%d", index)
		case 2:
			reference.Kind = MessageReferenceDirectory
			reference.Text = fmt.Sprintf("/workspace/dir-%d", index)
			reference.ResourceRef = fmt.Sprintf("host-resource:directory:%d", index)
		case 3:
			reference.Kind = MessageReferenceTerminal
			reference.Text = fmt.Sprintf("terminal output %d", index)
		case 4:
			reference.Kind = MessageReferenceProcess
			reference.Text = fmt.Sprintf("process snapshot %d", index)
		default:
			reference.Kind = MessageReferenceText
		}
		references[index] = reference
	}
	return references
}
