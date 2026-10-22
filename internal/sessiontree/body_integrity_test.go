package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func TestCanonicalStreamingJSONStringMatchesPublishedEncoding(t *testing.T) {
	texts := []string{" \t\r\n\u2003", "<>&\u2028\u2029\"\\\x00\b\f\n\r\t", "中文🌸é", "\xff\xc0\xaf\xed\xa0\x80\xe2\x82"}
	for _, text := range append([]string(nil), texts...) {
		for offset := 0; offset < utf8.UTFMax; offset++ {
			texts = append(texts, strings.Repeat(" ", canonicalBodyReadBytes-offset)+text+strings.Repeat("é", canonicalBodyReadBytes/2))
		}
	}
	for index, source := range texts {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			backend := newMigrationTestBackend()
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				ref, err := retainCanonicalBody(t.Context(), tx, source)
				if err != nil {
					return err
				}
				var encoded bytes.Buffer
				facts, err := writeCanonicalJSONString(t.Context(), tx, &encoded, ref)
				if err != nil {
					return err
				}
				want, err := json.Marshal(source)
				if err != nil {
					return err
				}
				if !bytes.Equal(encoded.Bytes(), want) || facts.runes != utf8.RuneCountInString(source) || facts.nonSpace != (strings.TrimSpace(source) != "") {
					t.Fatalf("streamed encoding or content facts differ: bytes=%v facts=%+v", bytes.Equal(encoded.Bytes(), want), facts)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func integrityStoredFixture(t *testing.T, entry Entry) (*migrationTestBackend, Entry) {
	t.Helper()
	backend := newMigrationTestBackend()
	entry.ID, entry.ThreadID = "entry", "thread"
	entry = PrepareEntry(entry)
	var stored canonicalStoredEntry
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		key := backendDomainEntryKey(entry.ThreadID, 0)
		if err := putCanonicalStoredEntry(t.Context(), tx, backendDomainV31Format, key, entry.ID, entry.ThreadID, 0, entry); err != nil {
			return err
		}
		record, _, err := readBackendDomainRecord(tx, backendDomainV31Format, key, backendDomainRecordEntry, entry.ID, entry.ThreadID, 0)
		if err != nil {
			return err
		}
		stored, err = decodeCanonicalStoredEntry(backendDomainV31Format, record)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stored.Entry.bodyFields = stored.Fields
	return backend, stored.Entry
}

func TestCanonicalStreamingIntegrityMatchesCompleteValidation(t *testing.T) {
	content := strings.Repeat("<>&🌸\u2028\u2029\r\n\"\\", 9000)
	start, end := 3, utf8.RuneCountInString(content)
	entries := []Entry{
		{Type: EntryUserMessage, Message: session.Message{Role: session.User}},
		{Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: content, Reasoning: "private\nreasoning", Sources: []citation.Source{{Title: "Source", URL: "https://example.com", StartIndex: &start, EndIndex: &end}}}},
		{Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "read", ToolArgs: content}},
		{Type: EntryToolResult, Message: session.Message{Role: session.Tool, Content: content, ControlSignal: &session.ControlSignalView{Name: "done", OutputText: content, Payload: map[string]any{"nested": []any{true, nil, "<&>"}}}}},
		{Type: EntryToolResult, Message: session.Message{Role: session.Tool, Activity: &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Operation: "read", Status: "success", Rows: []tools.StructuredActivityRow{{Title: "File", Content: content[:60000], Format: tools.StructuredActivityRowFormatCode}}}}}},
		{Type: EntryCompaction, Summary: content, Error: content, Metadata: map[string]string{"z": "last", "a": "first"}, Payload: json.RawMessage("{\"z\": 1, \"a\":\"<>&\"}")},
	}
	for i, original := range entries {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			backend, stored := integrityStoredFixture(t, original)
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				complete, err := materializeCanonicalStoredEntry(t.Context(), tx, canonicalStoredEntry{Entry: stored, Fields: stored.bodyFields})
				if err != nil || ValidateEntryIntegrity(complete) != nil {
					t.Fatalf("complete validation: %v", err)
				}
				_, _, err = validateCanonicalStoredIntegrity(t.Context(), tx, stored)
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCanonicalStreamingIntegrityRejectsCorruption(t *testing.T) {
	for _, damage := range []string{"body", "hash", "source_range", "source_role", "source_duplicate", "source_normalization", "metadata"} {
		t.Run(damage, func(t *testing.T) {
			start, end := 0, 2
			backend, stored := integrityStoredFixture(t, Entry{Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "🌸x", Sources: []citation.Source{{URL: "https://example.com", StartIndex: &start, EndIndex: &end}}}})
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				switch damage {
				case "body":
					ref := stored.bodyFields["content"]
					payload, _ := json.Marshal([]byte("🌸y"))
					raw, err := storagecodec.EncodeEnvelope("sessiontree-v30-body-block", payload)
					if err != nil {
						return err
					}
					if err := tx.Put(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, 0), raw); err != nil {
						return err
					}
				case "hash":
					stored.RawHash = stableHash("different")
				case "metadata":
					stored.Metadata = map[string]string{"altered": "yes"}
				default:
					complete, err := materializeCanonicalStoredEntry(t.Context(), tx, canonicalStoredEntry{Entry: stored, Fields: stored.bodyFields})
					if err != nil {
						return err
					}
					switch damage {
					case "source_range":
						bad := 3
						complete.Message.Sources[0].EndIndex = &bad
					case "source_role":
						complete.Message.Role = session.User
					case "source_duplicate":
						complete.Message.Sources = append(complete.Message.Sources, complete.Message.Sources[0])
					case "source_normalization":
						complete.Message.Sources[0].Title = " untrimmed "
					}
					complete = PrepareEntry(complete)
					if ValidateEntryIntegrity(complete) == nil {
						t.Fatal("original validator accepted malformed source")
					}
					stored.Message = complete.Message
					stored.Message.Content = ""
					stored.RawHash = complete.RawHash
				}
				_, _, err := validateCanonicalStoredIntegrity(t.Context(), tx, stored)
				if !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("corruption accepted: %v", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCanonicalStreamingIntegrityHonorsCancellation(t *testing.T) {
	backend, stored := integrityStoredFixture(t, Entry{Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("text", 40000)}})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, _, err := validateCanonicalStoredIntegrity(ctx, cancelIntegrityReadTx{ReadTx: tx, cancel: cancel}, stored)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type cancelIntegrityReadTx struct {
	spi.ReadTx
	cancel context.CancelFunc
}

func (tx cancelIntegrityReadTx) Get(namespace string, key []byte) ([]byte, error) {
	value, err := tx.ReadTx.Get(namespace, key)
	if namespace == canonicalBodyNamespace {
		tx.cancel()
	}
	return value, err
}
