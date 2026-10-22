package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type canonicalBodyFacts struct {
	runes    int
	nonSpace bool
}

// Hash the exact released JSON field order and escaping while reading text in
// fixed-size chunks. The body digest and entry checksum are both verified.
func validateCanonicalStoredIntegrity(ctx context.Context, tx spi.ReadTx, entry Entry) (Entry, map[string]canonicalBodyFacts, error) {
	facts := map[string]canonicalBodyFacts{}
	if ref, found := entry.bodyFields["activity_json"]; found {
		text, err := materializeCanonicalBody(ctx, tx, ref)
		if err != nil {
			return Entry{}, nil, err
		}
		if err := restoreCanonicalField(&entry, "activity_json", text); err != nil {
			return Entry{}, nil, err
		}
	}
	hash := sha256.New()
	var encode func(reflect.Value, string) error
	encode = func(value reflect.Value, path string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		field := canonicalRawBodyField(path)
		if ref, stored := entry.bodyFields[field]; stored && field != "activity_json" {
			var err error
			facts[field], err = writeCanonicalJSONString(ctx, tx, hash, ref)
			return err
		}
		// Only these three structs own segmented string fields. All other values
		// retain encoding/json's exact custom encoding and canonical key order.
		if value.Kind() == reflect.Pointer && !value.IsNil() && value.Type().Elem() == reflect.TypeFor[session.ControlSignalView]() {
			value = value.Elem()
		}
		if value.Type() != reflect.TypeFor[canonicalEntryRaw]() && value.Type() != reflect.TypeFor[session.Message]() && value.Type() != reflect.TypeFor[session.ControlSignalView]() {
			raw, err := json.Marshal(value.Interface())
			if err != nil {
				return err
			}
			_, err = hash.Write(raw)
			return err
		}
		io.WriteString(hash, "{")
		count := 0
		for i := 0; i < value.NumField(); i++ {
			typeField, item := value.Type().Field(i), value.Field(i)
			name, options, _ := strings.Cut(typeField.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = typeField.Name
			}
			next := path + "/" + name
			_, stored := entry.bodyFields[canonicalRawBodyField(next)]
			if !stored && options == "omitempty" && emptyCanonicalJSONValue(item) {
				continue
			}
			if count > 0 {
				io.WriteString(hash, ",")
			}
			raw, _ := json.Marshal(name)
			hash.Write(raw)
			io.WriteString(hash, ":")
			if err := encode(item, next); err != nil {
				return err
			}
			count++
		}
		io.WriteString(hash, "}")
		return nil
	}
	if err := encode(reflect.ValueOf(canonicalRawValue(entry)), ""); err != nil {
		return Entry{}, nil, err
	}
	if hex.EncodeToString(hash.Sum(nil)) != entry.RawHash {
		return Entry{}, nil, ErrAuthorityCorrupt
	}
	if err := ValidateEntryMessageAttachments(entry); err != nil {
		return Entry{}, nil, ErrAuthorityCorrupt
	}
	// Citation coordinates need only the complete Unicode count, not the text.
	if len(entry.Message.Sources) > 0 {
		if entry.Type != EntryAssistantMessage || entry.Message.Role != session.Assistant {
			return Entry{}, nil, ErrAuthorityCorrupt
		}
		count := facts["content"].runes
		if _, stored := entry.bodyFields["content"]; !stored {
			count = utf8.RuneCountInString(entry.Message.Content)
		}
		normalized := citation.Normalize(entry.Message.Sources)
		if len(normalized) != len(entry.Message.Sources) {
			return Entry{}, nil, ErrAuthorityCorrupt
		}
		for i, source := range normalized {
			if !citation.Equal(source, entry.Message.Sources[i]) || source.EndIndex != nil && *source.EndIndex > count {
				return Entry{}, nil, ErrAuthorityCorrupt
			}
		}
	}
	validation := entry
	validation.Message.Sources = nil
	if entry.Type == EntryModelContext && entry.bodyFields["content"].Bytes > 0 {
		return Entry{}, nil, ErrAuthorityCorrupt
	}
	if err := ValidateEntryMessageReferences(validation); err != nil {
		return Entry{}, nil, ErrAuthorityCorrupt
	}
	if entry.Type == EntryCustom && entry.Metadata["kind"] == HostedToolEntryKind {
		if _, err := HostedToolObservation(entry); err != nil {
			return Entry{}, nil, ErrAuthorityCorrupt
		}
	}
	return entry, facts, nil
}

func canonicalRawBodyField(path string) string {
	switch path {
	case "/message/Content":
		return "content"
	case "/message/Reasoning":
		return "reasoning"
	case "/message/ToolArgs":
		return "arguments"
	case "/message/control_signal/output_text":
		return "control_output"
	case "/message/activity":
		return "activity_json"
	case "/summary":
		return "summary"
	case "/error":
		return "error"
	default:
		return ""
	}
}

func emptyCanonicalJSONValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	}
	return false
}

func writeCanonicalJSONString(ctx context.Context, tx spi.ReadTx, writer io.Writer, ref canonicalBodyRef) (canonicalBodyFacts, error) {
	facts := canonicalBodyFacts{}
	if _, err := io.WriteString(writer, "\""); err != nil {
		return facts, err
	}
	var carry []byte
	write := func(part []byte) error {
		for remaining := part; len(remaining) > 0; {
			r, size := utf8.DecodeRune(remaining)
			facts.runes++
			facts.nonSpace = facts.nonSpace || !unicode.IsSpace(r)
			remaining = remaining[size:]
		}
		raw, err := json.Marshal(string(part))
		if err != nil {
			return err
		}
		_, err = writer.Write(raw[1 : len(raw)-1])
		return err
	}
	err := visitCanonicalBody(ctx, tx, ref, func(chunk []byte) error {
		part := append(carry, chunk...)
		end := len(part)
		for i := max(0, end-utf8.UTFMax+1); i < end; i++ {
			if utf8.RuneStart(part[i]) && !utf8.FullRune(part[i:]) {
				end = i
				break
			}
		}
		if err := write(part[:end]); err != nil {
			return err
		}
		carry = append([]byte(nil), part[end:]...)
		return nil
	})
	if err == nil && len(carry) > 0 {
		err = write(carry)
	}
	if err != nil {
		return facts, err
	}
	_, err = io.WriteString(writer, "\"")
	return facts, err
}

func canonicalValidationEntry(ctx context.Context, tx spi.ReadTx, entry Entry) (Entry, canonicalBodyFacts, error) {
	validated, facts, err := validateCanonicalStoredIntegrity(ctx, tx, entry)
	if err != nil {
		return Entry{}, canonicalBodyFacts{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	if entry.Type == EntryParentEventsAdmitted {
		if ref, found := entry.bodyFields["content"]; found {
			if ref.Bytes > ParentEventBatchMaxBytes {
				return Entry{}, canonicalBodyFacts{}, ErrAuthorityCorrupt
			}
			validated.Message.Content, err = materializeCanonicalBody(ctx, tx, ref)
			if err != nil {
				return Entry{}, canonicalBodyFacts{}, err
			}
		}
	}
	return validated, facts["content"], nil
}
