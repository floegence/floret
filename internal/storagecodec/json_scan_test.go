package storagecodec

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func scanTestJSON(raw []byte, budget int64, restart bool) (map[string]string, error) {
	state := JSONScanState{}
	values := map[string]string{}
	for iteration := 0; iteration <= len(raw)+2; iteration++ {
		before := state.Offset
		err := ScanJSON(bytes.NewReader(raw[state.Offset:]), &state, budget, 5,
			func(span JSONSpan) ([]byte, error) { return raw[span.Offset : span.Offset+span.Length], nil },
			func(path []string, span JSONSpan, _ byte) error {
				key, _ := json.Marshal(path)
				values[string(key)] = string(raw[span.Offset : span.Offset+span.Length])
				return nil
			})
		if err != nil {
			return values, err
		}
		if state.Offset == int64(len(raw)) && state.Done {
			return values, nil
		}
		if state.Offset == before {
			return values, io.ErrNoProgress
		}
		if restart {
			encoded, err := json.Marshal(state)
			if err != nil {
				return values, err
			}
			state = JSONScanState{}
			if err := json.Unmarshal(encoded, &state); err != nil {
				return values, err
			}
		}
	}
	return values, errors.New("scanner did not finish")
}

func TestJSONScanResumesEveryGrammarBoundary(t *testing.T) {
	inputs := []string{
		"{}", "[]", "null", "true", "false", "0", "-0.13E-19", "12345678901234567890",
		" {\"foo\": [0, 1, -2.3e+4, true, false, null, {\"escaped\\\"key\": \"\\u0041\\\\中文🌸\"}], \"empty\": {}} \n",
	}
	for _, raw := range inputs {
		want, err := scanTestJSON([]byte(raw), int64(len(raw)+1), false)
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		for budget := int64(1); budget <= int64(len(raw)+1); budget++ {
			got, err := scanTestJSON([]byte(raw), budget, true)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("budget=%d raw=%q: got=%v want=%v err=%v", budget, raw, got, want, err)
			}
		}
	}
}

func TestJSONScanRejectsInvalidSyntax(t *testing.T) {
	for _, raw := range []string{"", "{", "[", "tru", "null true", "[1,]", "{\"a\":1,}", "{\"a\" 1}", "{x:2}", "[01]", "[1.]", "[1e]", "[1e+]", "[--1]", "[.1]", "[+1]", "[1 2]", "\"bad\\x\"", "\"bad\\u00g1\"", "\"raw\nline\"", "[falsex]", "[] trailing"} {
		for _, budget := range []int64{1, 7, 1000} {
			if _, err := scanTestJSON([]byte(raw), budget, true); err == nil {
				t.Fatalf("accepted %q with budget %d", raw, budget)
			}
		}
	}
}

func TestJSONScanDoesNotRetainLargeBody(t *testing.T) {
	raw := []byte("{\"payload\":\"" + strings.Repeat("body\\n", 100000) + "\"}")
	state := JSONScanState{}
	var spans []JSONSpan
	for state.Offset < int64(len(raw)) {
		err := ScanJSON(bytes.NewReader(raw[state.Offset:]), &state, 32768, 1,
			func(span JSONSpan) ([]byte, error) { return raw[span.Offset : span.Offset+span.Length], nil },
			func(_ []string, span JSONSpan, _ byte) error { spans = append(spans, span); return nil })
		if err != nil {
			t.Fatal(err)
		}
		checkpoint, _ := json.Marshal(state)
		if len(checkpoint) > 1024 {
			t.Fatalf("checkpoint retained body: %d", len(checkpoint))
		}
	}
	if !state.Done || len(spans) != 2 || spans[0].Length != int64(len(raw)-12) {
		t.Fatalf("state=%+v spans=%v", state, spans)
	}
}

func FuzzJSONScanMatchesStandardSyntax(f *testing.F) {
	for _, raw := range []string{"{}", "[1,2]", "{\"a\":null}", "-1.5e10", "{\"a\":\"\\u1234\"}", "\"中文\""} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 32768 {
			t.Skip()
		}
		_, err := scanTestJSON(raw, 17, false)
		if (err == nil) != json.Valid(raw) {
			t.Fatalf("valid=%v err=%v input=%q", json.Valid(raw), err, raw)
		}
	})
}

func TestJSONScanRejectsCorruptCheckpoints(t *testing.T) {
	states := []JSONScanState{
		{Offset: -1}, {Done: true}, {Started: true, Offset: 1},
		{Started: true, Offset: 1, Token: 's', TokenKey: true},
		{Started: true, Offset: 1, Token: 'l', Literal: "true", LiteralAt: -1},
		{Started: true, Offset: 1, Token: 'l', Literal: "true", LiteralAt: 4},
		{Started: true, Offset: 1, Token: 'n', Number: 9},
		{Started: true, Offset: 1, Token: 's', Unicode: 5},
		{Started: true, Offset: 1, Frames: []JSONScanFrame{{Kind: '{', State: 5, Path: []string{}}}},
		{Started: true, Offset: 1, Frames: []JSONScanFrame{{Kind: '[', State: 3, Path: []string{}}}},
		{Started: true, Offset: 1, Frames: []JSONScanFrame{{Kind: '{', Path: []string{"forged"}}}},
	}
	for index, checkpoint := range states {
		before, _ := json.Marshal(checkpoint)
		err := ScanJSON(strings.NewReader("\"}"), &checkpoint, 100, 4,
			func(JSONSpan) ([]byte, error) { t.Fatal("corrupt checkpoint read source"); return nil, nil },
			func([]string, JSONSpan, byte) error { t.Fatal("corrupt checkpoint published locator"); return nil })
		if err == nil {
			t.Fatalf("checkpoint %d accepted", index)
		}
		after, _ := json.Marshal(checkpoint)
		if !bytes.Equal(before, after) {
			t.Fatalf("checkpoint %d mutated", index)
		}
	}
}
