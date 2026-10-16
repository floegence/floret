package requestestimate

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/deepseektokenizer"
)

func TestContentDistributionUsesDecodedTextAndActualModel(t *testing.T) {
	text := strings.Repeat(`<a href="/path?q=1&amp;x=2">工具输出</a>`+"\n", 100)
	raw, _ := json.Marshal(map[string]any{"input": []any{map[string]any{"type": "function_call_output", "call_id": "call", "output": text}}})
	got, err := Count("deepseek", "deepseek-flash", "openai_responses", raw)
	if err != nil {
		t.Fatal(err)
	}
	want, err := deepseektokenizer.Count(text)
	if err != nil {
		t.Fatal(err)
	}
	if got.Breakdown.ToolResultTokens != want || got.Breakdown.Tokenizer != "deepseek_v4" || got.Breakdown.Quality != "token_estimate" {
		t.Fatalf("got %+v, want raw tool output %d", got, want)
	}
}

func TestContentDistributionNeverGuessesProviderFromModel(t *testing.T) {
	raw := []byte(`{"messages":[{"role":"user","content":"hello"}]}`)
	for _, name := range []string{"prov_UUID", "openai_compatible", "anthropic"} {
		got, err := Count(name, "deepseek-flash", "openai_chat", raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.Breakdown.Quality != "relative_weight" {
			t.Fatalf("%s incorrectly claims model vocabulary: %+v", name, got)
		}
	}
}

func TestRecordedContextQualification(t *testing.T) {
	root := os.Getenv("FLORET_RECORDED_CONTEXT_DIR")
	if root == "" {
		data, err := os.ReadFile("testdata/deepseek_flash_qualification.json.gz")
		if err != nil {
			t.Fatal(err)
		}
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		raw, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		var rows []struct{ Name string }
		if err = json.Unmarshal(raw, &rows); err != nil {
			t.Fatal(err)
		}
		var records []json.RawMessage
		_ = json.Unmarshal(raw, &records)
		root = t.TempDir()
		for i, row := range rows {
			if err = os.WriteFile(filepath.Join(root, row.Name+".json"), records[i], 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	files, err := filepath.Glob(filepath.Join(root, "*.json"))
	if err != nil || len(files) != 24 {
		t.Fatal("exactly 24 recorded cases required")
	}
	errors := []float64{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var row struct {
			Name        string
			Payload     json.RawMessage
			NativeUsage struct {
				Input int64 `json:"input_tokens"`
			} `json:"native_usage"`
		}
		if err = json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		got, err := Count("deepseek", "deepseek-flash", "openai_responses", row.Payload)
		if err != nil {
			t.Fatal(err)
		}
		b := got.Breakdown
		n := b.SystemTokens + b.UserTokens + b.AssistantTokens + b.ToolCallTokens + b.ToolResultTokens + b.ToolDefinitionTokens + b.ModelContextTokens + b.CompactionTokens + b.ReasoningTokens + b.FramingTokens
		errors = append(errors, math.Abs(float64(n-row.NativeUsage.Input))/float64(row.NativeUsage.Input)*100)
		t.Logf("%s native=%d content=%d budget=%d diff=%.2f%% breakdown=%+v", row.Name, row.NativeUsage.Input, n, got.Prefix+got.Messages+got.Tools, 100*float64(n-row.NativeUsage.Input)/float64(row.NativeUsage.Input), b)
		if output := os.Getenv("FLORET_CONTEXT_COMPARISON_DIR"); output != "" {
			if err = os.MkdirAll(output, 0700); err != nil {
				t.Fatal(err)
			}
			data, _ := json.MarshalIndent(map[string]any{"name": row.Name, "native_input": row.NativeUsage.Input, "content": n, "estimate": got}, "", "  ")
			if err = os.WriteFile(filepath.Join(output, fmt.Sprintf("%s.json", row.Name)), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	sort.Float64s(errors)
	median, p95, maximum := (errors[11]+errors[12])/2, errors[22], errors[23]
	t.Logf("absolute relative error: median=%.3f%% p95=%.3f%% max=%.3f%%", median, p95, maximum)
	if median > 5 || p95 > 10 || maximum > 15 {
		t.Fatalf("fixed qualification thresholds exceeded")
	}

}

func TestProtocolContentAttributionAndBudgetIsolation(t *testing.T) {
	for _, tc := range []struct{ format, payload, path string }{
		{"openai_chat", `{"messages":[{"role":"user","content":"skills环境"}]}`, "/messages/0/content"},
		{"openai_responses", `{"input":[{"role":"user","content":[{"type":"input_text","text":"skills环境"}]}]}`, "/input/0/content/0/text"},
		{"anthropic_messages", `{"messages":[{"role":"user","content":[{"type":"text","text":"skills环境"}]}]}`, "/messages/0/content/0/text"},
	} {
		t.Run(tc.format, func(t *testing.T) {
			attrs := []Attribution{{Path: tc.path, Category: "model_context", Parts: []Part{{Start: 0, End: 6, Key: "skills"}, {Start: 6, End: 12, Key: "execution_environment"}}}}
			got, err := CountAttributed("openai", "gpt-4o", tc.format, []byte(tc.payload), attrs)
			if err != nil || got.Breakdown.Diagnostic != "" || got.Breakdown.UserTokens != 0 || got.Breakdown.ModelContextTokens <= 0 || len(got.Breakdown.Details.Items) != 2 {
				t.Fatalf("attribution %+v %v", got, err)
			}
			base, err := Count("openai", "gpt-4o", tc.format, []byte(tc.payload))
			if err != nil {
				t.Fatal(err)
			}
			attrs[0].Parts[0].End = 1
			invalid, err := CountAttributed("openai", "gpt-4o", tc.format, []byte(tc.payload), attrs)
			if err != nil || invalid.Breakdown.Source != "" || invalid.Breakdown.Diagnostic != "attribution_mismatch" || invalid.Messages != base.Messages {
				t.Fatalf("optional attribution changed budget: %+v base=%+v %v", invalid, base, err)
			}
			attrs[0].Path = "/missing"
			missing, err := CountAttributed("openai", "gpt-4o", tc.format, []byte(tc.payload), attrs)
			if err != nil || missing.Breakdown.Source != "" || missing.Messages != base.Messages {
				t.Fatalf("missing path changed budget: %+v %v", missing, err)
			}
		})
	}
}
func TestToolMetadataDoesNotEnterContentOrBudget(t *testing.T) {
	raw := []byte(`{"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"alias_read","parameters":{"type":"object","properties":{"cache_control":{"type":"string"}}},"annotations":{"resource":"private"}}}]}`)
	got, err := CountAttributed("deepseek", "deepseek-flash", "openai_chat", raw, []Attribution{{Path: "/tools/0", Category: "tool_definition", Key: "read", Source: "mcp", Namespace: "docs"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Breakdown.Details.Items[0].Key != "read" || got.Breakdown.Details.Items[0].Source != "mcp" {
		t.Fatalf("tool attribution %+v", got)
	}
	changed := strings.Replace(string(raw), "private", strings.Repeat("secret", 1000), 1)
	other, err := Count("deepseek", "deepseek-flash", "openai_chat", []byte(changed))
	if err != nil || got.Tools != other.Tools {
		t.Fatalf("metadata affected budget %+v %+v %v", got, other, err)
	}
}
func TestOpaqueAndMediaNeverClaimTextAccuracy(t *testing.T) {
	for _, tc := range []struct{ format, payload string }{
		{"openai_responses", `{"input":[{"type":"reasoning","encrypted_content":"opaque"}]}`},
		{"anthropic_messages", `{"messages":[{"role":"assistant","content":[{"type":"redacted_thinking","data":"opaque"}]}]}`},
		{"openai_chat", `{"messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"opaque"}}]}]}`},
		{"input_json", `{"messages":[{"role":"user","content":[{"type":"file","file_uri":"data:application/pdf;base64,opaque"}]}]}`},
	} {
		got, err := Count("openai", "gpt-4o", tc.format, []byte(tc.payload))
		if err != nil || got.Breakdown.Quality != "relative_weight" || got.Messages <= 0 {
			t.Fatalf("opaque %+v %v", got, err)
		}
	}
}
func TestPublishedDeepSeekToolTemplate(t *testing.T) {
	c := newCounter("deepseek", "deepseek-flash", nil)
	got, err := c.trustedTemplate(deepSeekToolTemplate)
	if err != nil || got != 221 {
		t.Fatalf("official reference template tokens=%d: %v", got, err)
	}
}

func TestSourceBoundariesDoNotRetokenizeMergedText(t *testing.T) {
	raw := []byte(`{"instructions":"abcdefgh","input":[]}`)
	base, err := Count("openai", "gpt-4o", "openai_responses", raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := CountAttributed("openai", "gpt-4o", "openai_responses", raw, []Attribution{{Path: "/instructions", Category: "system", Parts: []Part{{Start: 0, End: 3, Category: "system"}, {Start: 3, End: 8, Category: "compaction"}}}})
	if err != nil || got.Breakdown.SystemTokens+got.Breakdown.CompactionTokens != base.Breakdown.SystemTokens {
		t.Fatalf("source boundary changed text count: %+v base=%+v %v", got, base, err)
	}
}

func TestGenericTypedToolContentDoesNotCountTransportMetadata(t *testing.T) {
	raw := []byte(`{"messages":[{"role":"assistant","content":[{"type":"reasoning","text":"consider"},{"type":"tool_call","tool_name":"read","tool_call_id":"secret_id","args_json":"{}","json":"encoded_transport"}]},{"role":"tool","content":[{"type":"tool_result","text":"visible output","tool_call_id":"secret_id"}]}]}`)
	got, err := Count("remote", "model", "input_json", raw)
	if err != nil || got.Breakdown.ReasoningTokens <= 0 || got.Breakdown.ToolCallTokens <= 0 || got.Breakdown.ToolResultTokens <= 0 {
		t.Fatalf("typed generic %+v %v", got, err)
	}
	other, err := Count("remote", "model", "input_json", []byte(strings.ReplaceAll(string(raw), "secret_id", strings.Repeat("metadata", 1000))))
	if err != nil || other.Messages != got.Messages {
		t.Fatal("transport identity entered model text")
	}
}
