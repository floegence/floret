package floret_test

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestV3PublicAPIMatchesDesignedBaseline(t *testing.T) {
	command := exec.Command("go", "run", "./internal/architecture/apibaseline", "-root", ".")
	command.Env = append(os.Environ(), "GOWORK=off")
	actual, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generate actual v3 API surface: %v\n%s", err, actual)
	}
	expected := readContractInput(t, "v3-public-api.txt")
	if string(actual) != string(expected) {
		t.Fatalf("public API differs from the manually designed v3 baseline; change the implementation or make an explicit API decision\n%s", firstSurfaceDifference(string(expected), string(actual)))
	}
}

func TestV3SymbolDecisionsCoverEveryV2Declaration(t *testing.T) {
	rows := readTSV(t, "v3-symbol-decisions.tsv", []string{"v2_declaration", "decision", "v3_declaration", "rationale"})
	v3Declarations := qualifiedContractDeclarations(readContractInput(t, "v3-public-api.txt"))
	decisions := make(map[string][]string, len(rows))
	for line, row := range rows {
		declaration := row[0]
		if declaration == "*" || declaration == "?" || strings.HasSuffix(declaration, ".*") {
			t.Fatalf("v3-symbol-decisions.tsv:%d uses a wildcard", line+2)
		}
		if _, duplicate := decisions[declaration]; duplicate {
			t.Fatalf("v3-symbol-decisions.tsv:%d duplicates %q", line+2, declaration)
		}
		if !slices.Contains([]string{"keep", "remove", "rename", "move"}, row[1]) {
			t.Fatalf("v3-symbol-decisions.tsv:%d has invalid decision %q", line+2, row[1])
		}
		if row[1] == "remove" && row[2] != "-" {
			t.Fatalf("v3-symbol-decisions.tsv:%d removed declaration must use '-' target", line+2)
		}
		if row[1] != "remove" && (row[2] == "" || row[2] == "-") {
			t.Fatalf("v3-symbol-decisions.tsv:%d retained declaration requires an exact v3 target", line+2)
		}
		if row[1] != "remove" && !v3Declarations[row[2]] {
			t.Fatalf("v3-symbol-decisions.tsv:%d target is absent from v3-public-api.txt: %s", line+2, row[2])
		}
		if strings.TrimSpace(row[3]) == "" {
			t.Fatalf("v3-symbol-decisions.tsv:%d requires a rationale", line+2)
		}
		decisions[declaration] = row
	}

	v2 := readContractInput(t, "v2-public-api.txt")
	scanner := bufio.NewScanner(strings.NewReader(string(v2)))
	var packagePath string
	for scanner.Scan() {
		declaration := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(declaration, "package ") {
			packagePath = strings.TrimPrefix(declaration, "package ")
			continue
		}
		if declaration == "" || strings.HasPrefix(declaration, "#") {
			continue
		}
		qualified := packagePath + " :: " + declaration
		if _, ok := decisions[qualified]; !ok {
			t.Errorf("v2 declaration has no explicit v3 decision: %s", qualified)
		}
		delete(decisions, qualified)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for unexpected := range decisions {
		t.Errorf("symbol decision does not identify an exact v2 declaration: %s", unexpected)
	}
}

func TestV3BehaviorContractCoversHostSurface(t *testing.T) {
	data := string(readContractInput(t, "v3-api-behavior.yaml"))
	for _, method := range []string{
		"Open", "Host.Threads", "Host.Thread", "Host.Shutdown",
		"Thread.Reader", "Thread.Lifecycle", "Thread.TurnExecutor", "Thread.Compactor", "Thread.SubAgentManager",
		"ThreadReader.Bootstrap", "ThreadReader.ReadAuthoritativeProjection", "Turns.ExecuteAdmission",
		"Threads.ListThreads", "Threads.CreateThread", "Thread.Snapshot", "Thread.Subscribe", "Subscription.Next",
		"Thread.ReadOverview", "Thread.ReadTurn", "Thread.ListTurns", "Thread.ReadAgentTodos",
		"Thread.ReadContext", "Thread.ReadApprovalQueue", "Thread.ReadProjection",
		"Thread.ListPendingToolTargets", "Thread.ListSubAgents", "Thread.SetTitle",
		"Thread.PendingToolRecovery", "Thread.InterruptedTurnRecovery",
		"Thread.ForkThread", "Thread.DeleteThread", "Turns.StartTurn", "Turns.RetryTurn",
		"Thread.Compact",
		"Turns.ContinuePendingTool", "Turns.RecordPendingToolOutcome", "Turns.ResolveApproval", "Turns.UpdateTodos",
		"SubAgents.SpawnSubAgent", "SubAgents.SendSubAgentMessage", "SubAgents.InterruptSubAgent",
		"SubAgents.WaitSubAgents", "SubAgents.CloseSubAgent",
		"Child.ReadTurn", "Child.ListTurns", "Child.PendingToolRecovery", "Child.InterruptedTurnRecovery",
		"PendingToolRecovery.Settle", "InterruptedTurnRecovery.Recover",
	} {
		block := behaviorBlock(data, method)
		if block == "" {
			t.Errorf("v3-api-behavior.yaml does not cover %s", method)
			continue
		}
		for _, field := range []string{"errors_is:", "errors_as:", "retryability:", "commit:", "replay:", "concurrency:", "cancellation:", "closing:"} {
			if !strings.Contains(block, "\n    "+field) {
				t.Errorf("behavior for %s omits %s", method, strings.TrimSuffix(field, ":"))
			}
		}
	}
}

func TestV3OwnershipMatrixCoversCanonicalAgentState(t *testing.T) {
	rows := readTSV(t, "v3-ownership-matrix.tsv", []string{"domain", "owner", "sink", "encoder", "payload_schema", "writer_call_path"})
	domains := map[string]bool{}
	for line, row := range rows {
		if row[1] != "floret" && row[1] != "redeven" {
			t.Fatalf("v3-ownership-matrix.tsv:%d invalid owner %q", line+2, row[1])
		}
		for column, value := range row {
			if strings.TrimSpace(value) == "" {
				t.Fatalf("v3-ownership-matrix.tsv:%d column %d is empty", line+2, column+1)
			}
		}
		if row[1] == "redeven" {
			for _, forbidden := range []string{"map[string]any", "json.RawMessage", "Floret DTO", "serialized any"} {
				if strings.Contains(row[4], forbidden) {
					t.Fatalf("v3-ownership-matrix.tsv:%d permits forbidden durable payload %q", line+2, forbidden)
				}
			}
		}
		domains[row[0]] = true
	}
	for _, domain := range []string{
		"admitted_messages_references", "thread_turn_run_lifecycle", "title", "approval", "todo",
		"tool_invocation_outcome", "pending_settlement", "artifact", "control_signal", "context_compaction",
		"provider_ledger_state", "prompt_cache", "subagent_hierarchy", "activity_projection",
		"persona_prompt_source", "provider_profile_credentials", "tool_policy_authorization", "routing", "read_state",
	} {
		if !domains[domain] {
			t.Errorf("ownership matrix omits %s", domain)
		}
	}
}

func TestV3ReadProvenanceCoversEveryConsumerClass(t *testing.T) {
	rows := readTSV(t, "v3-read-provenance.tsv", []string{"consumer_kind", "consumer", "floret_public_source", "product_projection", "owner"})
	kinds := map[string]bool{}
	for line, row := range rows {
		if row[4] != "floret" && row[4] != "redeven" {
			t.Fatalf("v3-read-provenance.tsv:%d invalid owner %q", line+2, row[4])
		}
		if row[4] == "floret" && !strings.Contains(row[2], "runtime.") {
			t.Fatalf("v3-read-provenance.tsv:%d Floret-owned fact lacks a public runtime source", line+2)
		}
		if strings.Contains(row[2], "github.com/floegence/floret/v3/internal/") || strings.HasPrefix(row[2], "internal/") {
			t.Fatalf("v3-read-provenance.tsv:%d depends on Floret internal API", line+2)
		}
		kinds[row[0]] = true
	}
	for _, kind := range []string{"go_endpoint", "background_decision", "bootstrap", "live_event", "typescript_reducer", "ui_field"} {
		if !kinds[kind] {
			t.Errorf("read provenance matrix omits %s consumers", kind)
		}
	}
}

func qualifiedContractDeclarations(data []byte) map[string]bool {
	declarations := map[string]bool{}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	var packagePath string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "package ") {
			packagePath = strings.TrimPrefix(line, "package ")
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		declarations[packagePath+" :: "+line] = true
	}
	return declarations
}

func readContractInput(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("internal", "architecture", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readTSV(t *testing.T, name string, header []string) map[int][]string {
	t.Helper()
	data := readContractInput(t, name)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	if !scanner.Scan() || !slices.Equal(strings.Split(scanner.Text(), "\t"), header) {
		t.Fatalf("%s has an invalid header", name)
	}
	rows := map[int][]string{}
	line := 1
	for scanner.Scan() {
		line++
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		row := strings.Split(scanner.Text(), "\t")
		if len(row) != len(header) {
			t.Fatalf("%s:%d has %d columns, want %d", name, line, len(row), len(header))
		}
		rows[line] = row
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatalf("%s has no decisions", name)
	}
	return rows
}

func behaviorBlock(data, method string) string {
	marker := "  - method: " + method + "\n"
	start := strings.Index(data, marker)
	if start < 0 {
		return ""
	}
	rest := data[start+len(marker):]
	if end := strings.Index(rest, "\n  - method: "); end >= 0 {
		rest = rest[:end]
	}
	return marker + rest
}
