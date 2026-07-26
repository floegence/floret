#!/usr/bin/env bash
set -euo pipefail

readonly module_path="github.com/floegence/floret"
readonly smoke_examples=(
  "minimal-durable-host"
  "custom-model-gateway"
  "tool-effect-approval"
  "startup-recovery"
  "store-maintenance-host"
)
readonly scaffold_profiles=(
  "memory"
  "durable-basic"
  "approval"
  "production-recovery"
  "subagent"
)

usage() {
  cat >&2 <<'EOF'
usage: scripts/check_published_release_adoption.sh <exact-tag>
       scripts/check_published_release_adoption.sh --check

Validate one published Floret tag from a blank downstream module. --check
validates the embedded templates without resolving a Floret release.
EOF
}

fail() {
  printf 'published release adoption: %s\n' "$*" >&2
  exit 1
}

assert_workspace_disabled() {
  case $(go env GOWORK) in
    off | /dev/null) ;;
    *) fail "GOWORK must resolve to off" ;;
  esac
}

write_consumer_test() {
  local destination=$1
  cat >"${destination}" <<'EOF'
package adoption_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/florettest"
	"github.com/floegence/floret/runtime"
)

func TestPublishedModelGatewayContract(t *testing.T) {
	florettest.RunModelGatewayContract(t, florettest.ScriptedModelGatewayFactory)
}

func TestPublishedDurableHostRestartAndStoreMaintenance(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	startup, err := runtime.StartSQLiteStore(ctx, path, runtime.SQLiteStartupRequest{
		MigrationPolicy: runtime.SQLiteMigrationRefuse,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startup.Inspection.State != runtime.SQLiteStoreStateMissing || startup.Store == nil {
		t.Fatalf("initial startup = %#v", startup)
	}
	store := startup.Store
	fixture, err := florettest.PopulateStoreFixture(ctx, store, florettest.StoreFixtureInput{
		ThreadID:       "published-thread",
		CreateIntentID: "published-create",
		Turns: []florettest.StoreFixtureTurn{{
			Request: runtime.RunTurnRequest{
				TurnID: "published-turn", RunID: "published-run",
				Input: runtime.TurnInput{Text: "exercise the published module"},
			},
			ModelSteps: []florettest.ModelStep{{Events: []runtime.ModelEvent{
				{Type: runtime.ModelEventDelta, Text: "published response"},
				{Type: runtime.ModelEventDone, Reason: "stop"},
			}}},
		}},
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if len(fixture.Turns) != 1 || fixture.Turns[0].Status != runtime.TurnStatusCompleted {
		_ = store.Close()
		t.Fatalf("fixture result = %#v", fixture)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStartup, err := runtime.StartSQLiteStore(ctx, path, runtime.SQLiteStartupRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if reopenedStartup.Store == nil || reopenedStartup.Verification.Inspection.State != runtime.SQLiteStoreStateCurrent ||
		reopenedStartup.Verification.Inspection.LeasePolicyState != runtime.SQLiteStoreLeasePolicyMatches {
		t.Fatalf("reopened startup = %#v", reopenedStartup)
	}
	for _, check := range reopenedStartup.Verification.Checks {
		if !check.Passed {
			t.Fatalf("verification check = %#v", check)
		}
	}
	reopened := reopenedStartup.Store
	defer reopened.Close()

	var readBinder *runtime.ThreadReadHostBinder
	if err := runtime.ConfigureHostCapabilities(reopened, func(bootstrap *runtime.HostBootstrap) error {
		var configureErr error
		readBinder, configureErr = runtime.NewThreadReadHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := readBinder.NewHost(ctx, "published-thread")
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.ListThreadTurns(ctx, runtime.ListThreadTurnsRequest{
		ThreadID: "published-thread", Tail: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := page.Validate(); err != nil {
		t.Fatalf("turn page validation: %v", err)
	}
	if len(page.Turns) != 1 || page.Turns[0].TurnID != "published-turn" ||
		len(page.Turns[0].Projection.Segments) != 1 ||
		page.Turns[0].Projection.Segments[0].Kind != runtime.ThreadTurnProjectionSegmentAssistantText ||
		page.Turns[0].Projection.Segments[0].Text != "published response" {
		t.Fatalf("restarted turn page = %#v", page)
	}
	exact, err := reader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{
		ThreadID: "published-thread", TurnID: "published-turn",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := exact.Validate(); err != nil {
		t.Fatalf("exact turn validation: %v", err)
	}
	overview, err := reader.ReadThreadOverview(ctx, "published-thread")
	if err != nil {
		t.Fatal(err)
	}
	if err := overview.Validate(); err != nil {
		t.Fatalf("overview validation: %v", err)
	}
	if exact.TurnID != page.Turns[0].TurnID || exact.RunID != page.Turns[0].RunID ||
		exact.UserInput != page.Turns[0].UserInput || exact.Status != page.Turns[0].Status ||
		exact.ThroughOrdinal != page.Turns[0].ThroughOrdinal {
		t.Fatalf("exact/list mismatch: exact=%#v listed=%#v", exact, page.Turns[0])
	}
}

func TestPublishedMemoryExactReadAndNotFound(t *testing.T) {
	ctx := context.Background()
	store := runtime.NewMemoryStore()
	defer store.Close()
	if _, err := florettest.PopulateStoreFixture(ctx, store, florettest.StoreFixtureInput{
		ThreadID: "memory-thread", CreateIntentID: "memory-create",
		Turns: []florettest.StoreFixtureTurn{{
			Request: runtime.RunTurnRequest{TurnID: "memory-turn", RunID: "memory-run", Input: runtime.TurnInput{Text: "memory"}},
			ModelSteps: []florettest.ModelStep{{Events: []runtime.ModelEvent{{Type: runtime.ModelEventDelta, Text: "ok"}, {Type: runtime.ModelEventDone, Reason: "stop"}}}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	var reader *runtime.ThreadReadHost
	if err := runtime.ConfigureHostCapabilities(store, func(bootstrap *runtime.HostBootstrap) error {
		binder, err := runtime.NewThreadReadHostBinder(bootstrap)
		if err != nil {
			return err
		}
		reader, err = binder.NewHost(ctx, "memory-thread")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	turn, err := reader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: "memory-thread", TurnID: "memory-turn"})
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: "memory-thread", TurnID: "missing"}); !errors.Is(err, runtime.ErrTurnNotFound) {
		t.Fatalf("missing memory turn error = %v", err)
	}
}

func TestPublishedSubAgentExactReadAuthority(t *testing.T) {
	ctx := context.Background()
	store := runtime.NewMemoryStore()
	defer store.Close()
	var createBinder *runtime.ThreadCreateHostBinder
	var subAgentBinder *runtime.SubAgentHostBinder
	var readBinder *runtime.SubAgentReadHostBinder
	if err := runtime.ConfigureHostCapabilities(store, func(bootstrap *runtime.HostBootstrap) error {
		var err error
		createBinder, err = runtime.NewThreadCreateHostBinder(bootstrap)
		if err != nil {
			return err
		}
		subAgentBinder, err = runtime.NewSubAgentHostBinder(bootstrap)
		if err != nil {
			return err
		}
		readBinder, err = runtime.NewSubAgentReadHostBinder(bootstrap)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	creator, err := createBinder.Bind("root", "create-root")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.CreateThread(ctx, runtime.CreateThreadRequest{ThreadID: "root", CreateIntentID: "create-root"}); err != nil {
		t.Fatal(err)
	}
	options := runtime.SubAgentHostOptions{Config: config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done", SystemPrompt: "published child"}, SubAgentRunTimeout: 2 * time.Second}
	rootFactory, err := subAgentBinder.Bind("root")
	if err != nil {
		t.Fatal(err)
	}
	rootHost, err := rootFactory.NewHost(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	child, err := rootHost.SpawnSubAgent(ctx, runtime.SpawnSubAgentRequest{PublicationID: "publish-child", ParentThreadID: "root", ParentTurnID: "root-turn", ThreadID: "child", TaskName: "child", Message: "child", ForkMode: runtime.SubAgentForkNone})
	if err != nil {
		t.Fatal(err)
	}
	waited, err := rootHost.WaitSubAgents(ctx, runtime.WaitSubAgentsRequest{ParentThreadID: "root", ChildThreadIDs: []runtime.ThreadID{child.ThreadID}, Timeout: 2 * time.Second})
	if err != nil || waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].LatestTurnID == "" {
		t.Fatalf("child wait = %#v, err = %v", waited, err)
	}
	child = waited.Snapshots[0]
	sibling, err := rootHost.SpawnSubAgent(ctx, runtime.SpawnSubAgentRequest{PublicationID: "publish-sibling", ParentThreadID: "root", ParentTurnID: "root-turn", ThreadID: "sibling", TaskName: "sibling", Message: "sibling", ForkMode: runtime.SubAgentForkNone})
	if err != nil {
		t.Fatal(err)
	}
	if waited, err = rootHost.WaitSubAgents(ctx, runtime.WaitSubAgentsRequest{ParentThreadID: "root", ChildThreadIDs: []runtime.ThreadID{sibling.ThreadID}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("sibling wait = %#v, err = %v", waited, err)
	}
	childFactory, err := subAgentBinder.Bind(child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	childHost, err := childFactory.NewHost(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := childHost.SpawnSubAgent(ctx, runtime.SpawnSubAgentRequest{PublicationID: "publish-grandchild", ParentThreadID: child.ThreadID, ParentTurnID: child.LatestTurnID, ThreadID: "grandchild", TaskName: "grandchild", Message: "grandchild", ForkMode: runtime.SubAgentForkNone})
	if err != nil {
		t.Fatal(err)
	}
	waited, err = childHost.WaitSubAgents(ctx, runtime.WaitSubAgentsRequest{ParentThreadID: child.ThreadID, ChildThreadIDs: []runtime.ThreadID{grandchild.ThreadID}, Timeout: 2 * time.Second})
	if err != nil || waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].LatestTurnID == "" {
		t.Fatalf("grandchild wait = %#v, err = %v", waited, err)
	}
	grandchild = waited.Snapshots[0]
	rootReader, err := readBinder.NewHost(ctx, "root")
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct{ threadID runtime.ThreadID; turnID runtime.TurnID }{{child.ThreadID, child.LatestTurnID}, {grandchild.ThreadID, grandchild.LatestTurnID}} {
		turn, err := rootReader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: target.threadID, TurnID: target.turnID})
		if err != nil {
			t.Fatal(err)
		}
		if err := turn.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	childReader, err := readBinder.NewHost(ctx, child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []runtime.ThreadID{"root", child.ThreadID, sibling.ThreadID} {
		if _, err := childReader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: target, TurnID: child.LatestTurnID}); !errors.Is(err, runtime.ErrSubAgentNotFound) {
			t.Fatalf("unauthorized target %q error = %v", target, err)
		}
	}
}
EOF
}

write_verifier() {
  local destination=$1
  cat >"${destination}" <<'EOF'
package main

import (
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type moduleList struct {
	Path    string
	Version string
	Replace *moduleList
}

type moduleDownload struct {
	Path     string
	Version  string
	Info     string
	GoMod    string
	Zip      string
	Dir      string
	Sum      string
	GoModSum string
	Error    string
}

type editedGoMod struct {
	Module  struct{ Path string }
	Require []struct {
		Path    string
		Version string
	}
	Replace []json.RawMessage
}

func main() {
	if len(os.Args) == 4 && os.Args[1] == "--check-consumer" {
		checkConsumerImports(os.Args[2], os.Args[3])
		return
	}
	if len(os.Args) != 8 {
		fatalf("usage: verifier <module> <version> <list-json> <download-json> <gomod-json> <consumer-root> <dir-output>")
	}
	modulePath, version := os.Args[1], os.Args[2]
	var listed moduleList
	decodeFile(os.Args[3], &listed)
	if listed.Path != modulePath || listed.Version != version || listed.Replace != nil {
		fatalf("resolved module = %#v, want %s %s without replacement", listed, modulePath, version)
	}
	var downloaded moduleDownload
	decodeFile(os.Args[4], &downloaded)
	if downloaded.Error != "" {
		fatalf("module download error: %s", downloaded.Error)
	}
	if downloaded.Path != modulePath || downloaded.Version != version {
		fatalf("downloaded module = %s %s, want %s %s", downloaded.Path, downloaded.Version, modulePath, version)
	}
	for name, value := range map[string]string{
		"Info": downloaded.Info, "GoMod": downloaded.GoMod, "Zip": downloaded.Zip,
		"Dir": downloaded.Dir, "Sum": downloaded.Sum, "GoModSum": downloaded.GoModSum,
	} {
		if strings.TrimSpace(value) == "" {
			fatalf("module download omitted %s", name)
		}
	}
	var goMod editedGoMod
	decodeFile(os.Args[5], &goMod)
	if goMod.Module.Path != "example.com/floret-published-adoption-smoke" {
		fatalf("consumer module path = %q", goMod.Module.Path)
	}
	if len(goMod.Replace) != 0 {
		fatalf("consumer go.mod contains replace directives")
	}
	found := false
	for _, requirement := range goMod.Require {
		if requirement.Path == modulePath {
			found = requirement.Version == version
		}
	}
	if !found {
		fatalf("consumer go.mod does not require exact %s %s", modulePath, version)
	}
	checkConsumerImports(os.Args[6], modulePath)
	checkExampleImports(downloaded.Dir, modulePath, []string{
		"minimal-durable-host", "custom-model-gateway", "tool-effect-approval",
		"startup-recovery", "store-maintenance-host",
	})
	if err := os.WriteFile(os.Args[7], []byte(downloaded.Dir), 0o600); err != nil {
		fatalf("write verified module directory: %v", err)
	}
	fmt.Printf("published release module: path=%s version=%s sum=%s gomod_sum=%s\n",
		downloaded.Path, downloaded.Version, downloaded.Sum, downloaded.GoModSum)
}

func decodeFile(path string, target any) {
	data, err := os.ReadFile(path)
	if err != nil {
		fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		fatalf("decode %s: %v", path, err)
	}
}

func checkConsumerImports(root, modulePath string) {
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && path != root {
			return filepath.SkipDir
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		return checkGoImports(path, modulePath)
	})
	if err != nil {
		fatalf("check consumer imports: %v", err)
	}
}

func checkExampleImports(moduleDir, modulePath string, examples []string) {
	for _, example := range examples {
		root := filepath.Join(moduleDir, "cmd", "examples", example)
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			fatalf("published module omitted example %s", example)
		}
		err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			return checkGoImports(path, modulePath)
		})
		if err != nil {
			fatalf("check example %s imports: %v", example, err)
		}
	}
}

func checkGoImports(path, modulePath string) error {
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		return err
	}
	for _, imported := range parsed.Imports {
		value, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			return err
		}
		if value == modulePath+"/internal" || strings.HasPrefix(value, modulePath+"/internal/") {
			return fmt.Errorf("%s: forbidden downstream import %q", path, value)
		}
	}
	return nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "published release verifier: "+format+"\n", args...)
	os.Exit(1)
}
EOF
}

root=$(mktemp -d "${TMPDIR:-/tmp}/floret-published-adoption.XXXXXX")
cleanup_root() {
  chmod -R u+w "${root}" 2>/dev/null || true
  rm -rf -- "${root}"
}
trap cleanup_root EXIT
export GOWORK=off
assert_workspace_disabled
mkdir -p "${root}/consumer" "${root}/verifier"
write_consumer_test "${root}/consumer/adoption_test.go"
write_verifier "${root}/verifier/main.go"
gofmt -w "${root}/consumer/adoption_test.go" "${root}/verifier/main.go"
go build -o "${root}/verifier/check" "${root}/verifier/main.go"

if [[ ${1:-} == "--check" ]]; then
  [[ $# -eq 1 ]] || { usage; exit 2; }
  "${root}/verifier/check" --check-consumer "${root}/consumer" "${module_path}"
  printf 'published release adoption templates: ok\n'
  exit 0
fi
[[ $# -eq 1 ]] || { usage; exit 2; }
readonly tag=$1
[[ ${tag} =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]] || fail "tag must be an exact semantic version, got ${tag}"

export GO111MODULE=on
export GOFLAGS=
export GOPATH="${root}/gopath"
export GOMODCACHE="${root}/modcache"
export GOCACHE="${root}/buildcache"
export GOPRIVATE=
export GONOPROXY=
export GONOSUMDB=
mkdir -p "${GOPATH}" "${GOMODCACHE}" "${GOCACHE}"

assert_workspace_disabled
readonly go_sum_db=$(go env GOSUMDB)
[[ -n ${go_sum_db} && ${go_sum_db} != "off" ]] || fail "GOSUMDB must be enabled"
readonly configured_proxy=$(go env GOPROXY)
proxy_only=${configured_proxy%%,direct*}
proxy_only=${proxy_only%%|direct*}
proxy_only=${proxy_only%%,off*}
proxy_only=${proxy_only%%|off*}
[[ -n ${proxy_only} && ${proxy_only} != "off" && ${proxy_only} != "direct" ]] || fail "GOPROXY must start with a module proxy"
[[ ${proxy_only} != *direct* && ${proxy_only} != *off* ]] || fail "GOPROXY contains an unsupported direct or off entry"
export GOPROXY="${proxy_only}"

pushd "${root}/consumer" >/dev/null
go mod init example.com/floret-published-adoption-smoke
go get "${module_path}@${tag}"
for profile in "${scaffold_profiles[@]}"; do
  go run "${module_path}/cmd/floret-host-init@${tag}" \
    --profile "${profile}" \
    --package adoption \
    --dir "${root}/consumer/generated/${profile}" \
    --write
done
go mod tidy
export GOFLAGS="-mod=readonly"
go list -m -json "${module_path}" >"${root}/module-list.json"
go mod download -json "${module_path}@${tag}" >"${root}/module-download.json"
go mod edit -json >"${root}/consumer-gomod.json"
"${root}/verifier/check" \
  "${module_path}" "${tag}" \
  "${root}/module-list.json" "${root}/module-download.json" \
  "${root}/consumer-gomod.json" "${root}/consumer" "${root}/published-dir.txt"
go test ./...
popd >/dev/null

readonly published_dir=$(<"${root}/published-dir.txt")
for example in "${smoke_examples[@]}"; do
  (
    cd "${published_dir}"
    go run "./cmd/examples/${example}"
  )
done

printf 'published release adoption: %s %s verified with Go %s\n' \
  "${module_path}" "${tag}" "$(go env GOVERSION)"
