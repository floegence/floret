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

write_consumer_test() {
  local destination=$1
  cat >"${destination}" <<'EOF'
package adoption_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/florettest"
	"github.com/floegence/floret/runtime"
)

func TestPublishedModelGatewayContract(t *testing.T) {
	florettest.RunModelGatewayContract(t, florettest.ScriptedModelGatewayFactory)
}

func TestPublishedDurableHostRestartAndStoreMaintenance(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	inspection, err := runtime.InspectSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.State != runtime.SQLiteStoreStateMissing {
		t.Fatalf("initial inspection state = %q", inspection.State)
	}
	store, err := runtime.OpenSQLiteStore(ctx, path, runtime.SQLiteStoreOpenRequest{
		ExpectedState: inspection.State,
	})
	if err != nil {
		t.Fatal(err)
	}
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

	verification, err := runtime.VerifySQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if verification.Inspection.State != runtime.SQLiteStoreStateCurrent ||
		verification.Inspection.LeasePolicyState != runtime.SQLiteStoreLeasePolicyMatches {
		t.Fatalf("verification inspection = %#v", verification.Inspection)
	}
	for _, check := range verification.Checks {
		if !check.Passed {
			t.Fatalf("verification check = %#v", check)
		}
	}
	reopened, err := runtime.OpenSQLiteStore(ctx, path, runtime.SQLiteStoreOpenRequest{
		ExpectedState:  verification.Inspection.State,
		ExpectedSchema: verification.Inspection.Observed,
	})
	if err != nil {
		t.Fatal(err)
	}
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
	if len(page.Turns) != 1 || page.Turns[0].TurnID != "published-turn" ||
		page.Turns[0].Projection.Output != "published response" {
		t.Fatalf("restarted turn page = %#v", page)
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
trap 'rm -rf -- "${root}"' EXIT
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
export GOWORK=off
export GOFLAGS=
export GOPATH="${root}/gopath"
export GOMODCACHE="${root}/modcache"
export GOCACHE="${root}/buildcache"
export GOPRIVATE=
export GONOPROXY=
export GONOSUMDB=
mkdir -p "${GOPATH}" "${GOMODCACHE}" "${GOCACHE}"

[[ $(go env GOWORK) == "/dev/null" ]] || fail "GOWORK must resolve to off"
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
