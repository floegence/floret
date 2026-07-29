#!/usr/bin/env bash
set -euo pipefail

readonly module_path="github.com/floegence/floret/v2"
readonly version="v0.0.0-candidate"

if [[ -n $(git status --porcelain --untracked-files=all) ]]; then
  printf 'candidate release adoption: worktree must be clean; this gate packages committed HEAD only\n' >&2
  exit 1
fi

readonly root=$(mktemp -d "${TMPDIR:-/tmp}/floret-candidate-adoption.XXXXXX")

cleanup_root() {
  chmod -R u+w "${root}" 2>/dev/null || true
  rm -rf -- "${root}"
}
trap cleanup_root EXIT

readonly proxy_root="${root}/proxy"
readonly module_proxy_dir="${proxy_root}/${module_path}/@v"
readonly archive_prefix="${module_path}@${version}/"
mkdir -p "${module_proxy_dir}" "${root}/consumer"

git archive --format=zip --prefix="${archive_prefix}" HEAD -o "${module_proxy_dir}/${version}.zip"
cp go.mod "${module_proxy_dir}/${version}.mod"
printf '{"Version":"%s","Time":"1970-01-01T00:00:00Z"}\n' "${version}" >"${module_proxy_dir}/${version}.info"
printf '%s\n' "${version}" >"${module_proxy_dir}/list"

export GOWORK=off
export GO111MODULE=on
readonly upstream_proxy=$(go env GOPROXY)
if [[ -z ${upstream_proxy} || ${upstream_proxy} == "off" || ${upstream_proxy} == "direct" ]]; then
  printf 'candidate release adoption: GOPROXY must include an upstream module proxy for transitive dependencies\n' >&2
  exit 1
fi
export GOPROXY="file://${proxy_root},${upstream_proxy}"
export GOPRIVATE=
export GONOPROXY=
export GONOSUMDB=
export GOSUMDB=off
export GOPATH="${root}/gopath"
export GOMODCACHE="${root}/modcache"
export GOCACHE="${root}/buildcache"
mkdir -p "${GOPATH}" "${GOMODCACHE}" "${GOCACHE}"

mkdir -p "${root}/legacy-fixture"
pushd "${root}/legacy-fixture" >/dev/null
go mod init example.com/floret-v0312-fixture
go get "${module_path}@v0.31.2"
cat >main.go <<'EOF'
package main

import (
	"context"
	"os"

	"github.com/floegence/floret/v2/florettest"
	"github.com/floegence/floret/v2/runtime"
)

func main() {
	if len(os.Args) != 2 {
		panic("fixture path is required")
	}
	ctx := context.Background()
	startup, err := runtime.StartSQLiteStore(ctx, os.Args[1], runtime.SQLiteStartupRequest{})
	if err != nil {
		panic(err)
	}
	_, err = florettest.PopulateStoreFixture(ctx, startup.Store, florettest.StoreFixtureInput{
		ThreadID: "v0312-thread", CreateIntentID: "v0312-create",
		Turns: []florettest.StoreFixtureTurn{{
			Request: runtime.RunTurnRequest{TurnID: "v0312-turn", RunID: "v0312-run", Input: runtime.TurnInput{Text: "persist schema v16"}},
			ModelSteps: []florettest.ModelStep{{Events: []runtime.ModelEvent{{Type: runtime.ModelEventDelta, Text: "v0312"}, {Type: runtime.ModelEventDone, Reason: "stop"}}}},
		}},
	})
	if err != nil {
		_ = startup.Store.Close()
		panic(err)
	}
	if err := startup.Store.Close(); err != nil {
		panic(err)
	}
}
EOF
go mod tidy
go run . "${root}/v0312-schema-v16.db"
popd >/dev/null

pushd "${root}/consumer" >/dev/null
go mod init example.com/floret-candidate-adoption
go get "${module_path}@${version}"
for profile in memory durable-basic approval production-recovery subagent; do
  go run "${module_path}/cmd/floret-host-init@${version}" \
    --profile "${profile}" \
    --package adoption \
    --dir "${root}/consumer/generated/${profile}" \
    --write
done
cat >exact_read_surface_test.go <<'EOF'
package adoption_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/florettest"
	"github.com/floegence/floret/v2/runtime"
)

func TestCandidateExactReadSurface(t *testing.T) {
	ctx := context.Background()
	store := runtime.NewMemoryStore()
	defer store.Close()
	var createBinder *runtime.ThreadCreateHostBinder
	var turnBinder *runtime.TurnExecutionHostBinder
	var readBinder *runtime.ThreadReadHostBinder
	var subAgentBinder *runtime.SubAgentHostBinder
	var subAgentReadBinder *runtime.SubAgentReadHostBinder
	if err := runtime.ConfigureHostCapabilities(store, func(bootstrap *runtime.HostBootstrap) error {
		var err error
		createBinder, err = runtime.NewThreadCreateHostBinder(bootstrap)
		if err != nil {
			return err
		}
		turnBinder, err = runtime.NewTurnExecutionHostBinder(bootstrap)
		if err != nil {
			return err
		}
		readBinder, err = runtime.NewThreadReadHostBinder(bootstrap)
		if err != nil {
			return err
		}
		subAgentBinder, err = runtime.NewSubAgentHostBinder(bootstrap)
		if err != nil {
			return err
		}
		subAgentReadBinder, err = runtime.NewSubAgentReadHostBinder(bootstrap)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	creator, err := createBinder.Bind("candidate-thread", "candidate-create")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.CreateThread(ctx, runtime.CreateThreadRequest{ThreadID: "candidate-thread", CreateIntentID: "candidate-create"}); err != nil {
		t.Fatal(err)
	}
	turnFactory, err := turnBinder.Bind("candidate-thread")
	if err != nil {
		t.Fatal(err)
	}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	turnOptions, err := runtime.NewTurnExecutionHostOptions(
		config.Config{SystemPrompt: "candidate", ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}},
		runtime.WithTurnModelGateway(
			florettest.NewScriptedModelGateway(florettest.ModelStep{Events: []runtime.ModelEvent{{Type: runtime.ModelEventDelta, Text: "ok"}, {Type: runtime.ModelEventDone, Reason: "stop"}}}),
			runtime.ModelGatewayIdentity{Provider: "candidate", Model: "candidate", StateCompatibilityKey: "candidate:v1"},
			runtime.ModelGatewayCapabilities{Reasoning: &reasoning},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	execHost, err := turnFactory.NewHost(ctx, turnOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execHost.RunTurn(ctx, runtime.RunTurnRequest{ThreadID: "candidate-thread", TurnID: "candidate-turn", RunID: "candidate-run", Input: runtime.TurnInput{Text: "candidate"}}); err != nil {
		t.Fatal(err)
	}
	reader, err := readBinder.NewHost(ctx, "candidate-thread")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := reader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: "candidate-thread", TurnID: "candidate-turn"})
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: "candidate-thread", TurnID: "missing"}); !errors.Is(err, runtime.ErrTurnNotFound) {
		t.Fatalf("unknown exact turn error = %v", err)
	}
	subAgentOptions, err := runtime.NewSubAgentHostOptions(
		config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done", SystemPrompt: "candidate child"},
		runtime.WithSubAgentRunTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootFactory, err := subAgentBinder.Bind("candidate-thread")
	if err != nil {
		t.Fatal(err)
	}
	rootSubAgents, err := rootFactory.NewHost(ctx, subAgentOptions)
	if err != nil {
		t.Fatal(err)
	}
	child, err := rootSubAgents.SpawnSubAgent(ctx, runtime.SpawnSubAgentRequest{PublicationID: "publish-child", ParentThreadID: "candidate-thread", ParentTurnID: "candidate-turn", ThreadID: "candidate-child", TaskName: "child", Message: "child", ForkMode: runtime.SubAgentForkNone})
	if err != nil {
		t.Fatal(err)
	}
	waited, err := rootSubAgents.WaitSubAgents(ctx, runtime.WaitSubAgentsRequest{ParentThreadID: "candidate-thread", ChildThreadIDs: []runtime.ThreadID{child.ThreadID}, Timeout: 2 * time.Second})
	if err != nil || waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].LatestTurnID == "" {
		t.Fatalf("child wait = %#v, err = %v", waited, err)
	}
	child = waited.Snapshots[0]
	sibling, err := rootSubAgents.SpawnSubAgent(ctx, runtime.SpawnSubAgentRequest{PublicationID: "publish-sibling", ParentThreadID: "candidate-thread", ParentTurnID: "candidate-turn", ThreadID: "candidate-sibling", TaskName: "sibling", Message: "sibling", ForkMode: runtime.SubAgentForkNone})
	if err != nil {
		t.Fatal(err)
	}
	if waited, err = rootSubAgents.WaitSubAgents(ctx, runtime.WaitSubAgentsRequest{ParentThreadID: "candidate-thread", ChildThreadIDs: []runtime.ThreadID{sibling.ThreadID}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("sibling wait = %#v, err = %v", waited, err)
	}
	childFactory, err := subAgentBinder.Bind(child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	childSubAgents, err := childFactory.NewHost(ctx, subAgentOptions)
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := childSubAgents.SpawnSubAgent(ctx, runtime.SpawnSubAgentRequest{PublicationID: "publish-grandchild", ParentThreadID: child.ThreadID, ParentTurnID: child.LatestTurnID, ThreadID: "candidate-grandchild", TaskName: "grandchild", Message: "grandchild", ForkMode: runtime.SubAgentForkNone})
	if err != nil {
		t.Fatal(err)
	}
	waited, err = childSubAgents.WaitSubAgents(ctx, runtime.WaitSubAgentsRequest{ParentThreadID: child.ThreadID, ChildThreadIDs: []runtime.ThreadID{grandchild.ThreadID}, Timeout: 2 * time.Second})
	if err != nil || waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].LatestTurnID == "" {
		t.Fatalf("grandchild wait = %#v, err = %v", waited, err)
	}
	grandchild = waited.Snapshots[0]
	rootChildReader, err := subAgentReadBinder.NewHost(ctx, "candidate-thread")
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct{ threadID runtime.ThreadID; turnID runtime.TurnID }{{child.ThreadID, child.LatestTurnID}, {grandchild.ThreadID, grandchild.LatestTurnID}} {
		exact, err := rootChildReader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: target.threadID, TurnID: target.turnID})
		if err != nil {
			t.Fatal(err)
		}
		if err := exact.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	childReader, err := subAgentReadBinder.NewHost(ctx, child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []runtime.ThreadID{"candidate-thread", child.ThreadID, sibling.ThreadID} {
		if _, err := childReader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: target, TurnID: child.LatestTurnID}); !errors.Is(err, runtime.ErrSubAgentNotFound) {
			t.Fatalf("unauthorized target %q error = %v", target, err)
		}
	}
}

func TestCandidateReopensV0312SchemaV16(t *testing.T) {
	ctx := context.Background()
	startup, err := runtime.StartSQLiteStore(ctx, os.Getenv("FLORET_V0312_FIXTURE"), runtime.SQLiteStartupRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if startup.Store == nil || startup.Migration != nil || startup.Verification == nil || startup.Verification.Inspection.State != runtime.SQLiteStoreStateCurrent {
		t.Fatalf("v0.31.2 reopen = %#v", startup)
	}
	defer startup.Store.Close()
	var readBinder *runtime.ThreadReadHostBinder
	if err := runtime.ConfigureHostCapabilities(startup.Store, func(bootstrap *runtime.HostBootstrap) error {
		var configureErr error
		readBinder, configureErr = runtime.NewThreadReadHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := readBinder.NewHost(ctx, "v0312-thread")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := reader.ReadThreadTurn(ctx, runtime.ReadThreadTurnRequest{ThreadID: "v0312-thread", TurnID: "v0312-turn"})
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.Validate(); err != nil || turn.RunID != "v0312-run" {
		t.Fatalf("v0.31.2 exact turn = %#v, err = %v", turn, err)
	}
}
EOF
go mod tidy
FLORET_V0312_FIXTURE="${root}/v0312-schema-v16.db" GOFLAGS=-mod=readonly go test ./...
if grep -Eq '(^|[[:space:]])replace([[:space:]]|$)' go.mod; then
  printf 'candidate release adoption: generated consumer contains replace\n' >&2
  exit 1
fi
candidate_dir=$(go list -m -f '{{.Dir}}' "${module_path}")
popd >/dev/null

for example in custom-model-gateway message-references minimal-durable-host startup-recovery store-maintenance-host subagent-host tool-effect-approval; do
	(
		cd "${candidate_dir}"
		go run "./cmd/examples/${example}"
	)
done

printf 'candidate release adoption: clean committed HEAD packaged as %s, reopened v0.31.2 schema-v16, and ran generated hosts and examples without workspace or replace wiring\n' "${version}"
