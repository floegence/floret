#!/usr/bin/env bash
set -euo pipefail

readonly module_path="github.com/floegence/floret"
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
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/florettest"
	"github.com/floegence/floret/runtime"
)

func TestCandidateExactReadSurface(t *testing.T) {
	ctx := context.Background()
	store := runtime.NewMemoryStore()
	defer store.Close()
	var createBinder *runtime.ThreadCreateHostBinder
	var turnBinder *runtime.TurnExecutionHostBinder
	var readBinder *runtime.ThreadReadHostBinder
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
	execHost, err := turnFactory.NewHost(ctx, runtime.TurnExecutionHostOptions{
		Config: config.Config{SystemPrompt: "candidate", ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}},
		ModelGateway: florettest.NewScriptedModelGateway(florettest.ModelStep{Events: []runtime.ModelEvent{{Type: runtime.ModelEventDelta, Text: "ok"}, {Type: runtime.ModelEventDone, Reason: "stop"}}}),
		ModelGatewayIdentity: runtime.ModelGatewayIdentity{Provider: "candidate", Model: "candidate", StateCompatibilityKey: "candidate:v1"},
		ModelGatewayCapabilities: runtime.ModelGatewayCapabilities{Reasoning: &reasoning},
	})
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
}
EOF
go mod tidy
GOFLAGS=-mod=readonly go test ./...
if grep -Eq '(^|[[:space:]])replace([[:space:]]|$)' go.mod; then
  printf 'candidate release adoption: generated consumer contains replace\n' >&2
  exit 1
fi
popd >/dev/null

printf 'candidate release adoption: clean committed HEAD packaged as %s and compiled without workspace or replace wiring\n' "${version}"
