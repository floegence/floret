#!/usr/bin/env bash
set -euo pipefail

readonly module_path="github.com/floegence/floret/v4"
readonly baseline_tag="v3.0.0"
readonly apidiff_version="v0.0.0-20260727155853-b88d891fe743"
readonly apidiff_package="golang.org/x/exp/cmd/apidiff@${apidiff_version}"

export GOWORK=off
go test ./internal/architecture -run TestV3PublicAPIMatchesDesignedBaseline -count=1

if ! git rev-parse --quiet --verify "refs/tags/${baseline_tag}" >/dev/null; then
  printf 'v3 API compatibility: %s is not tagged; the designed go/types baseline matches\n' "${baseline_tag}"
  exit 0
fi

go list -m "${module_path}@${baseline_tag}" >/dev/null
readonly compatibility_root=$(mktemp -d "${TMPDIR:-/tmp}/floret-v3-apidiff.XXXXXX")
cleanup() {
  chmod -R u+w "${compatibility_root}" 2>/dev/null || true
  rm -rf -- "${compatibility_root}"
}
trap cleanup EXIT

mkdir -p "${compatibility_root}/baseline"
cd "${compatibility_root}/baseline"
go mod init example.com/floret-v3-api-baseline >/dev/null
go get "${module_path}/...@${baseline_tag}" >/dev/null
go run "${apidiff_package}" -m -w "${compatibility_root}/v3.api" "${module_path}"
cd - >/dev/null

go run "${apidiff_package}" -m -w "${compatibility_root}/head.api" "${module_path}"
readonly incompatible=$(go run "${apidiff_package}" -m -incompatible "${compatibility_root}/v3.api" "${compatibility_root}/head.api")
if [[ -n ${incompatible} ]]; then
  printf 'v3 API compatibility: incompatible changes from %s to HEAD\n%s\n' "${baseline_tag}" "${incompatible}" >&2
  exit 1
fi
printf 'v3 API compatibility: %s to HEAD has no incompatible changes\n' "${baseline_tag}"
