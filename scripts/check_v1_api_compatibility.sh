#!/usr/bin/env bash
set -euo pipefail

readonly module_path="github.com/floegence/floret"
readonly baseline_tag="v1.0.0"
readonly apidiff_version="v0.0.0-20260727155853-b88d891fe743"
readonly apidiff_package="golang.org/x/exp/cmd/apidiff@${apidiff_version}"

export GOWORK=off

go test ./internal/architecture -run TestV1PublicAPISurfaceMatchesBaseline -count=1

if ! git rev-parse --quiet --verify "refs/tags/${baseline_tag}" >/dev/null; then
	printf 'v1 API compatibility: %s is not tagged; local go/types baseline matches\n' "${baseline_tag}"
	exit 0
fi

go list -m "${module_path}@${baseline_tag}" >/dev/null

readonly root=$(mktemp -d "${TMPDIR:-/tmp}/floret-v1-apidiff.XXXXXX")
cleanup_root() {
	chmod -R u+w "${root}" 2>/dev/null || true
	rm -rf -- "${root}"
}
trap cleanup_root EXIT

mkdir -p "${root}/baseline"
pushd "${root}/baseline" >/dev/null
go mod init example.com/floret-v1-api-baseline >/dev/null
go get "${module_path}@${baseline_tag}" >/dev/null
go run "${apidiff_package}" -m -w "${root}/v1.api" "${module_path}"
popd >/dev/null

go run "${apidiff_package}" -m -w "${root}/head.api" "${module_path}"
incompatible=$(go run "${apidiff_package}" -m -incompatible "${root}/v1.api" "${root}/head.api")
if [[ -n ${incompatible} ]]; then
	printf 'v1 API compatibility: incompatible changes from %s to HEAD\n%s\n' "${baseline_tag}" "${incompatible}" >&2
	exit 1
fi

printf 'v1 API compatibility: %s to HEAD has no incompatible changes\n' "${baseline_tag}"
