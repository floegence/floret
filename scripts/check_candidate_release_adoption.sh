#!/usr/bin/env bash
set -euo pipefail

readonly module_path="github.com/floegence/floret/v3"
readonly version="v3.0.0-candidate"
readonly repository_root=$(git rev-parse --show-toplevel)

if [[ -n $(git status --porcelain --untracked-files=all) ]]; then
  printf 'candidate release adoption: worktree must be clean; the gate packages committed HEAD only\n' >&2
  exit 1
fi

readonly adoption_root=$(mktemp -d "${TMPDIR:-/tmp}/floret-v3-candidate.XXXXXX")
cleanup() {
  chmod -R u+w "${adoption_root}" 2>/dev/null || true
  rm -rf -- "${adoption_root}"
}
trap cleanup EXIT

readonly proxy_root="${adoption_root}/proxy"
readonly version_dir="${proxy_root}/${module_path}/@v"
mkdir -p "${version_dir}" "${adoption_root}/consumer"
git archive --format=zip --prefix="${module_path}@${version}/" HEAD -o "${version_dir}/${version}.zip"
cp "${repository_root}/go.mod" "${version_dir}/${version}.mod"
printf '{"Version":"%s","Time":"1970-01-01T00:00:00Z"}\n' "${version}" >"${version_dir}/${version}.info"
printf '%s\n' "${version}" >"${version_dir}/list"

export GOWORK=off
export GO111MODULE=on
readonly upstream_proxy=$(go env GOPROXY)
if [[ -z ${upstream_proxy} || ${upstream_proxy} == "off" || ${upstream_proxy} == "direct" ]]; then
  printf 'candidate release adoption: GOPROXY must contain an upstream proxy for transitive dependencies\n' >&2
  exit 1
fi
export GOPROXY="file://${proxy_root},${upstream_proxy}"
export GOSUMDB=off
export GOPATH="${adoption_root}/gopath"
export GOMODCACHE="${adoption_root}/modcache"
export GOCACHE="${adoption_root}/buildcache"
mkdir -p "${GOPATH}" "${GOMODCACHE}" "${GOCACHE}"

cd "${adoption_root}/consumer"
go mod init example.com/floret-v3-candidate-adoption
cp "${repository_root}/scripts/testdata/v3_adoption_test.go" adoption_test.go
go get "${module_path}@${version}"
go mod tidy
GOFLAGS=-mod=readonly go test ./...

if [[ -n $(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all) ]]; then
  printf 'candidate release adoption: consumer resolved a replacement module\n' >&2
  exit 1
fi
readonly resolved_version=$(go list -m -f '{{.Version}}' "${module_path}")
if [[ ${resolved_version} != "${version}" ]]; then
  printf 'candidate release adoption: resolved %s, want %s\n' "${resolved_version}" "${version}" >&2
  exit 1
fi

printf 'candidate release adoption: blank module passed against committed %s without workspace, replace, or sibling wiring\n' "${version}"
