#!/usr/bin/env bash
set -euo pipefail

readonly module_path="github.com/floegence/floret/v6"
readonly repository_root=$(git rev-parse --show-toplevel)

if [[ $# -ne 1 || $1 != v6.* ]]; then
  printf 'usage: scripts/check_published_release_adoption.sh <exact-v6-tag>\n' >&2
  exit 1
fi
readonly tag=$1
readonly adoption_root=$(mktemp -d "${TMPDIR:-/tmp}/floret-v6-published.XXXXXX")
cleanup() {
  chmod -R u+w "${adoption_root}" 2>/dev/null || true
  rm -rf -- "${adoption_root}"
}
trap cleanup EXIT

export GOWORK=off
export GO111MODULE=on
export GOPATH="${adoption_root}/gopath"
export GOMODCACHE="${adoption_root}/modcache"
export GOCACHE="${adoption_root}/buildcache"
mkdir -p "${GOPATH}" "${GOMODCACHE}" "${GOCACHE}" "${adoption_root}/consumer"

cd "${adoption_root}/consumer"
go mod init example.com/floret-v6-published-adoption
cp "${repository_root}/scripts/testdata/v6_adoption_test.go" adoption_test.go
go get "${module_path}@${tag}"
go mod tidy
GOFLAGS=-mod=readonly go test ./...
go mod verify

if [[ -n $(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all) ]]; then
  printf 'published release adoption: consumer resolved a replacement module\n' >&2
  exit 1
fi
readonly resolved_version=$(go list -m -f '{{.Version}}' "${module_path}")
readonly resolved_dir=$(go list -m -f '{{.Dir}}' "${module_path}")
if [[ ${resolved_version} != "${tag}" ]]; then
  printf 'published release adoption: resolved %s, want %s\n' "${resolved_version}" "${tag}" >&2
  exit 1
fi
case ${resolved_dir} in
  "${repository_root}"|"${repository_root}"/*)
    printf 'published release adoption: module resolved from the local repository\n' >&2
    exit 1
    ;;
esac
if ! grep -Fq "${module_path} ${tag} " go.sum || ! grep -Fq "${module_path} ${tag}/go.mod " go.sum; then
  printf 'published release adoption: exact module and go.mod checksums are missing\n' >&2
  exit 1
fi

printf 'published release adoption: %s %s passed from a blank checksummed module\n' "${module_path}" "${tag}"
