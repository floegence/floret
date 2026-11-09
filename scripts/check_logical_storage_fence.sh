#!/usr/bin/env bash
set -euo pipefail

# IMPORTANT: An incompatible domain must be rejected by a released old Runtime
# before domain admission. A mocked error classifier cannot establish this fence.
readonly repository_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ $# == 1 && $1 == v7.* ]] || { printf 'usage: scripts/check_logical_storage_fence.sh <exact-v7-version>\n' >&2; exit 1; }
readonly version=$1
readonly root=$(mktemp -d "${TMPDIR:-/tmp}/floret-logical-fence.XXXXXX")
trap 'rm -rf -- "${root}"' EXIT
export GOWORK=off
mkdir -p "${root}/writer" "${root}/reader"
cd "${root}/writer"
go mod init example.com/logical-fence-writer
cp "${repository_root}/scripts/testdata/logical-storage-fence/writer.go" main.go
go get "github.com/floegence/floret/v7@${version}"
go mod tidy
go mod verify
[[ $(go list -m -f '{{.Version}}' github.com/floegence/floret/v7) == "${version}" ]]
[[ -z $(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all) ]]
go run . "${root}/history.sqlite"
cd "${root}/reader"
go mod init example.com/logical-fence-reader
cp "${repository_root}/scripts/testdata/logical-storage-fence/reader.go" main.go
go get github.com/floegence/floret/v7@v7.107.9
go mod tidy
go mod verify
[[ $(go list -m -f '{{.Version}}' github.com/floegence/floret/v7) == v7.107.9 ]]
[[ -z $(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all) ]]
go run . "${root}/history.sqlite"
cd "${root}/writer"
go run . "${root}/history.sqlite"
