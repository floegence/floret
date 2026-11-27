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
# Continue the same database across released writers and the proposed upgrade.
use_floret() {
  go get "github.com/floegence/floret/v7@$1"
  go mod tidy
  go mod verify
  [[ $(go list -m -f '{{.Version}}' github.com/floegence/floret/v7) == "$1" ]]
  [[ -z $(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all) ]]
}
for writer_version in v7.110.0 v7.129.3 v7.131.0 v7.132.2 v7.133.0 "${version}"; do
  use_floret "${writer_version}"
  go run . "${root}/history.sqlite"
done
cd "${root}/reader"
go mod init example.com/logical-fence-reader
cp "${repository_root}/scripts/testdata/logical-storage-fence/reader.go" main.go
for reader_version in v7.107.9 v7.109.0 v7.110.0 v7.111.0 v7.119.1 v7.120.7 v7.129.3 v7.131.0 v7.132.2 v7.133.0; do
  use_floret "${reader_version}"
  go run . "${root}/history.sqlite"
done
cd "${root}/writer"
go run . "${root}/history.sqlite"
