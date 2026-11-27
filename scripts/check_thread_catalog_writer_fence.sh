#!/usr/bin/env bash
set -euo pipefail

readonly repo=$(git rev-parse --show-toplevel)
readonly scratch=$(mktemp -d "${TMPDIR:?}/thread-catalog-fence.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
export GOWORK=off
mkdir "$scratch/fixture" "$scratch/consumer"
go run "$repo/scripts/testdata/thread-catalog-fence/create.go" "$scratch/fixture"
manifest() (
  cd "$scratch/fixture"
  find . -type f -print | LC_ALL=C sort | while IFS= read -r file; do shasum -a 256 "$file"; done
)
manifest > "$scratch/before"
cd "$scratch/consumer"
go mod init example.com/thread-catalog-fence
cp "$repo/scripts/testdata/thread-catalog-fence/old.go" main.go
for version in v7.132.1 v7.133.2; do
  go get "github.com/floegence/floret/v7@$version"
  go mod tidy
  go mod verify
  test "$(go list -m -f '{{.Version}}' github.com/floegence/floret/v7)" = "$version"
  test -z "$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)"
  go run -mod=readonly . "$scratch/fixture/floret_threads.sqlite"
  manifest > "$scratch/after"
  diff -u "$scratch/before" "$scratch/after"
  printf 'verified released writer %s, with unchanged fixture files\n' "$version"
done
