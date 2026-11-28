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

mkdir "$scratch/member" "$scratch/control"
(cd "$repo" && go run "$repo/scripts/testdata/thread-catalog-fence/create.go" "$scratch/member" member)
member_manifest() (
  cd "$scratch/member"
  find . -type f -print | LC_ALL=C sort | while IFS= read -r file; do shasum -a 256 "$file"; done
)
member_manifest > "$scratch/before"
cp "$repo/scripts/testdata/thread-catalog-fence/member-writer.go" main.go
go get github.com/floegence/floret/v7@v7.134.1
go mod tidy
go mod verify
test "$(go list -m -f '{{.Version}}' github.com/floegence/floret/v7)" = v7.134.1
test -z "$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)"
go run -mod=readonly . "$scratch/control/floret_threads.sqlite" "$scratch/member/floret_threads.sqlite"
member_manifest > "$scratch/after"
diff -u "$scratch/before" "$scratch/after"
printf 'verified released v7.134.1 thread writer against control and unchanged second-generation catalog and member\n'
