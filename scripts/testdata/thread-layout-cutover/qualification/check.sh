#!/usr/bin/env bash
set -euo pipefail
umask 077
export GOWORK=off GOTELEMETRY=off
GOTOOLCHAIN=$(go env GOVERSION)
export GOTOOLCHAIN
sources=$(cd -- "$(dirname -- "$0")" && pwd -P)
scratch=$(mktemp -d "${TMPDIR:?}/qualified-copy.XXXXXX")
report=${FLOEGENCE_TEST_REPORTS:?}/published-upgrade.txt
cleanup() {
  status=$?
  rm -rf -- "$scratch" || exit 1
  test ! -e "$scratch" || exit 1
  printf 'qualification_exit=%s scratch_removed=true\n' "$status"
  cat "$report" >&2
}
trap cleanup EXIT
exec > "$report"
git rev-parse HEAD
go version
for version in v7.132.1 v7.133.2; do
  directory=$scratch/$version
  mkdir "$directory"
  (
    cd "$directory"
    cp "$sources/probe.go" main.go
    go mod init example.com/qualified-copy
    go get "github.com/floegence/floret/v7@$version"
    go mod tidy
    go mod verify
    test "$(go list -m -f '{{.Version}}' github.com/floegence/floret/v7)" = "$version"
    test -z "$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)"
    go build -mod=readonly -o "$directory/probe" .
    if [[ $version == v7.132.1 ]]; then
      cp "$sources/../../logical-storage-fence/writer.go" main.go
      go run -mod=readonly . "$scratch/fixture.sqlite"
    fi
  )
done
verify() {
  local label=$1 old=$2 current=$3 thread=$4 before after restart
  local statement="SELECT json_extract(CAST(value AS TEXT), '\$.version') FROM floret_backend_records WHERE namespace='floret.system' AND key=CAST('logical-schema' AS BLOB)"
  test -s "$current"
  test "$(sqlite3 -init /dev/null "$current" "$statement")" = 19
  before=$("$scratch/v7.132.1/probe" "$old" "$thread")
  after=$("$scratch/v7.133.2/probe" "$current" "$thread")
  restart=$("$scratch/v7.133.2/probe" "$current" "$thread")
  test "$(sqlite3 -init /dev/null "$current" "$statement")" = 20
  printf '%s old: %s\n%s upgraded: %s\n%s reopened: %s\n' "$label" "$before" "$label" "$after" "$label" "$restart"
  test "$before" = "$after" && test "$after" = "$restart" || return 1
  reference_digest=${before##*sha256=}
}
thread=$(sqlite3 -init /dev/null "$scratch/fixture.sqlite" "SELECT json_extract(CAST(value AS TEXT),'$.payload.id') FROM floret_backend_records WHERE namespace='floret.domain.sessiontree.v17' AND json_extract(CAST(value AS TEXT),'$.payload.kind')='thread'")
(cd "$scratch" && sqlite3 -init /dev/null fixture.sqlite '.backup fixture-new.sqlite')
verify fixture "$scratch/fixture.sqlite" "$scratch/fixture-new.sqlite" "$thread"
if [[ $# == 2 ]]; then
  python3 -IB "$sources/clone.py" "$1" "$scratch/old.sqlite" "$scratch/new.sqlite"
  verify copy "$scratch/old.sqlite" "$scratch/new.sqlite" "$2"
  sqlite3 -init /dev/null -readonly "$scratch/new.sqlite" 'SELECT namespace,count(*) FROM floret_backend_records GROUP BY namespace ORDER BY namespace'
  OFFLINE_PAGE_DIGEST=$reference_digest go run -mod=readonly -tags floret_layout_cutover "$sources/stage" "$scratch/new.sqlite" "$2"
elif [[ $# != 0 ]]; then
  exit 2
fi
