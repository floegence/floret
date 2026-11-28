#!/usr/bin/env bash
set -euo pipefail

source_path=${1:?expected the stopped format-six fence}
thread=${2:?expected a thread to read after installation}
run=${FLOEGENCE_TEST_RUN_DIR:?}
reports=${FLOEGENCE_TEST_REPORTS:?}
area=$(mktemp -d "$run/member-cutover.XXXXXX")
cleanup() {
  rm -rf "$area"
  test ! -e "$area" && printf 'removed private snapshot and candidate\n' > "$reports/cleanup.txt"
}
trap cleanup EXIT
python3 scripts/testdata/thread-member-cutover/snapshot.py "$source_path" "$area/source"
GOWORK=off go run -tags floret_layout_cutover ./scripts/testdata/thread-member-cutover prepare "$area/source/$(basename "$source_path")" "$area/candidate" 2>&1 | tee "$reports/full-stage.log"
cp "$area/candidate/prepared.json" "$reports/prepared.json"
GOWORK=off go run -tags floret_layout_cutover ./scripts/testdata/thread-member-cutover install "$area/source/$(basename "$source_path")" "$area/candidate"
GOWORK=off go run -tags floret_layout_cutover ./scripts/testdata/thread-member-cutover verify-window "$area/source/$(basename "$source_path")" "$thread" | tee "$reports/installed-window.log"
