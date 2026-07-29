#!/bin/sh
set -eu

go test ./...
go test ./internal/testing/eval -run TestCleanCommandEnvRemovesHookRepositoryVariables -count=1
git diff --check
