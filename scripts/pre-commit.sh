#!/bin/sh
set -eu

go test ./...
go test ./internal/testui -run 'TestServerStreamsAgentTurnEventsBeforeCompletion|TestServerAgentSessionTurnIgnoresServerTimeout|TestRunnerRunningSnapshotUsesRealTurnID' -count=1
go test ./internal/testui -run 'TestRunnerToolScenarioSuitePassesAndPersistsCoverage|TestServerRunAPIExposesSavedToolScenarioSuite' -count=1
go test ./eval -run TestCleanCommandEnvRemovesHookRepositoryVariables -count=1
node --check internal/testui/static/*.js internal/testui/static/views/*.js internal/testui/static/components/*.js
git diff --check
