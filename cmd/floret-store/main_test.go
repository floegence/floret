package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	floretruntime "github.com/floegence/floret/runtime"
)

func TestInspectJSONWritesSingleVersionedEnvelope(t *testing.T) {
	api := rejectingAPI(t)
	api.inspect = func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
		return floretruntime.SQLiteStoreInspection{
			State:  floretruntime.SQLiteStoreStateUpgradeable,
			Reason: floretruntime.SQLiteStoreReason("migration_available"),
		}, nil
	}
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	code := execute(context.Background(), []string{"--json", "inspect", "store.db"}, stdout, stderr, api, fixedOperationID)
	if code != exitMigration {
		t.Fatalf("exit code = %d, want %d", code, exitMigration)
	}
	if stderr.Len() != 0 {
		t.Fatalf("JSON command wrote human output to stderr: %q", stderr.String())
	}
	envelope := decodeSingleEnvelope(t, stdout.Bytes())
	if envelope.SchemaVersion != envelopeSchemaVersion || envelope.Command != "inspect" || envelope.OperationID != "operation-1" {
		t.Fatalf("envelope identity = %#v", envelope)
	}
	if envelope.Status != "action_required" || envelope.Reason != "migration_available" || envelope.Result == nil {
		t.Fatalf("envelope outcome = %#v", envelope)
	}
}

func TestInspectExitCodesFollowPublicStoreState(t *testing.T) {
	tests := []struct {
		name   string
		state  floretruntime.SQLiteStoreState
		reason floretruntime.SQLiteStoreReason
		code   int
	}{
		{name: "current", state: floretruntime.SQLiteStoreStateCurrent, code: exitSuccess},
		{name: "missing inspection", state: floretruntime.SQLiteStoreStateMissing, code: exitSuccess},
		{name: "upgradeable", state: floretruntime.SQLiteStoreStateUpgradeable, code: exitMigration},
		{name: "busy", state: floretruntime.SQLiteStoreStateBusy, reason: floretruntime.SQLiteStoreReasonBusy, code: exitBusy},
		{name: "permission", state: floretruntime.SQLiteStoreStatePermissionDenied, reason: floretruntime.SQLiteStoreReasonPermission, code: exitIO},
		{name: "io", state: floretruntime.SQLiteStoreStateIOError, reason: floretruntime.SQLiteStoreReasonIO, code: exitIO},
		{name: "unsupported older", state: floretruntime.SQLiteStoreStateUnsupportedOlder, code: exitUnsupported},
		{name: "future", state: floretruntime.SQLiteStoreStateFuture, code: exitUnsupported},
		{name: "drifted", state: floretruntime.SQLiteStoreStateDrifted, code: exitVerification},
		{name: "corrupt", state: floretruntime.SQLiteStoreStateCorrupt, code: exitVerification},
		{name: "unknown", state: floretruntime.SQLiteStoreState("unexpected"), code: exitVerification},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := rejectingAPI(t)
			api.inspect = func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
				return floretruntime.SQLiteStoreInspection{State: tc.state, Reason: tc.reason}, nil
			}
			code := execute(context.Background(), []string{"inspect", "store.db"}, io.Discard, io.Discard, api, fixedOperationID)
			if code != tc.code {
				t.Fatalf("exit code = %d, want %d", code, tc.code)
			}
		})
	}
}

func TestUnknownMigrationStatusDoesNotMapToSuccess(t *testing.T) {
	result := floretruntime.SQLiteStoreMigrationResult{
		Status: floretruntime.SQLiteStoreMaintenanceStatus("unexpected"),
		After:  floretruntime.SQLiteStoreInspection{State: floretruntime.SQLiteStoreStateCurrent},
	}
	if code, status := classifyMigration(result); code != exitVerification || status != "failed" {
		t.Fatalf("classification = (%d, %q), want (%d, failed)", code, status, exitVerification)
	}
}

func TestVerifyFailureUsesDedicatedExitCode(t *testing.T) {
	api := rejectingAPI(t)
	api.verify = func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreVerification, error) {
		return floretruntime.SQLiteStoreVerification{
			Inspection: floretruntime.SQLiteStoreInspection{State: floretruntime.SQLiteStoreStateCurrent},
			Checks:     []floretruntime.SQLiteStoreVerificationCheck{{Code: "thread_authority", Passed: false}},
		}, nil
	}
	stdout := &bytes.Buffer{}
	code := execute(context.Background(), []string{"verify", "--json", "store.db"}, stdout, io.Discard, api, fixedOperationID)
	if code != exitVerification {
		t.Fatalf("exit code = %d, want %d", code, exitVerification)
	}
	envelope := decodeSingleEnvelope(t, stdout.Bytes())
	if envelope.Status != "failed" || envelope.Reason != "thread_authority" {
		t.Fatalf("verification envelope = %#v", envelope)
	}
}

func TestMigrateDefaultsToPlanAndApplyIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		mode floretruntime.SQLiteStoreMigrationMode
		code int
	}{
		{name: "plan", args: []string{"migrate", "store.db"}, mode: floretruntime.SQLiteStoreMigrationPlan, code: exitMigration},
		{name: "apply", args: []string{"migrate", "store.db", "--apply"}, mode: floretruntime.SQLiteStoreMigrationApply, code: exitSuccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := floretruntime.StoreSchemaIdentity{Version: "15", Fingerprint: "observed"}
			api := rejectingAPI(t)
			api.inspect = func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
				return floretruntime.SQLiteStoreInspection{State: floretruntime.SQLiteStoreStateUpgradeable, Observed: observed}, nil
			}
			api.migrate = func(_ context.Context, _ string, request floretruntime.SQLiteStoreMigrationRequest, _ ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreMigrationResult, error) {
				if request.OperationID != "operation-1" || request.Mode != tc.mode || request.ExpectedSchema != observed {
					t.Fatalf("migration request = %#v", request)
				}
				result := floretruntime.SQLiteStoreMigrationResult{
					OperationID: request.OperationID, Mode: request.Mode, Status: floretruntime.SQLiteStoreMaintenanceReady,
					Before: floretruntime.SQLiteStoreInspection{State: floretruntime.SQLiteStoreStateUpgradeable},
					After:  floretruntime.SQLiteStoreInspection{State: floretruntime.SQLiteStoreStateCurrent},
				}
				if request.Mode == floretruntime.SQLiteStoreMigrationApply {
					result.Committed = true
				}
				return result, nil
			}
			code := execute(context.Background(), tc.args, io.Discard, io.Discard, api, fixedOperationID)
			if code != tc.code {
				t.Fatalf("exit code = %d, want %d", code, tc.code)
			}
		})
	}
}

func TestMaintenanceErrorExitCodesAndHumanOutputAreSanitized(t *testing.T) {
	tests := []struct {
		name   string
		reason floretruntime.SQLiteStoreReason
		code   int
	}{
		{name: "busy", reason: floretruntime.SQLiteStoreReason("busy"), code: exitBusy},
		{name: "permission", reason: floretruntime.SQLiteStoreReasonPermission, code: exitIO},
		{name: "io", reason: floretruntime.SQLiteStoreReasonIO, code: exitIO},
		{name: "cancelled", reason: floretruntime.SQLiteStoreReasonCancelled, code: exitCancelled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := rejectingAPI(t)
			api.inspect = func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
				return floretruntime.SQLiteStoreInspection{}, &floretruntime.SQLiteStoreMaintenanceError{
					Operation: floretruntime.SQLiteStoreOperationInspect,
					Reason:    tc.reason,
					Err:       errors.New("sensitive path /private/customer/store.db"),
				}
			}
			stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
			code := execute(context.Background(), []string{"inspect", "/private/customer/store.db"}, stdout, stderr, api, fixedOperationID)
			if code != tc.code {
				t.Fatalf("exit code = %d, want %d", code, tc.code)
			}
			if stdout.Len() != 0 || strings.Contains(stderr.String(), "/private/") {
				t.Fatalf("human output leaked path: stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestUsageRejectsDangerousOrAmbiguousCommands(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"repair", "store.db"},
		{"reset", "store.db"},
		{"delete", "store.db"},
		{"inspect", "--apply", "store.db"},
		{"migrate", "first.db", "second.db"},
	} {
		stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
		code := execute(context.Background(), append([]string{"--json"}, args...), stdout, stderr, rejectingAPI(t), fixedOperationID)
		if code != exitUsage {
			t.Fatalf("args %q exit code = %d, want %d", args, code, exitUsage)
		}
		envelope := decodeSingleEnvelope(t, stdout.Bytes())
		if envelope.SchemaVersion != envelopeSchemaVersion || envelope.Status != "failed" || envelope.Reason != "invalid_request" {
			t.Fatalf("args %q envelope = %#v", args, envelope)
		}
		if !strings.Contains(stderr.String(), "Usage:") {
			t.Fatalf("args %q missing usage on stderr", args)
		}
	}
}

func TestPublicRuntimeMaintenanceSmoke(t *testing.T) {
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := floretruntime.OpenSQLiteStore(context.Background(), path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState: floretruntime.SQLiteStoreStateMissing,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"inspect", path}, {"verify", path}, {"migrate", path}} {
		if code := execute(context.Background(), args, io.Discard, io.Discard, publicMaintenanceAPI, fixedOperationID); code != exitSuccess {
			t.Fatalf("args %q exit code = %d, want %d", args, code, exitSuccess)
		}
	}
}

func rejectingAPI(t *testing.T) maintenanceAPI {
	t.Helper()
	return maintenanceAPI{
		inspect: func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
			t.Fatal("unexpected inspect call")
			return floretruntime.SQLiteStoreInspection{}, nil
		},
		verify: func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreVerification, error) {
			t.Fatal("unexpected verify call")
			return floretruntime.SQLiteStoreVerification{}, nil
		},
		migrate: func(context.Context, string, floretruntime.SQLiteStoreMigrationRequest, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreMigrationResult, error) {
			t.Fatal("unexpected migrate call")
			return floretruntime.SQLiteStoreMigrationResult{}, nil
		},
	}
}

func fixedOperationID() (string, error) {
	return "operation-1", nil
}

func decodeSingleEnvelope(t *testing.T, data []byte) outputEnvelope {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	var envelope outputEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, data)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("stdout contains more than one JSON document: %v\n%s", err, data)
	}
	return envelope
}
