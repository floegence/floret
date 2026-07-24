package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	floretruntime "github.com/floegence/floret/runtime"
)

func TestExample(t *testing.T) {
	output := &bytes.Buffer{}
	if err := run(context.Background(), filepath.Join(t.TempDir(), "floret.db"), output, publicMaintenanceFunctions); err != nil {
		t.Fatal(err)
	}
	if output.Len() == 0 {
		t.Fatal("example emitted no product-neutral status")
	}
}

func TestMigrationCallsUseExpectedSchemaAndMonotonicProgress(t *testing.T) {
	expected := floretruntime.StoreSchemaIdentity{Version: "14", Fingerprint: "fixture"}
	api := maintenanceFunctions{
		inspect: func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
			return floretruntime.SQLiteStoreInspection{}, nil
		},
		verify: func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreVerification, error) {
			return floretruntime.SQLiteStoreVerification{}, nil
		},
		migrate: func(_ context.Context, _ string, request floretruntime.SQLiteStoreMigrationRequest, _ ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreMigrationResult, error) {
			if request.ExpectedSchema != expected {
				t.Fatalf("expected schema = %#v", request.ExpectedSchema)
			}
			request.Progress(floretruntime.SQLiteStoreMaintenanceProgress{
				OperationID: request.OperationID, Sequence: 1, Phase: floretruntime.SQLiteStoreMaintenancePreflight,
				Status: floretruntime.SQLiteStoreMaintenanceRunning, Step: 1, Total: 4, SafeToCancel: true,
			})
			request.Progress(floretruntime.SQLiteStoreMaintenanceProgress{
				OperationID: request.OperationID, Sequence: 2, Phase: floretruntime.SQLiteStoreMaintenanceVerifying,
				Status: floretruntime.SQLiteStoreMaintenanceReady, Step: 4, Total: 4, SafeToRetry: true,
			})
			return floretruntime.SQLiteStoreMigrationResult{
				OperationID: request.OperationID, Mode: request.Mode, Status: floretruntime.SQLiteStoreMaintenanceReady,
			}, nil
		},
	}
	for _, mode := range []floretruntime.SQLiteStoreMigrationMode{
		floretruntime.SQLiteStoreMigrationPlan,
		floretruntime.SQLiteStoreMigrationApply,
	} {
		result, progress, err := runMigrationOperation(context.Background(), api, "fixture.db", "operation-"+string(mode), mode, expected)
		if err != nil || result.Mode != mode || len(progress) != 2 {
			t.Fatalf("mode=%s result=%#v progress=%#v err=%v", mode, result, progress, err)
		}
	}
}

func TestTypedMaintenanceOutcomeMapping(t *testing.T) {
	sensitive := errors.New("fixture details must not be classified by text")
	tests := []struct {
		name string
		call func(maintenanceFunctions) (typedOutcome, error)
		api  maintenanceFunctions
		want typedOutcome
	}{
		{
			name: "busy",
			call: func(api maintenanceFunctions) (typedOutcome, error) {
				return migrateTypedOutcome(context.Background(), api, "fixture.db", floretruntime.SQLiteStoreMigrationRequest{
					OperationID: "busy-operation", Mode: floretruntime.SQLiteStoreMigrationApply,
				})
			},
			api: maintenanceFunctions{
				migrate: func(context.Context, string, floretruntime.SQLiteStoreMigrationRequest, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreMigrationResult, error) {
					return floretruntime.SQLiteStoreMigrationResult{}, &floretruntime.SQLiteStoreMaintenanceError{
						Operation: floretruntime.SQLiteStoreOperationMigrate, Reason: floretruntime.SQLiteStoreReasonBusy,
						Retryable: true, SafeToRetry: true, Err: sensitive,
					}
				},
			},
			want: typedOutcome{
				StoreState: floretruntime.SQLiteStoreStateBusy, Reason: floretruntime.SQLiteStoreReasonBusy,
				Retryable: true, SafeToRetry: true,
			},
		},
		{
			name: "future",
			call: func(api maintenanceFunctions) (typedOutcome, error) {
				return inspectTypedOutcome(context.Background(), api, "fixture.db")
			},
			api: maintenanceFunctions{
				inspect: func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
					return floretruntime.SQLiteStoreInspection{
						State: floretruntime.SQLiteStoreStateFuture, Reason: floretruntime.SQLiteStoreReasonNewerReader,
					}, nil
				},
			},
			want: typedOutcome{StoreState: floretruntime.SQLiteStoreStateFuture, Reason: floretruntime.SQLiteStoreReasonNewerReader},
		},
		{
			name: "corrupt",
			call: func(api maintenanceFunctions) (typedOutcome, error) {
				return inspectTypedOutcome(context.Background(), api, "fixture.db")
			},
			api: maintenanceFunctions{
				inspect: func(context.Context, string, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreInspection, error) {
					return floretruntime.SQLiteStoreInspection{
						State: floretruntime.SQLiteStoreStateCorrupt, Reason: floretruntime.SQLiteStoreReasonCorrupt,
					}, nil
				},
			},
			want: typedOutcome{StoreState: floretruntime.SQLiteStoreStateCorrupt, Reason: floretruntime.SQLiteStoreReasonCorrupt},
		},
		{
			name: "rollback",
			call: func(api maintenanceFunctions) (typedOutcome, error) {
				return migrateTypedOutcome(context.Background(), api, "fixture.db", floretruntime.SQLiteStoreMigrationRequest{
					OperationID: "rollback-operation", Mode: floretruntime.SQLiteStoreMigrationApply,
				})
			},
			api: maintenanceFunctions{
				migrate: func(context.Context, string, floretruntime.SQLiteStoreMigrationRequest, ...floretruntime.SQLiteStoreOption) (floretruntime.SQLiteStoreMigrationResult, error) {
					return floretruntime.SQLiteStoreMigrationResult{
							Before: floretruntime.SQLiteStoreInspection{State: floretruntime.SQLiteStoreStateUpgradeable},
							Status: floretruntime.SQLiteStoreMaintenanceFailed, Reason: floretruntime.SQLiteStoreReasonMigrationFailed,
							RolledBack: true, SafeToRetry: true,
						}, &floretruntime.SQLiteStoreMaintenanceError{
							Operation: floretruntime.SQLiteStoreOperationMigrate, Reason: floretruntime.SQLiteStoreReasonMigrationFailed,
							SafeToRetry: true, Err: sensitive,
						}
				},
			},
			want: typedOutcome{
				StoreState: floretruntime.SQLiteStoreStateUpgradeable, OperationStatus: floretruntime.SQLiteStoreMaintenanceFailed,
				Reason: floretruntime.SQLiteStoreReasonMigrationFailed, RolledBack: true, SafeToRetry: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := test.call(test.api)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("typed outcome = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestProgressValidationRejectsRegressions(t *testing.T) {
	base := []floretruntime.SQLiteStoreMaintenanceProgress{
		{OperationID: "operation", Sequence: 1, Phase: floretruntime.SQLiteStoreMaintenancePreflight, Step: 1},
		{OperationID: "operation", Sequence: 2, Phase: floretruntime.SQLiteStoreMaintenanceVerifying, Step: 4},
	}
	if err := validateMonotonicProgress("operation", base); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]floretruntime.SQLiteStoreMaintenanceProgress){
		"identity": func(progress []floretruntime.SQLiteStoreMaintenanceProgress) { progress[1].OperationID = "other" },
		"sequence": func(progress []floretruntime.SQLiteStoreMaintenanceProgress) { progress[1].Sequence = 1 },
		"step":     func(progress []floretruntime.SQLiteStoreMaintenanceProgress) { progress[1].Step = 0 },
		"phase": func(progress []floretruntime.SQLiteStoreMaintenanceProgress) {
			progress[0].Phase = floretruntime.SQLiteStoreMaintenanceVerifying
			progress[1].Phase = floretruntime.SQLiteStoreMaintenanceMigrating
		},
		"commit": func(progress []floretruntime.SQLiteStoreMaintenanceProgress) {
			progress[0].Committed = true
			progress[1].Committed = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			progress := append([]floretruntime.SQLiteStoreMaintenanceProgress(nil), base...)
			mutate(progress)
			if err := validateMonotonicProgress("operation", progress); err == nil {
				t.Fatal("progress regression passed validation")
			}
		})
	}
}
