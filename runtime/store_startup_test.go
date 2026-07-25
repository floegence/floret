package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestSQLiteStartupReinspectsBusyVerificationOnce(t *testing.T) {
	api := &scriptedSQLiteStoreStartupAPI{
		inspections: []SQLiteStoreInspection{currentSQLiteStartupInspection(), currentSQLiteStartupInspection()},
		verifications: []SQLiteStoreVerification{
			{Inspection: SQLiteStoreInspection{
				State: SQLiteStoreStateBusy, Reason: SQLiteStoreReasonBusy,
				Retryable: true, SafeToRetry: true,
			}},
			currentSQLiteStartupVerification(),
		},
		stores: []*Store{{}},
	}
	result, err := startSQLiteStoreWithAPI(
		context.Background(), "store.db", SQLiteStartupRequest{}, SQLiteMigrationRefuse, nil, api,
	)
	if err != nil || result.Store == nil || result.Inspection == nil || result.Verification == nil {
		t.Fatalf("startup result=%#v err=%v", result, err)
	}
	if api.inspectCalls != 2 || api.verifyCalls != 2 || api.openCalls != 1 {
		t.Fatalf("calls inspect/verify/open=%d/%d/%d, want 2/2/1", api.inspectCalls, api.verifyCalls, api.openCalls)
	}
}

func TestSQLiteStartupStopsAfterOneBusyVerificationReinspection(t *testing.T) {
	busy := SQLiteStoreVerification{Inspection: SQLiteStoreInspection{
		State: SQLiteStoreStateBusy, Reason: SQLiteStoreReasonBusy,
		Retryable: true, SafeToRetry: true,
	}}
	api := &scriptedSQLiteStoreStartupAPI{
		inspections:   []SQLiteStoreInspection{currentSQLiteStartupInspection(), currentSQLiteStartupInspection()},
		verifications: []SQLiteStoreVerification{busy, busy},
	}
	result, err := startSQLiteStoreWithAPI(
		context.Background(), "store.db", SQLiteStartupRequest{}, SQLiteMigrationRefuse, nil, api,
	)
	var maintenance *SQLiteStoreMaintenanceError
	if result.Store != nil || !errors.As(err, &maintenance) || maintenance.Operation != SQLiteStoreOperationVerify ||
		maintenance.Reason != SQLiteStoreReasonBusy || !maintenance.SafeToRetry {
		t.Fatalf("startup result=%#v error=%#v err=%v", result, maintenance, err)
	}
	if api.inspectCalls != 2 || api.verifyCalls != 2 || api.openCalls != 0 {
		t.Fatalf("calls inspect/verify/open=%d/%d/%d, want 2/2/0", api.inspectCalls, api.verifyCalls, api.openCalls)
	}
}

func TestSQLiteStartupReportsLeaseMismatchFromLatestInspection(t *testing.T) {
	mismatch := currentSQLiteStartupInspection()
	mismatch.LeasePolicyState = SQLiteStoreLeasePolicyMismatch
	mismatch.Reason = SQLiteStoreReasonLeaseMismatch
	api := &scriptedSQLiteStoreStartupAPI{
		inspections: []SQLiteStoreInspection{
			{State: SQLiteStoreStateMissing, Reason: SQLiteStoreReasonStoreMissing},
			mismatch,
		},
		openErrors: []error{newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationOpen,
			SQLiteStoreReasonInspectionStale,
			true,
			true,
			errors.New("stale open"),
		)},
	}
	result, err := startSQLiteStoreWithAPI(
		context.Background(), "store.db", SQLiteStartupRequest{}, SQLiteMigrationRefuse, nil, api,
	)
	var maintenance *SQLiteStoreMaintenanceError
	if result.Inspection == nil || result.Inspection.LeasePolicyState != SQLiteStoreLeasePolicyMismatch ||
		!errors.As(err, &maintenance) || maintenance.Reason != SQLiteStoreReasonLeaseMismatch {
		t.Fatalf("startup result=%#v error=%#v err=%v", result, maintenance, err)
	}
	if api.inspectCalls != 2 || api.openCalls != 1 || api.verifyCalls != 0 {
		t.Fatalf("calls inspect/verify/open=%d/%d/%d, want 2/0/1", api.inspectCalls, api.verifyCalls, api.openCalls)
	}
}

type scriptedSQLiteStoreStartupAPI struct {
	inspections   []SQLiteStoreInspection
	verifications []SQLiteStoreVerification
	stores        []*Store
	openErrors    []error
	inspectCalls  int
	verifyCalls   int
	openCalls     int
}

func (a *scriptedSQLiteStoreStartupAPI) Inspect(context.Context, string, ...SQLiteStoreOption) (SQLiteStoreInspection, error) {
	a.inspectCalls++
	if len(a.inspections) == 0 {
		return SQLiteStoreInspection{}, errors.New("unexpected inspect")
	}
	inspection := a.inspections[0]
	a.inspections = a.inspections[1:]
	return inspection, nil
}

func (a *scriptedSQLiteStoreStartupAPI) Verify(context.Context, string, ...SQLiteStoreOption) (SQLiteStoreVerification, error) {
	a.verifyCalls++
	if len(a.verifications) == 0 {
		return SQLiteStoreVerification{}, errors.New("unexpected verify")
	}
	verification := a.verifications[0]
	a.verifications = a.verifications[1:]
	return verification, nil
}

func (*scriptedSQLiteStoreStartupAPI) Migrate(context.Context, string, SQLiteStoreMigrationRequest, ...SQLiteStoreOption) (SQLiteStoreMigrationResult, error) {
	return SQLiteStoreMigrationResult{}, errors.New("unexpected migrate")
}

func (a *scriptedSQLiteStoreStartupAPI) Open(context.Context, string, SQLiteStoreOpenRequest, ...SQLiteStoreOption) (*Store, error) {
	a.openCalls++
	if len(a.openErrors) > 0 {
		err := a.openErrors[0]
		a.openErrors = a.openErrors[1:]
		return nil, err
	}
	if len(a.stores) == 0 {
		return nil, errors.New("unexpected open")
	}
	store := a.stores[0]
	a.stores = a.stores[1:]
	return store, nil
}

func currentSQLiteStartupInspection() SQLiteStoreInspection {
	schema := StoreSchemaIdentity{Version: "current", Fingerprint: "fingerprint"}
	return SQLiteStoreInspection{
		Kind: SQLiteStoreKindFloret, State: SQLiteStoreStateCurrent, Exists: true,
		Observed: schema, Current: schema, LeasePolicyState: SQLiteStoreLeasePolicyMatches,
		Reason: SQLiteStoreReasonCurrent,
	}
}

func currentSQLiteStartupVerification() SQLiteStoreVerification {
	return SQLiteStoreVerification{
		Inspection: currentSQLiteStartupInspection(),
		Checks:     []SQLiteStoreVerificationCheck{{Code: "schema_contract", Passed: true}},
	}
}
