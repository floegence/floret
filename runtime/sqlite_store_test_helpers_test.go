package runtime

import (
	"context"
	"fmt"
)

func openSQLiteStoreForTest(path string, options ...SQLiteStoreOption) (*Store, error) {
	ctx := context.Background()
	inspection, err := InspectSQLiteStore(ctx, path, options...)
	if err != nil {
		return nil, err
	}
	switch inspection.State {
	case SQLiteStoreStateMissing, SQLiteStoreStateEmpty:
		return OpenSQLiteStore(ctx, path, SQLiteStoreOpenRequest{ExpectedState: inspection.State}, options...)
	case SQLiteStoreStateUpgradeable:
		result, err := MigrateSQLiteStore(ctx, path, SQLiteStoreMigrationRequest{
			OperationID: "runtime-test-store-migration",
			Mode:        SQLiteStoreMigrationApply,
			ExpectedSchema: StoreSchemaIdentity{
				Version: inspection.Observed.Version, Fingerprint: inspection.Observed.Fingerprint,
			},
		}, options...)
		if err != nil {
			return nil, err
		}
		if result.Status != SQLiteStoreMaintenanceReady {
			return nil, fmt.Errorf("runtime test store migration status %q", result.Status)
		}
	case SQLiteStoreStateCurrent:
	default:
		return nil, fmt.Errorf("runtime test store state %q cannot be opened", inspection.State)
	}
	verification, err := VerifySQLiteStore(ctx, path, options...)
	if err != nil {
		return nil, err
	}
	if verification.Inspection.State != SQLiteStoreStateCurrent || verification.Inspection.LeasePolicyState != SQLiteStoreLeasePolicyMatches {
		return nil, fmt.Errorf("runtime test store verification state %q lease %q", verification.Inspection.State, verification.Inspection.LeasePolicyState)
	}
	for _, check := range verification.Checks {
		if !check.Passed {
			return nil, fmt.Errorf("runtime test store verification check %q failed", check.Code)
		}
	}
	return OpenSQLiteStore(ctx, path, SQLiteStoreOpenRequest{
		ExpectedState:  verification.Inspection.State,
		ExpectedSchema: verification.Inspection.Observed,
	}, options...)
}
