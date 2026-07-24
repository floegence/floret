package runtime_test

import (
	"context"
	"fmt"

	floretruntime "github.com/floegence/floret/runtime"
)

func openPublicSQLiteStoreForTest(path string, options ...floretruntime.SQLiteStoreOption) (*floretruntime.Store, error) {
	ctx := context.Background()
	inspection, err := floretruntime.InspectSQLiteStore(ctx, path, options...)
	if err != nil {
		return nil, err
	}
	if inspection.State == floretruntime.SQLiteStoreStateMissing || inspection.State == floretruntime.SQLiteStoreStateEmpty {
		return floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{ExpectedState: inspection.State}, options...)
	}
	verification, err := floretruntime.VerifySQLiteStore(ctx, path, options...)
	if err != nil {
		return nil, err
	}
	if verification.Inspection.State != floretruntime.SQLiteStoreStateCurrent || verification.Inspection.LeasePolicyState != floretruntime.SQLiteStoreLeasePolicyMatches {
		return nil, fmt.Errorf("public test store verification state %q lease %q", verification.Inspection.State, verification.Inspection.LeasePolicyState)
	}
	for _, check := range verification.Checks {
		if !check.Passed {
			return nil, fmt.Errorf("public test store verification check %q failed", check.Code)
		}
	}
	return floretruntime.OpenSQLiteStore(ctx, path, floretruntime.SQLiteStoreOpenRequest{
		ExpectedState:  verification.Inspection.State,
		ExpectedSchema: verification.Inspection.Observed,
	}, options...)
}
