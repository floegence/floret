package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/storage/sqlite"
)

type SQLiteStoreState string

const (
	SQLiteStoreStateMissing          SQLiteStoreState = "missing"
	SQLiteStoreStateEmpty            SQLiteStoreState = "empty"
	SQLiteStoreStateCurrent          SQLiteStoreState = "current"
	SQLiteStoreStateUpgradeable      SQLiteStoreState = "upgradeable"
	SQLiteStoreStateUnsupportedOlder SQLiteStoreState = "unsupported_older"
	SQLiteStoreStateFuture           SQLiteStoreState = "future"
	SQLiteStoreStateDrifted          SQLiteStoreState = "drifted"
	SQLiteStoreStateCorrupt          SQLiteStoreState = "corrupt"
	SQLiteStoreStateBusy             SQLiteStoreState = "busy"
	SQLiteStoreStatePermissionDenied SQLiteStoreState = "permission_denied"
	SQLiteStoreStateIOError          SQLiteStoreState = "io_error"
)

type SQLiteStoreKind string

const (
	SQLiteStoreKindUnknown SQLiteStoreKind = "unknown"
	SQLiteStoreKindFloret  SQLiteStoreKind = "floret"
)

type SQLiteStoreAction string

const (
	SQLiteStoreActionRetryInspection     SQLiteStoreAction = "retry_inspection"
	SQLiteStoreActionMigrate             SQLiteStoreAction = "migrate"
	SQLiteStoreActionRequiresNewerReader SQLiteStoreAction = "requires_newer_reader"
	SQLiteStoreActionExportDiagnostics   SQLiteStoreAction = "export_diagnostics"
)

type SQLiteStoreLeasePolicyState string

const (
	SQLiteStoreLeasePolicyUnavailable SQLiteStoreLeasePolicyState = "unavailable"
	SQLiteStoreLeasePolicyMatches     SQLiteStoreLeasePolicyState = "matches"
	SQLiteStoreLeasePolicyMismatch    SQLiteStoreLeasePolicyState = "mismatch"
)

type SQLiteStoreInspection struct {
	Kind                 SQLiteStoreKind              `json:"kind"`
	State                SQLiteStoreState             `json:"state"`
	Exists               bool                         `json:"exists"`
	Empty                bool                         `json:"empty"`
	Observed             StoreSchemaIdentity          `json:"observed"`
	Current              StoreSchemaIdentity          `json:"current"`
	Migratable           []StoreSchemaMigrationSource `json:"migratable"`
	PersistedLeasePolicy *StoreLeasePolicy            `json:"persisted_lease_policy,omitempty"`
	RequestedLeasePolicy StoreLeasePolicy             `json:"requested_lease_policy"`
	LeasePolicyState     SQLiteStoreLeasePolicyState  `json:"lease_policy_state"`
	AutomaticMigration   bool                         `json:"automatic_migration"`
	RequiresExclusive    bool                         `json:"requires_exclusive_access"`
	Retryable            bool                         `json:"retryable"`
	SafeToRetry          bool                         `json:"safe_to_retry"`
	Actions              []SQLiteStoreAction          `json:"actions,omitempty"`
	Reason               SQLiteStoreReason            `json:"reason"`
	SafeDetail           string                       `json:"safe_detail,omitempty"`
}

type SQLiteStoreVerificationCheck struct {
	Code       string `json:"code"`
	Passed     bool   `json:"passed"`
	SafeDetail string `json:"safe_detail,omitempty"`
}

type SQLiteStoreVerification struct {
	Inspection SQLiteStoreInspection          `json:"inspection"`
	Checks     []SQLiteStoreVerificationCheck `json:"checks"`
}

type SQLiteStoreMigrationMode string

const (
	SQLiteStoreMigrationPlan  SQLiteStoreMigrationMode = "plan"
	SQLiteStoreMigrationApply SQLiteStoreMigrationMode = "apply"
)

type SQLiteStoreMaintenancePhase string

const (
	SQLiteStoreMaintenancePreflight SQLiteStoreMaintenancePhase = "preflight"
	SQLiteStoreMaintenanceWaiting   SQLiteStoreMaintenancePhase = "waiting_for_exclusive_access"
	SQLiteStoreMaintenanceMigrating SQLiteStoreMaintenancePhase = "migrating"
	SQLiteStoreMaintenanceVerifying SQLiteStoreMaintenancePhase = "verifying"
)

type SQLiteStoreMaintenanceStatus string

const (
	SQLiteStoreMaintenanceRunning   SQLiteStoreMaintenanceStatus = "running"
	SQLiteStoreMaintenanceReady     SQLiteStoreMaintenanceStatus = "ready"
	SQLiteStoreMaintenanceFailed    SQLiteStoreMaintenanceStatus = "failed"
	SQLiteStoreMaintenanceCancelled SQLiteStoreMaintenanceStatus = "cancelled"
)

type SQLiteStoreMaintenanceProgress struct {
	OperationID  string                       `json:"operation_id"`
	Sequence     uint64                       `json:"sequence"`
	Phase        SQLiteStoreMaintenancePhase  `json:"phase"`
	Status       SQLiteStoreMaintenanceStatus `json:"status"`
	Step         int                          `json:"step,omitempty"`
	Total        int                          `json:"total,omitempty"`
	SafeToCancel bool                         `json:"safe_to_cancel"`
	Committed    bool                         `json:"committed"`
	RolledBack   bool                         `json:"rolled_back"`
	Retryable    bool                         `json:"retryable"`
	SafeToRetry  bool                         `json:"safe_to_retry"`
	Reason       SQLiteStoreReason            `json:"reason,omitempty"`
}

type SQLiteStoreMigrationStep struct {
	From StoreSchemaIdentity `json:"from"`
	To   StoreSchemaIdentity `json:"to"`
	Code string              `json:"code"`
}

type SQLiteStoreMigrationRequest struct {
	OperationID    string
	Mode           SQLiteStoreMigrationMode
	ExpectedSchema StoreSchemaIdentity
	Progress       func(SQLiteStoreMaintenanceProgress)
}

type SQLiteStoreMigrationResult struct {
	OperationID string                       `json:"operation_id"`
	Mode        SQLiteStoreMigrationMode     `json:"mode"`
	Before      SQLiteStoreInspection        `json:"before"`
	After       SQLiteStoreInspection        `json:"after"`
	Steps       []SQLiteStoreMigrationStep   `json:"steps,omitempty"`
	Status      SQLiteStoreMaintenanceStatus `json:"status"`
	Changed     bool                         `json:"changed"`
	Committed   bool                         `json:"committed"`
	RolledBack  bool                         `json:"rolled_back"`
	Retryable   bool                         `json:"retryable"`
	SafeToRetry bool                         `json:"safe_to_retry"`
	Reason      SQLiteStoreReason            `json:"reason,omitempty"`
}

type SQLiteStoreMaintenanceOperation string

const (
	SQLiteStoreOperationInspect SQLiteStoreMaintenanceOperation = "inspect"
	SQLiteStoreOperationVerify  SQLiteStoreMaintenanceOperation = "verify"
	SQLiteStoreOperationMigrate SQLiteStoreMaintenanceOperation = "migrate"
)

type SQLiteStoreReason string

const (
	SQLiteStoreReasonInvalidRequest     SQLiteStoreReason = "invalid_request"
	SQLiteStoreReasonCancelled          SQLiteStoreReason = "cancelled"
	SQLiteStoreReasonBusy               SQLiteStoreReason = "busy"
	SQLiteStoreReasonPermission         SQLiteStoreReason = "permission_denied"
	SQLiteStoreReasonIO                 SQLiteStoreReason = "io_error"
	SQLiteStoreReasonCorrupt            SQLiteStoreReason = "corrupt"
	SQLiteStoreReasonInspectionStale    SQLiteStoreReason = "inspection_stale"
	SQLiteStoreReasonStoreMissing       SQLiteStoreReason = "store_missing"
	SQLiteStoreReasonStoreEmpty         SQLiteStoreReason = "store_empty"
	SQLiteStoreReasonUnrecognized       SQLiteStoreReason = "unrecognized_store"
	SQLiteStoreReasonSchemaMetadata     SQLiteStoreReason = "schema_metadata_invalid"
	SQLiteStoreReasonNewerReader        SQLiteStoreReason = "requires_newer_reader"
	SQLiteStoreReasonUnsupported        SQLiteStoreReason = "unsupported_older_schema"
	SQLiteStoreReasonFingerprint        SQLiteStoreReason = "schema_fingerprint_mismatch"
	SQLiteStoreReasonContract           SQLiteStoreReason = "schema_contract_mismatch"
	SQLiteStoreReasonLegacyMigration    SQLiteStoreReason = "non_empty_schema_requires_legacy_migration"
	SQLiteStoreReasonMigrationAvailable SQLiteStoreReason = "migration_available"
	SQLiteStoreReasonLeaseMismatch      SQLiteStoreReason = "lease_policy_mismatch"
	SQLiteStoreReasonCurrent            SQLiteStoreReason = "store_current"
	SQLiteStoreReasonMigrationFailed    SQLiteStoreReason = "migration_failed"
)

type SQLiteStoreMaintenanceError struct {
	Operation   SQLiteStoreMaintenanceOperation
	Reason      SQLiteStoreReason
	Retryable   bool
	SafeToRetry bool
	Err         error
}

func (e *SQLiteStoreMaintenanceError) Error() string {
	if e == nil {
		return "floret sqlite store maintenance failed"
	}
	return fmt.Sprintf("floret sqlite store %s failed: %s", e.Operation, e.Reason)
}

func (e *SQLiteStoreMaintenanceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func InspectSQLiteStore(ctx context.Context, path string, options ...SQLiteStoreOption) (SQLiteStoreInspection, error) {
	configured, err := resolveSQLiteStoreOptions(options)
	if err != nil {
		return SQLiteStoreInspection{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationInspect, SQLiteStoreReasonInvalidRequest, false, false, err)
	}
	inspection, err := sqlite.Inspect(ctx, path, configured.leasePolicy)
	if err != nil {
		return SQLiteStoreInspection{}, maintenanceError(SQLiteStoreOperationInspect, err)
	}
	return mapSQLiteStoreInspection(inspection), nil
}

func VerifySQLiteStore(ctx context.Context, path string, options ...SQLiteStoreOption) (SQLiteStoreVerification, error) {
	configured, err := resolveSQLiteStoreOptions(options)
	if err != nil {
		return SQLiteStoreVerification{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationVerify, SQLiteStoreReasonInvalidRequest, false, false, err)
	}
	verification, err := sqlite.Verify(ctx, path, configured.leasePolicy)
	if err != nil {
		return SQLiteStoreVerification{}, maintenanceError(SQLiteStoreOperationVerify, err)
	}
	checks := make([]SQLiteStoreVerificationCheck, len(verification.Checks))
	for index, check := range verification.Checks {
		checks[index] = SQLiteStoreVerificationCheck{Code: check.Code, Passed: check.Passed, SafeDetail: check.SafeDetail}
	}
	return SQLiteStoreVerification{Inspection: mapSQLiteStoreInspection(verification.Inspection), Checks: checks}, nil
}

func MigrateSQLiteStore(ctx context.Context, path string, request SQLiteStoreMigrationRequest, options ...SQLiteStoreOption) (SQLiteStoreMigrationResult, error) {
	request.OperationID = strings.TrimSpace(request.OperationID)
	if request.OperationID == "" {
		return SQLiteStoreMigrationResult{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationMigrate, SQLiteStoreReasonInvalidRequest, false, false, errors.New("sqlite store migration operation id is required"))
	}
	configured, err := resolveSQLiteStoreOptions(options)
	if err != nil {
		return SQLiteStoreMigrationResult{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationMigrate, SQLiteStoreReasonInvalidRequest, false, false, err)
	}
	internalRequest := sqlite.MigrationRequest{
		Mode:           sqlite.MigrationMode(request.Mode),
		ExpectedSchema: storage.StoreSchemaIdentity{Version: request.ExpectedSchema.Version, Fingerprint: request.ExpectedSchema.Fingerprint},
		LeasePolicy:    configured.leasePolicy,
	}
	if request.Progress != nil {
		internalRequest.Progress = func(progress sqlite.MaintenanceProgress) {
			request.Progress(SQLiteStoreMaintenanceProgress{
				OperationID: request.OperationID, Sequence: progress.Sequence,
				Phase: SQLiteStoreMaintenancePhase(progress.Phase), Status: SQLiteStoreMaintenanceStatus(progress.Status),
				Step: progress.Step, Total: progress.Total, SafeToCancel: progress.SafeToCancel,
				Committed: progress.Committed, RolledBack: progress.RolledBack,
				Retryable: progress.Retryable, SafeToRetry: progress.SafeToRetry, Reason: SQLiteStoreReason(progress.Reason),
			})
		}
	}
	result, err := sqlite.Migrate(ctx, path, internalRequest)
	mapped := mapSQLiteStoreMigrationResult(request.OperationID, result)
	if err != nil {
		return mapped, maintenanceError(SQLiteStoreOperationMigrate, err)
	}
	return mapped, nil
}

func mapSQLiteStoreInspection(inspection sqlite.MaintenanceInspection) SQLiteStoreInspection {
	mapped := SQLiteStoreInspection{
		Kind: SQLiteStoreKind(inspection.Kind), State: SQLiteStoreState(inspection.State), Exists: inspection.Exists, Empty: inspection.Empty,
		Observed: mapStoreSchemaIdentity(inspection.Observed), Current: mapStoreSchemaIdentity(inspection.Current),
		Migratable:           mapStoreSchemaMigrationSources(inspection.Migratable),
		RequestedLeasePolicy: publicStoreLeasePolicy(inspection.RequestedLeasePolicy),
		LeasePolicyState:     SQLiteStoreLeasePolicyUnavailable,
		AutomaticMigration:   inspection.AutomaticMigration,
		RequiresExclusive:    inspection.RequiresExclusive,
		Retryable:            inspection.Retryable,
		SafeToRetry:          inspection.SafeToRetry,
		Reason:               SQLiteStoreReason(inspection.Reason), SafeDetail: inspection.SafeDetail,
	}
	if inspection.PersistedLeasePolicy != nil {
		persisted := publicStoreLeasePolicy(*inspection.PersistedLeasePolicy)
		mapped.PersistedLeasePolicy = &persisted
		if inspection.LeasePolicyMatches {
			mapped.LeasePolicyState = SQLiteStoreLeasePolicyMatches
		} else {
			mapped.LeasePolicyState = SQLiteStoreLeasePolicyMismatch
		}
	}
	switch {
	case mapped.LeasePolicyState == SQLiteStoreLeasePolicyMismatch:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionExportDiagnostics}
	case mapped.State == SQLiteStoreStateCurrent:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionRetryInspection, SQLiteStoreActionExportDiagnostics}
	case mapped.State == SQLiteStoreStateUpgradeable:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionMigrate, SQLiteStoreActionRetryInspection, SQLiteStoreActionExportDiagnostics}
	case mapped.State == SQLiteStoreStateFuture:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionRequiresNewerReader, SQLiteStoreActionExportDiagnostics}
	case mapped.State == SQLiteStoreStateBusy:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionRetryInspection, SQLiteStoreActionExportDiagnostics}
	case mapped.State == SQLiteStoreStatePermissionDenied:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionExportDiagnostics}
	case mapped.State == SQLiteStoreStateIOError && !mapped.SafeToRetry:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionExportDiagnostics}
	default:
		mapped.Actions = []SQLiteStoreAction{SQLiteStoreActionRetryInspection, SQLiteStoreActionExportDiagnostics}
	}
	return mapped
}

func mapStoreSchemaIdentity(identity storage.StoreSchemaIdentity) StoreSchemaIdentity {
	return StoreSchemaIdentity{Version: identity.Version, Fingerprint: identity.Fingerprint}
}

func mapSQLiteStoreMigrationResult(operationID string, result sqlite.MigrationResult) SQLiteStoreMigrationResult {
	steps := make([]SQLiteStoreMigrationStep, len(result.Steps))
	for index, step := range result.Steps {
		steps[index] = SQLiteStoreMigrationStep{From: mapStoreSchemaIdentity(step.From), To: mapStoreSchemaIdentity(step.To), Code: step.Code}
	}
	return SQLiteStoreMigrationResult{
		OperationID: operationID, Mode: SQLiteStoreMigrationMode(result.Mode),
		Before: mapSQLiteStoreInspection(result.Before), After: mapSQLiteStoreInspection(result.After), Steps: steps,
		Status: SQLiteStoreMaintenanceStatus(result.Status), Changed: result.Changed,
		Committed: result.Committed, RolledBack: result.RolledBack,
		Retryable: result.Retryable, SafeToRetry: result.SafeToRetry, Reason: SQLiteStoreReason(result.Reason),
	}
}

func maintenanceError(operation SQLiteStoreMaintenanceOperation, err error) error {
	var existing *SQLiteStoreMaintenanceError
	if errors.As(err, &existing) {
		return err
	}
	var internal *sqlite.MaintenanceError
	if errors.As(err, &internal) {
		return newSQLiteStoreMaintenanceError(
			operation,
			SQLiteStoreReason(internal.Reason),
			internal.Retryable,
			internal.SafeToRetry,
			err,
		)
	}
	reason := SQLiteStoreReasonIO
	retryable := false
	safeToRetry := false
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		reason = SQLiteStoreReasonCancelled
		safeToRetry = true
	case errors.Is(err, os.ErrPermission):
		reason = SQLiteStoreReasonPermission
	}
	return newSQLiteStoreMaintenanceError(operation, reason, retryable, safeToRetry, err)
}

func newSQLiteStoreMaintenanceError(operation SQLiteStoreMaintenanceOperation, reason SQLiteStoreReason, retryable, safeToRetry bool, err error) error {
	return &SQLiteStoreMaintenanceError{
		Operation: operation, Reason: reason, Retryable: retryable, SafeToRetry: safeToRetry, Err: err,
	}
}
