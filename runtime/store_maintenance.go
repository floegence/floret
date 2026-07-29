package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/floegence/floret/v2/internal/storage"
	"github.com/floegence/floret/v2/internal/storage/sqlite"
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

// Validate checks one self-contained Store inspection contract.
func (i SQLiteStoreInspection) Validate() error {
	switch i.Kind {
	case SQLiteStoreKindUnknown, SQLiteStoreKindFloret:
	default:
		return fmt.Errorf("unsupported sqlite store kind %q", i.Kind)
	}
	switch i.State {
	case SQLiteStoreStateMissing, SQLiteStoreStateEmpty, SQLiteStoreStateCurrent, SQLiteStoreStateUpgradeable,
		SQLiteStoreStateUnsupportedOlder, SQLiteStoreStateFuture, SQLiteStoreStateDrifted, SQLiteStoreStateCorrupt,
		SQLiteStoreStateBusy, SQLiteStoreStatePermissionDenied, SQLiteStoreStateIOError:
	default:
		return fmt.Errorf("unsupported sqlite store state %q", i.State)
	}
	if strings.TrimSpace(i.Current.Version) == "" || strings.TrimSpace(i.Current.Fingerprint) == "" {
		return errors.New("sqlite store inspection requires current schema identity")
	}
	if err := i.RequestedLeasePolicy.Validate(); err != nil {
		return fmt.Errorf("sqlite store requested lease policy: %w", err)
	}
	if i.PersistedLeasePolicy != nil {
		if err := i.PersistedLeasePolicy.Validate(); err != nil {
			return fmt.Errorf("sqlite store persisted lease policy: %w", err)
		}
		if i.LeasePolicyState == SQLiteStoreLeasePolicyUnavailable {
			return errors.New("sqlite store persisted lease policy has unavailable comparison state")
		}
	} else if i.LeasePolicyState != SQLiteStoreLeasePolicyUnavailable {
		return errors.New("sqlite store lease comparison requires persisted policy")
	}
	if i.LeasePolicyState != SQLiteStoreLeasePolicyUnavailable && i.LeasePolicyState != SQLiteStoreLeasePolicyMatches && i.LeasePolicyState != SQLiteStoreLeasePolicyMismatch {
		return fmt.Errorf("unsupported sqlite store lease policy state %q", i.LeasePolicyState)
	}
	if i.State == SQLiteStoreStateMissing && (i.Exists || i.Empty) {
		return errors.New("missing sqlite store inspection has inconsistent existence state")
	}
	if i.State == SQLiteStoreStateEmpty && (!i.Exists || !i.Empty) {
		return errors.New("empty sqlite store inspection has inconsistent existence state")
	}
	seenActions := make(map[SQLiteStoreAction]struct{}, len(i.Actions))
	for _, action := range i.Actions {
		switch action {
		case SQLiteStoreActionRetryInspection, SQLiteStoreActionMigrate, SQLiteStoreActionRequiresNewerReader, SQLiteStoreActionExportDiagnostics:
		default:
			return fmt.Errorf("unsupported sqlite store action %q", action)
		}
		if _, duplicate := seenActions[action]; duplicate {
			return fmt.Errorf("sqlite store inspection repeats action %q", action)
		}
		seenActions[action] = struct{}{}
	}
	for index, source := range i.Migratable {
		if strings.TrimSpace(source.Identity.Version) == "" {
			return fmt.Errorf("sqlite migration source %d has incomplete identity", index)
		}
		if source.Requirement != StoreSchemaMigrationRequirementNone && source.Requirement != StoreSchemaMigrationRequirementQuiescentAuthority {
			return fmt.Errorf("sqlite migration source %d has unsupported requirement %q", index, source.Requirement)
		}
	}
	return nil
}

// Validate checks one self-contained Store verification contract.
func (v SQLiteStoreVerification) Validate() error {
	if err := v.Inspection.Validate(); err != nil {
		return fmt.Errorf("sqlite store verification inspection: %w", err)
	}
	seen := make(map[string]struct{}, len(v.Checks))
	for index, check := range v.Checks {
		code := strings.TrimSpace(check.Code)
		if code == "" || code != check.Code {
			return fmt.Errorf("sqlite store verification check %d has invalid code", index)
		}
		if _, duplicate := seen[code]; duplicate {
			return fmt.Errorf("sqlite store verification repeats check %q", code)
		}
		seen[code] = struct{}{}
	}
	return nil
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

// SQLiteStoreOpenRequest binds a Store open to a maintenance inspection. Only
// missing, empty, and current inspections are valid open preconditions.
type SQLiteStoreOpenRequest struct {
	ExpectedState  SQLiteStoreState    `json:"expected_state"`
	ExpectedSchema StoreSchemaIdentity `json:"expected_schema"`
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

// Validate checks one self-contained Store migration result.
func (r SQLiteStoreMigrationResult) Validate() error {
	if !trimStableNonEmpty(r.OperationID) {
		return errors.New("sqlite store migration result requires operation id")
	}
	if r.Mode != SQLiteStoreMigrationPlan && r.Mode != SQLiteStoreMigrationApply {
		return fmt.Errorf("unsupported sqlite store migration mode %q", r.Mode)
	}
	switch r.Status {
	case SQLiteStoreMaintenanceRunning, SQLiteStoreMaintenanceReady, SQLiteStoreMaintenanceFailed, SQLiteStoreMaintenanceCancelled:
	default:
		return fmt.Errorf("unsupported sqlite store maintenance status %q", r.Status)
	}
	if err := r.Before.Validate(); err != nil {
		return fmt.Errorf("sqlite store migration before inspection: %w", err)
	}
	if err := r.After.Validate(); err != nil {
		return fmt.Errorf("sqlite store migration after inspection: %w", err)
	}
	if r.Committed && r.RolledBack {
		return errors.New("sqlite store migration cannot be committed and rolled back")
	}
	for index, step := range r.Steps {
		if !trimStableNonEmpty(step.Code) || strings.TrimSpace(step.From.Version) == "" || strings.TrimSpace(step.To.Version) == "" {
			return fmt.Errorf("sqlite store migration step %d is invalid", index)
		}
	}
	return nil
}

type SQLiteStoreMaintenanceOperation string

const (
	SQLiteStoreOperationInspect SQLiteStoreMaintenanceOperation = "inspect"
	SQLiteStoreOperationVerify  SQLiteStoreMaintenanceOperation = "verify"
	SQLiteStoreOperationMigrate SQLiteStoreMaintenanceOperation = "migrate"
	SQLiteStoreOperationOpen    SQLiteStoreMaintenanceOperation = "open"
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

// SQLiteMigrationPolicy controls whether StartSQLiteStore may apply a
// compatible schema migration. The zero value refuses migration.
type SQLiteMigrationPolicy string

const (
	SQLiteMigrationRefuse          SQLiteMigrationPolicy = "refuse"
	SQLiteMigrationApplyCompatible SQLiteMigrationPolicy = "apply_compatible"
)

type SQLiteStartupPhase string

const (
	SQLiteStartupInspecting SQLiteStartupPhase = "inspecting"
	SQLiteStartupMigrating  SQLiteStartupPhase = "migrating"
	SQLiteStartupVerifying  SQLiteStartupPhase = "verifying"
	SQLiteStartupOpening    SQLiteStartupPhase = "opening"
)

// SQLiteStartupProgress reports the current startup phase. Maintenance is set
// only for detailed migration progress; ordinary hosts can observe Phase alone.
type SQLiteStartupProgress struct {
	Phase       SQLiteStartupPhase              `json:"phase"`
	Maintenance *SQLiteStoreMaintenanceProgress `json:"maintenance,omitempty"`
}

// SQLiteStartupRequest configures one inspected and exact Store open. Existing
// current or migrated stores are also verified before open; missing or empty
// stores are initialized under the inspected precondition.
type SQLiteStartupRequest struct {
	MigrationPolicy SQLiteMigrationPolicy
	// MigrationOperationID is an optional correlation ID for an applied
	// migration. StartSQLiteStore derives a stable ID when it is omitted.
	MigrationOperationID string
	Progress             func(SQLiteStartupProgress)
}

// SQLiteStartupResult preserves the maintenance facts completed before a
// Store was opened or startup failed. Store is non-nil only on success.
type SQLiteStartupResult struct {
	Store        *Store
	Inspection   *SQLiteStoreInspection
	Verification *SQLiteStoreVerification
	Migration    *SQLiteStoreMigrationResult
}

// Validate checks the maintenance facts carried by one Store startup result.
func (r SQLiteStartupResult) Validate() error {
	if r.Inspection == nil {
		return errors.New("sqlite startup result requires inspection")
	}
	if err := r.Inspection.Validate(); err != nil {
		return fmt.Errorf("sqlite startup inspection: %w", err)
	}
	if r.Verification != nil {
		if err := r.Verification.Validate(); err != nil {
			return fmt.Errorf("sqlite startup verification: %w", err)
		}
	}
	if r.Migration != nil {
		if err := r.Migration.Validate(); err != nil {
			return fmt.Errorf("sqlite startup migration: %w", err)
		}
	}
	if r.Store != nil && r.Inspection.State != SQLiteStoreStateMissing && r.Inspection.State != SQLiteStoreStateEmpty && r.Verification == nil {
		return errors.New("opened existing sqlite startup result requires verification")
	}
	return nil
}

type sqliteStoreStartupAPI interface {
	Inspect(context.Context, string, ...SQLiteStoreOption) (SQLiteStoreInspection, error)
	Verify(context.Context, string, ...SQLiteStoreOption) (SQLiteStoreVerification, error)
	Migrate(context.Context, string, SQLiteStoreMigrationRequest, ...SQLiteStoreOption) (SQLiteStoreMigrationResult, error)
	Open(context.Context, string, SQLiteStoreOpenRequest, ...SQLiteStoreOption) (*Store, error)
}

type publicSQLiteStoreStartupAPI struct{}

func (publicSQLiteStoreStartupAPI) Inspect(ctx context.Context, path string, options ...SQLiteStoreOption) (SQLiteStoreInspection, error) {
	return InspectSQLiteStore(ctx, path, options...)
}

func (publicSQLiteStoreStartupAPI) Verify(ctx context.Context, path string, options ...SQLiteStoreOption) (SQLiteStoreVerification, error) {
	return VerifySQLiteStore(ctx, path, options...)
}

func (publicSQLiteStoreStartupAPI) Migrate(ctx context.Context, path string, request SQLiteStoreMigrationRequest, options ...SQLiteStoreOption) (SQLiteStoreMigrationResult, error) {
	return MigrateSQLiteStore(ctx, path, request, options...)
}

func (publicSQLiteStoreStartupAPI) Open(ctx context.Context, path string, request SQLiteStoreOpenRequest, options ...SQLiteStoreOption) (*Store, error) {
	return OpenSQLiteStore(ctx, path, request, options...)
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
	mapped := mapSQLiteStoreInspection(inspection)
	if err := mapped.Validate(); err != nil {
		return SQLiteStoreInspection{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationInspect, SQLiteStoreReasonContract, false, false, err)
	}
	return mapped, nil
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
	mapped := SQLiteStoreVerification{Inspection: mapSQLiteStoreInspection(verification.Inspection), Checks: checks}
	if err := mapped.Validate(); err != nil {
		return SQLiteStoreVerification{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationVerify, SQLiteStoreReasonContract, false, false, err)
	}
	return mapped, nil
}

// StartSQLiteStore runs the safe maintenance state machine and returns an
// exact-open Store. It never migrates unless apply_compatible is explicit.
func StartSQLiteStore(ctx context.Context, path string, request SQLiteStartupRequest, options ...SQLiteStoreOption) (SQLiteStartupResult, error) {
	policy := request.MigrationPolicy
	if policy == "" {
		policy = SQLiteMigrationRefuse
	}
	if policy != SQLiteMigrationRefuse && policy != SQLiteMigrationApplyCompatible {
		return SQLiteStartupResult{}, newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationOpen,
			SQLiteStoreReasonInvalidRequest,
			false,
			false,
			fmt.Errorf("unsupported sqlite migration policy %q", request.MigrationPolicy),
		)
	}
	request.MigrationOperationID = strings.TrimSpace(request.MigrationOperationID)
	configured, err := resolveSQLiteStoreOptions(options)
	if err != nil {
		return SQLiteStartupResult{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationOpen, SQLiteStoreReasonInvalidRequest, false, false, err)
	}
	stableOptions := []SQLiteStoreOption{func(target *sqliteStoreOptions) { *target = configured }}
	result, err := startSQLiteStoreWithAPI(ctx, path, request, policy, stableOptions, publicSQLiteStoreStartupAPI{})
	if err != nil {
		return result, err
	}
	if validationErr := result.Validate(); validationErr != nil {
		if result.Store != nil {
			_ = result.Store.Close()
			result.Store = nil
		}
		return result, newSQLiteStoreMaintenanceError(SQLiteStoreOperationOpen, SQLiteStoreReasonContract, false, false, validationErr)
	}
	return result, nil
}

func startSQLiteStoreWithAPI(ctx context.Context, path string, request SQLiteStartupRequest, policy SQLiteMigrationPolicy, options []SQLiteStoreOption, api sqliteStoreStartupAPI) (SQLiteStartupResult, error) {
	result := SQLiteStartupResult{}
	inspection, err := inspectSQLiteStoreForStartup(ctx, path, request.Progress, api, options...)
	if err != nil {
		return result, err
	}
	result.Inspection = &inspection
	return startSQLiteStoreFromInspection(ctx, path, request, policy, options, api, inspection, &result, true)
}

func startSQLiteStoreFromInspection(
	ctx context.Context,
	path string,
	request SQLiteStartupRequest,
	policy SQLiteMigrationPolicy,
	options []SQLiteStoreOption,
	api sqliteStoreStartupAPI,
	inspection SQLiteStoreInspection,
	result *SQLiteStartupResult,
	allowRecovery bool,
) (SQLiteStartupResult, error) {
	if inspection.LeasePolicyState == SQLiteStoreLeasePolicyMismatch {
		return *result, newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationOpen,
			SQLiteStoreReasonLeaseMismatch,
			false,
			false,
			errors.New("sqlite store lease policy does not match the requested policy"),
		)
	}

	switch inspection.State {
	case SQLiteStoreStateMissing, SQLiteStoreStateEmpty:
		store, openErr := openSQLiteStoreForStartup(ctx, path, SQLiteStoreOpenRequest{ExpectedState: inspection.State}, request.Progress, api, options...)
		if openErr != nil {
			return recoverSQLiteStoreStartup(ctx, path, request, policy, options, api, result, openErr, allowRecovery)
		}
		result.Store = store
		return *result, nil
	case SQLiteStoreStateCurrent:
		// Continue to verification and exact open below.
	case SQLiteStoreStateUpgradeable:
		if policy == SQLiteMigrationRefuse {
			return *result, newSQLiteStoreMaintenanceError(
				SQLiteStoreOperationOpen,
				SQLiteStoreReasonMigrationAvailable,
				false,
				false,
				errors.New("sqlite store requires an explicit compatible migration"),
			)
		}
		operationID, operationErr := sqliteStartupMigrationOperationID(path, request.MigrationOperationID, inspection)
		if operationErr != nil {
			return *result, newSQLiteStoreMaintenanceError(
				SQLiteStoreOperationMigrate,
				SQLiteStoreReasonInvalidRequest,
				false,
				false,
				operationErr,
			)
		}
		emitSQLiteStartupProgress(request.Progress, SQLiteStartupProgress{Phase: SQLiteStartupMigrating})
		migration, migrationErr := api.Migrate(ctx, path, SQLiteStoreMigrationRequest{
			OperationID:    operationID,
			Mode:           SQLiteStoreMigrationApply,
			ExpectedSchema: inspection.Observed,
			Progress: func(progress SQLiteStoreMaintenanceProgress) {
				emitSQLiteStartupProgress(request.Progress, SQLiteStartupProgress{Phase: SQLiteStartupMigrating, Maintenance: &progress})
			},
		}, options...)
		result.Migration = &migration
		if migrationErr != nil {
			return recoverSQLiteStoreStartup(ctx, path, request, policy, options, api, result, migrationErr, allowRecovery)
		}
	default:
		reason := inspection.Reason
		if reason == "" {
			reason = sqliteStoreReasonForState(inspection.State)
		}
		return *result, newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationOpen,
			reason,
			inspection.Retryable,
			inspection.SafeToRetry,
			fmt.Errorf("sqlite store state %q is not openable", inspection.State),
		)
	}

	verification, verifyErr := verifySQLiteStoreForStartup(ctx, path, request.Progress, api, options...)
	if verifyErr != nil {
		return recoverSQLiteStoreStartup(ctx, path, request, policy, options, api, result, verifyErr, allowRecovery)
	}
	result.Verification = &verification
	if verificationErr := validateSQLiteStoreVerificationForStartup(verification); verificationErr != nil {
		return recoverSQLiteStoreStartup(ctx, path, request, policy, options, api, result, verificationErr, allowRecovery)
	}
	store, openErr := openSQLiteStoreForStartup(ctx, path, SQLiteStoreOpenRequest{
		ExpectedState:  verification.Inspection.State,
		ExpectedSchema: verification.Inspection.Observed,
	}, request.Progress, api, options...)
	if openErr != nil {
		return recoverSQLiteStoreStartup(ctx, path, request, policy, options, api, result, openErr, allowRecovery)
	}
	result.Store = store
	return *result, nil
}

func recoverSQLiteStoreStartup(
	ctx context.Context,
	path string,
	request SQLiteStartupRequest,
	policy SQLiteMigrationPolicy,
	options []SQLiteStoreOption,
	api sqliteStoreStartupAPI,
	result *SQLiteStartupResult,
	cause error,
	allowRecovery bool,
) (SQLiteStartupResult, error) {
	if !allowRecovery || !sqliteStartupErrorAllowsReinspection(cause) {
		return *result, cause
	}
	inspection, err := inspectSQLiteStoreForStartup(ctx, path, request.Progress, api, options...)
	if err != nil {
		return *result, err
	}
	result.Inspection = &inspection
	if inspection.State != SQLiteStoreStateCurrent {
		if inspection.State == SQLiteStoreStateUpgradeable && result.Migration != nil {
			return *result, cause
		}
		reason := inspection.Reason
		if reason == "" {
			reason = sqliteStoreReasonForState(inspection.State)
		}
		return *result, newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationOpen,
			reason,
			inspection.Retryable,
			inspection.SafeToRetry,
			fmt.Errorf("sqlite store state %q is not a safe startup recovery target", inspection.State),
		)
	}
	return startSQLiteStoreFromInspection(ctx, path, request, policy, options, api, inspection, result, false)
}

func inspectSQLiteStoreForStartup(ctx context.Context, path string, progress func(SQLiteStartupProgress), api sqliteStoreStartupAPI, options ...SQLiteStoreOption) (SQLiteStoreInspection, error) {
	emitSQLiteStartupProgress(progress, SQLiteStartupProgress{Phase: SQLiteStartupInspecting})
	return api.Inspect(ctx, path, options...)
}

func verifySQLiteStoreForStartup(ctx context.Context, path string, progress func(SQLiteStartupProgress), api sqliteStoreStartupAPI, options ...SQLiteStoreOption) (SQLiteStoreVerification, error) {
	emitSQLiteStartupProgress(progress, SQLiteStartupProgress{Phase: SQLiteStartupVerifying})
	return api.Verify(ctx, path, options...)
}

func openSQLiteStoreForStartup(ctx context.Context, path string, request SQLiteStoreOpenRequest, progress func(SQLiteStartupProgress), api sqliteStoreStartupAPI, options ...SQLiteStoreOption) (*Store, error) {
	emitSQLiteStartupProgress(progress, SQLiteStartupProgress{Phase: SQLiteStartupOpening})
	return api.Open(ctx, path, request, options...)
}

func validateSQLiteStoreVerificationForStartup(verification SQLiteStoreVerification) error {
	inspection := verification.Inspection
	if inspection.LeasePolicyState == SQLiteStoreLeasePolicyMismatch {
		return newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationVerify,
			SQLiteStoreReasonLeaseMismatch,
			false,
			false,
			errors.New("sqlite store verification lease policy does not match the requested policy"),
		)
	}
	if inspection.State != SQLiteStoreStateCurrent {
		reason := inspection.Reason
		if reason == "" {
			reason = sqliteStoreReasonForState(inspection.State)
		}
		return newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationVerify,
			reason,
			inspection.Retryable,
			inspection.SafeToRetry,
			fmt.Errorf("sqlite store verification state %q is not current", inspection.State),
		)
	}
	if len(verification.Checks) == 0 {
		return newSQLiteStoreMaintenanceError(
			SQLiteStoreOperationVerify,
			SQLiteStoreReasonContract,
			false,
			false,
			errors.New("sqlite store verification returned no checks"),
		)
	}
	for _, check := range verification.Checks {
		if strings.TrimSpace(check.Code) == "" || !check.Passed {
			return newSQLiteStoreMaintenanceError(
				SQLiteStoreOperationVerify,
				SQLiteStoreReasonContract,
				false,
				false,
				errors.New("sqlite store verification check failed"),
			)
		}
	}
	return nil
}

func emitSQLiteStartupProgress(progress func(SQLiteStartupProgress), update SQLiteStartupProgress) {
	if progress != nil {
		progress(update)
	}
}

func sqliteStartupErrorAllowsReinspection(err error) bool {
	var maintenance *SQLiteStoreMaintenanceError
	if !errors.As(err, &maintenance) || !maintenance.SafeToRetry {
		return false
	}
	return maintenance.Reason == SQLiteStoreReasonBusy || maintenance.Reason == SQLiteStoreReasonInspectionStale
}

func sqliteStartupMigrationOperationID(path, requested string, inspection SQLiteStoreInspection) (string, error) {
	if requested = strings.TrimSpace(requested); requested != "" {
		return requested, nil
	}
	canonical, err := sqlite.CanonicalDatabasePath(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		canonical,
		inspection.Observed.Version,
		inspection.Observed.Fingerprint,
		inspection.Current.Version,
		inspection.Current.Fingerprint,
	}, "\x00")))
	return "sqlite-startup-" + hex.EncodeToString(digest[:16]), nil
}

func sqliteStoreReasonForState(state SQLiteStoreState) SQLiteStoreReason {
	switch state {
	case SQLiteStoreStateUnsupportedOlder:
		return SQLiteStoreReasonUnsupported
	case SQLiteStoreStateFuture:
		return SQLiteStoreReasonNewerReader
	case SQLiteStoreStateDrifted:
		return SQLiteStoreReasonFingerprint
	case SQLiteStoreStateCorrupt:
		return SQLiteStoreReasonCorrupt
	case SQLiteStoreStateBusy:
		return SQLiteStoreReasonBusy
	case SQLiteStoreStatePermissionDenied:
		return SQLiteStoreReasonPermission
	default:
		return SQLiteStoreReasonIO
	}
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
	if err := mapped.Validate(); err != nil {
		return SQLiteStoreMigrationResult{}, newSQLiteStoreMaintenanceError(SQLiteStoreOperationMigrate, SQLiteStoreReasonContract, false, false, err)
	}
	return mapped, nil
}

func mapSQLiteStoreInspection(inspection sqlite.MaintenanceInspection) SQLiteStoreInspection {
	kind := SQLiteStoreKind(inspection.Kind)
	if kind == "" {
		kind = SQLiteStoreKindUnknown
	}
	mapped := SQLiteStoreInspection{
		Kind: kind, State: SQLiteStoreState(inspection.State), Exists: inspection.Exists, Empty: inspection.Empty,
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
