package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

type MaintenanceState string

const (
	MaintenanceStateMissing          MaintenanceState = "missing"
	MaintenanceStateEmpty            MaintenanceState = "empty"
	MaintenanceStateCurrent          MaintenanceState = "current"
	MaintenanceStateUpgradeable      MaintenanceState = "upgradeable"
	MaintenanceStateUnsupportedOlder MaintenanceState = "unsupported_older"
	MaintenanceStateFuture           MaintenanceState = "future"
	MaintenanceStateDrifted          MaintenanceState = "drifted"
	MaintenanceStateCorrupt          MaintenanceState = "corrupt"
	MaintenanceStateBusy             MaintenanceState = "busy"
	MaintenanceStatePermissionDenied MaintenanceState = "permission_denied"
	MaintenanceStateIOError          MaintenanceState = "io_error"
)

type MaintenanceStoreKind string

const (
	MaintenanceStoreKindUnknown MaintenanceStoreKind = "unknown"
	MaintenanceStoreKindFloret  MaintenanceStoreKind = "floret"
)

type MaintenanceInspection struct {
	Kind                 MaintenanceStoreKind
	State                MaintenanceState
	Exists               bool
	Empty                bool
	Observed             storage.StoreSchemaIdentity
	Current              storage.StoreSchemaIdentity
	Migratable           []storage.StoreSchemaMigrationSource
	PersistedLeasePolicy *sessiontree.LeasePolicy
	RequestedLeasePolicy sessiontree.LeasePolicy
	LeasePolicyMatches   bool
	AutomaticMigration   bool
	RequiresExclusive    bool
	Retryable            bool
	SafeToRetry          bool
	Reason               string
	SafeDetail           string
}

type MaintenanceErrorReason string

const (
	MaintenanceErrorInvalidRequest MaintenanceErrorReason = "invalid_request"
	MaintenanceErrorCancelled      MaintenanceErrorReason = "cancelled"
	MaintenanceErrorBusy           MaintenanceErrorReason = "busy"
	MaintenanceErrorPermission     MaintenanceErrorReason = "permission_denied"
	MaintenanceErrorIO             MaintenanceErrorReason = "io_error"
	MaintenanceErrorCorrupt        MaintenanceErrorReason = "corrupt"
	MaintenanceErrorStale          MaintenanceErrorReason = "inspection_stale"
)

type MaintenanceError struct {
	Reason      MaintenanceErrorReason
	Retryable   bool
	SafeToRetry bool
	Err         error
}

func (e *MaintenanceError) Error() string {
	if e == nil {
		return "sqlite store maintenance failed"
	}
	return fmt.Sprintf("sqlite store maintenance failed: %s", e.Reason)
}

func (e *MaintenanceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type VerificationCheck struct {
	Code       string
	Passed     bool
	SafeDetail string
}

type MaintenanceVerification struct {
	Inspection MaintenanceInspection
	Checks     []VerificationCheck
}

type MigrationMode string

const (
	MigrationModePlan  MigrationMode = "plan"
	MigrationModeApply MigrationMode = "apply"
)

type MaintenancePhase string

const (
	MaintenancePhasePreflight MaintenancePhase = "preflight"
	MaintenancePhaseWaiting   MaintenancePhase = "waiting_for_exclusive_access"
	MaintenancePhaseMigrating MaintenancePhase = "migrating"
	MaintenancePhaseVerifying MaintenancePhase = "verifying"
)

type MaintenanceStatus string

const (
	MaintenanceStatusRunning   MaintenanceStatus = "running"
	MaintenanceStatusReady     MaintenanceStatus = "ready"
	MaintenanceStatusFailed    MaintenanceStatus = "failed"
	MaintenanceStatusCancelled MaintenanceStatus = "cancelled"
)

type MaintenanceProgress struct {
	Sequence     uint64
	Phase        MaintenancePhase
	Status       MaintenanceStatus
	Step         int
	Total        int
	SafeToCancel bool
	Committed    bool
	RolledBack   bool
	Retryable    bool
	SafeToRetry  bool
	Reason       string
}

type MigrationStep struct {
	From storage.StoreSchemaIdentity
	To   storage.StoreSchemaIdentity
	Code string
}

type MigrationRequest struct {
	Mode           MigrationMode
	ExpectedSchema storage.StoreSchemaIdentity
	LeasePolicy    sessiontree.LeasePolicy
	Progress       func(MaintenanceProgress)
}

type MigrationResult struct {
	Mode        MigrationMode
	Before      MaintenanceInspection
	After       MaintenanceInspection
	Steps       []MigrationStep
	Status      MaintenanceStatus
	Changed     bool
	Committed   bool
	RolledBack  bool
	Retryable   bool
	SafeToRetry bool
	Reason      string
}

type maintenanceTransactionResult struct {
	Entered    bool
	Committed  bool
	RolledBack bool
}

func Inspect(ctx context.Context, path string, requested sessiontree.LeasePolicy) (MaintenanceInspection, error) {
	if ctx == nil {
		return MaintenanceInspection{}, maintenanceFailure(MaintenanceErrorInvalidRequest, false, false, errors.New("sqlite store inspection context is required"))
	}
	if strings.TrimSpace(path) == "" || path == ":memory:" {
		return MaintenanceInspection{}, maintenanceFailure(MaintenanceErrorInvalidRequest, false, false, errors.New("sqlite store inspection requires a file path"))
	}
	if err := requested.Validate(); err != nil {
		return MaintenanceInspection{}, maintenanceFailure(MaintenanceErrorInvalidRequest, false, false, fmt.Errorf("validate sqlite store inspection lease policy: %w", err))
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return MaintenanceInspection{
			State: MaintenanceStateMissing, Current: currentSchemaIdentity(), Migratable: migratableSchemaSources(),
			RequestedLeasePolicy: requested, Reason: "store_missing", SafeDetail: "store file does not exist",
		}, nil
	}
	if err != nil {
		return failedInspection(requested, err), nil
	}
	if !info.Mode().IsRegular() {
		return MaintenanceInspection{}, maintenanceFailure(MaintenanceErrorInvalidRequest, false, false, errors.New("sqlite store path is not a regular file"))
	}
	canonical, err := CanonicalDatabasePath(path)
	if err != nil {
		return failedInspection(requested, err), nil
	}
	db, err := openMaintenanceDB(canonical, "ro")
	if err != nil {
		return failedInspection(requested, err), nil
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return MaintenanceInspection{}, err
		}
		return failedInspection(requested, err), nil
	}
	inspection, err := inspectRunner(ctx, db, requested)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return MaintenanceInspection{}, err
		}
		return failedInspection(requested, err), nil
	}
	return inspection, nil
}

func Verify(ctx context.Context, path string, requested sessiontree.LeasePolicy) (MaintenanceVerification, error) {
	inspection, err := Inspect(ctx, path, requested)
	if err != nil {
		return MaintenanceVerification{}, err
	}
	report := MaintenanceVerification{Inspection: inspection}
	if inspection.State != MaintenanceStateCurrent && inspection.State != MaintenanceStateUpgradeable {
		report.Checks = append(report.Checks, VerificationCheck{
			Code: "schema_supported", Passed: false, SafeDetail: "store schema cannot be verified by this reader",
		})
		return report, nil
	}
	report.Checks = append(report.Checks, VerificationCheck{
		Code: "schema_contract", Passed: true, SafeDetail: "store schema matches its declared contract",
	})
	if inspection.State != MaintenanceStateCurrent {
		return report, nil
	}
	canonical, err := CanonicalDatabasePath(path)
	if err != nil {
		return MaintenanceVerification{}, err
	}
	db, err := openMaintenanceDB(canonical, "ro")
	if err != nil {
		return MaintenanceVerification{}, err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
		return MaintenanceVerification{}, err
	}
	threads, err := loadThreadAuthorityGraph(ctx, db)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return MaintenanceVerification{}, err
		}
		report.Checks = append(report.Checks, VerificationCheck{
			Code: "thread_authority", Passed: false, SafeDetail: "thread authority graph is invalid",
		})
		return report, nil
	}
	if err := sessiontree.ValidateThreadAuthorityGraph(threads); err != nil {
		report.Checks = append(report.Checks, VerificationCheck{
			Code: "thread_authority", Passed: false, SafeDetail: "thread authority graph is invalid",
		})
		return report, nil
	}
	report.Checks = append(report.Checks, VerificationCheck{
		Code: "thread_authority", Passed: true, SafeDetail: "thread authority graph is valid",
	})
	return report, nil
}

func Migrate(ctx context.Context, path string, request MigrationRequest) (result MigrationResult, resultErr error) {
	if ctx == nil {
		return MigrationResult{}, maintenanceFailure(MaintenanceErrorInvalidRequest, false, false, errors.New("sqlite store migration context is required"))
	}
	if request.Mode != MigrationModePlan && request.Mode != MigrationModeApply {
		return MigrationResult{}, maintenanceFailure(MaintenanceErrorInvalidRequest, false, false, fmt.Errorf("unsupported sqlite store migration mode %q", request.Mode))
	}
	if err := request.LeasePolicy.Validate(); err != nil {
		return MigrationResult{}, maintenanceFailure(MaintenanceErrorInvalidRequest, false, false, fmt.Errorf("validate sqlite store migration lease policy: %w", err))
	}
	sequence := uint64(0)
	emit := func(progress MaintenanceProgress) {
		sequence++
		progress.Sequence = sequence
		if request.Progress != nil {
			request.Progress(progress)
		}
	}
	emit(MaintenanceProgress{Phase: MaintenancePhasePreflight, Status: MaintenanceStatusRunning, Step: 1, Total: 4, SafeToCancel: true})
	before, err := Inspect(ctx, path, request.LeasePolicy)
	if err != nil {
		return failedMigrationResult(request.Mode, result, false, err, emit)
	}
	result = MigrationResult{Mode: request.Mode, Before: before, After: before, Steps: migrationSteps(before.Observed)}
	if !schemaIdentityMatchesExpected(before.Observed, request.ExpectedSchema) {
		err := maintenanceFailure(MaintenanceErrorStale, true, true, errors.New("sqlite store inspection is stale"))
		result.Reason = "inspection_stale"
		result.Retryable = true
		result.SafeToRetry = true
		return failedMigrationResult(request.Mode, result, false, err, emit)
	}
	if before.State == MaintenanceStateCurrent {
		result.Status = MaintenanceStatusReady
		result.SafeToRetry = true
		emit(MaintenanceProgress{Phase: MaintenancePhaseVerifying, Status: MaintenanceStatusReady, Step: 4, Total: 4, SafeToCancel: false, SafeToRetry: true})
		return result, nil
	}
	if before.State != MaintenanceStateUpgradeable {
		err := maintenanceErrorForInspection(before)
		result.Reason = before.Reason
		return failedMigrationResult(request.Mode, result, false, err, emit)
	}
	if request.Mode == MigrationModePlan {
		result.Status = MaintenanceStatusReady
		result.SafeToRetry = true
		emit(MaintenanceProgress{Phase: MaintenancePhaseVerifying, Status: MaintenanceStatusReady, Step: 4, Total: 4, SafeToCancel: false, SafeToRetry: true})
		return result, nil
	}

	emit(MaintenanceProgress{Phase: MaintenancePhaseWaiting, Status: MaintenanceStatusRunning, Step: 2, Total: 4, SafeToCancel: true})
	canonical, err := CanonicalDatabasePath(path)
	if err != nil {
		return failedMigrationResult(request.Mode, result, false, err, emit)
	}
	admission, err := NewWriterAdmission(canonical)
	if err != nil {
		return failedMigrationResult(request.Mode, result, false, err, emit)
	}
	defer admission.Close()
	db, err := openMaintenanceDB(canonical, "rw")
	if err != nil {
		return failedMigrationResult(request.Mode, result, false, err, emit)
	}
	defer db.Close()
	store := &Store{db: db, path: canonical, writerAdmission: admission, leasePolicy: request.LeasePolicy}
	emit(MaintenanceProgress{Phase: MaintenancePhaseMigrating, Status: MaintenanceStatusRunning, Step: 3, Total: 4, SafeToCancel: true})
	transaction, err := store.withMaintenanceImmediate(ctx, func(tx sqlRunner) error {
		observed, err := readSchemaIdentity(ctx, tx)
		if err != nil {
			return err
		}
		if observed != before.Observed {
			return maintenanceFailure(MaintenanceErrorStale, true, true, errors.New("sqlite store inspection is stale"))
		}
		if err := ensureSchema(ctx, tx, request.LeasePolicy); err != nil {
			return err
		}
		emit(MaintenanceProgress{Phase: MaintenancePhaseVerifying, Status: MaintenanceStatusRunning, Step: 4, Total: 4, SafeToCancel: false})
		threads, err := loadThreadAuthorityGraph(ctx, tx)
		if err != nil {
			return err
		}
		return sessiontree.ValidateThreadAuthorityGraph(threads)
	})
	if err != nil {
		result.Committed = transaction.Committed
		var maintenanceErr *MaintenanceError
		if errors.As(err, &maintenanceErr) && maintenanceErr.Reason == MaintenanceErrorStale {
			result.Reason = "inspection_stale"
			result.Retryable = true
			result.SafeToRetry = true
		}
		return failedMigrationResult(request.Mode, result, transaction.RolledBack, err, emit)
	}
	result.Committed = transaction.Committed
	after, err := Inspect(ctx, path, request.LeasePolicy)
	if err != nil {
		result.Changed = true
		return failedMigrationResult(request.Mode, result, false, fmt.Errorf("inspect migrated sqlite store: %w", err), emit)
	}
	result.After = after
	result.Changed = after.Observed != before.Observed
	if after.State != MaintenanceStateCurrent || !after.LeasePolicyMatches {
		return failedMigrationResult(request.Mode, result, false, maintenanceErrorForInspection(after), emit)
	}
	result.Status = MaintenanceStatusReady
	result.SafeToRetry = true
	emit(MaintenanceProgress{
		Phase: MaintenancePhaseVerifying, Status: MaintenanceStatusReady, Step: 4, Total: 4,
		Committed: result.Committed, SafeToRetry: true,
	})
	return result, nil
}

func (s *Store) withMaintenanceImmediate(ctx context.Context, fn func(sqlRunner) error) (out maintenanceTransactionResult, resultErr error) {
	releaseWriter, err := s.writerAdmission.Reserve(ctx)
	if err != nil {
		return out, err
	}
	defer releaseWriter()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return out, err
	}
	defer conn.Close()
	for _, pragma := range []string{"PRAGMA foreign_keys = ON", "PRAGMA busy_timeout = 0"} {
		if _, err := conn.ExecContext(ctx, pragma); err != nil {
			return out, err
		}
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		return out, err
	}
	out.Entered = true
	if err := fn(conn); err != nil {
		if _, rollbackErr := conn.ExecContext(context.Background(), "ROLLBACK"); rollbackErr != nil {
			return out, errors.Join(err, fmt.Errorf("rollback sqlite store migration: %w", rollbackErr))
		}
		out.RolledBack = true
		return out, err
	}
	if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
		if _, rollbackErr := conn.ExecContext(context.Background(), "ROLLBACK"); rollbackErr == nil {
			out.RolledBack = true
			return out, err
		} else {
			return out, errors.Join(err, fmt.Errorf("rollback sqlite store migration after commit failure: %w", rollbackErr))
		}
	}
	out.Committed = true
	return out, nil
}

func failedMigrationResult(mode MigrationMode, result MigrationResult, rolledBack bool, err error, emit func(MaintenanceProgress)) (MigrationResult, error) {
	result.Mode = mode
	result.RolledBack = rolledBack
	reason := classifyMaintenanceError(err)
	if reason == MaintenanceErrorCancelled {
		result.Status = MaintenanceStatusCancelled
		result.Reason = "cancelled"
		result.SafeToRetry = true
	} else {
		result.Status = MaintenanceStatusFailed
		if result.Reason == "" {
			result.Reason = string(reason)
		}
		var maintenanceErr *MaintenanceError
		if errors.As(err, &maintenanceErr) {
			result.Retryable = maintenanceErr.Retryable
			result.SafeToRetry = maintenanceErr.SafeToRetry
		} else if reason == MaintenanceErrorBusy {
			result.Retryable = true
			result.SafeToRetry = true
		}
	}
	emit(MaintenanceProgress{
		Phase: MaintenancePhaseVerifying, Status: result.Status, SafeToCancel: false,
		Committed: result.Committed, RolledBack: result.RolledBack,
		Retryable: result.Retryable, SafeToRetry: result.SafeToRetry, Reason: result.Reason,
	})
	return result, err
}

func inspectRunner(ctx context.Context, q sqlRunner, requested sessiontree.LeasePolicy) (MaintenanceInspection, error) {
	inspection := MaintenanceInspection{
		Exists: true, Current: currentSchemaIdentity(), Migratable: migratableSchemaSources(), RequestedLeasePolicy: requested,
	}
	hasSchema, err := hasUserSchema(ctx, q)
	if err != nil {
		return MaintenanceInspection{}, err
	}
	if !hasSchema {
		inspection.State = MaintenanceStateEmpty
		inspection.Empty = true
		inspection.Reason = "store_empty"
		inspection.SafeDetail = "store file is empty"
		return inspection, nil
	}
	hasMeta, err := schemaTableExists(ctx, q, "schema_meta")
	if err != nil {
		return MaintenanceInspection{}, err
	}
	if !hasMeta {
		inspection.State = MaintenanceStateUnsupportedOlder
		inspection.Reason = "unrecognized_store"
		inspection.SafeDetail = "store metadata is not recognized"
		return inspection, nil
	}
	inspection.Kind = MaintenanceStoreKindFloret
	observed, err := readSchemaIdentity(ctx, q)
	if err != nil {
		inspection.State = MaintenanceStateCorrupt
		inspection.Reason = "schema_metadata_invalid"
		inspection.SafeDetail = "store schema metadata is incomplete"
		return inspection, nil
	}
	inspection.Observed = observed
	versionNumber, versionErr := strconv.Atoi(observed.Version)
	currentNumber, _ := strconv.Atoi(schemaVersion)
	if versionErr == nil && versionNumber > currentNumber {
		inspection.State = MaintenanceStateFuture
		inspection.Reason = "requires_newer_reader"
		inspection.SafeDetail = "store requires a newer Floret reader"
		return inspection, nil
	}
	expectedFingerprint, known := schemaFingerprint(observed.Version)
	if !known {
		inspection.State = MaintenanceStateUnsupportedOlder
		inspection.Reason = "unsupported_older_schema"
		inspection.SafeDetail = "store schema is older than the supported migration inputs"
		return inspection, nil
	}
	if observed.Fingerprint != expectedFingerprint {
		inspection.State = MaintenanceStateDrifted
		inspection.Reason = "schema_fingerprint_mismatch"
		inspection.SafeDetail = "store schema identity does not match its declared version"
		return inspection, nil
	}
	var contractErr error
	if _, legacy := legacyPublishedSchemaSQL(observed.Version); legacy {
		contractErr = verifyLegacySchemaContract(ctx, q, observed.Version)
	} else {
		contractErr = verifySchemaVersion(ctx, q, observed.Version)
	}
	if contractErr != nil {
		inspection.State = MaintenanceStateDrifted
		inspection.Reason = "schema_contract_mismatch"
		inspection.SafeDetail = "store schema does not match its declared contract"
		return inspection, nil
	}
	if observed.Version != schemaVersion {
		inspection.State = MaintenanceStateUpgradeable
		inspection.AutomaticMigration = true
		inspection.RequiresExclusive = true
		inspection.Reason = "migration_available"
		inspection.SafeDetail = "store can be migrated by this Floret version"
		return inspection, nil
	}
	persisted, err := readLeasePolicy(ctx, q)
	if err != nil {
		inspection.State = MaintenanceStateCorrupt
		inspection.Reason = "lease_policy_invalid"
		inspection.SafeDetail = "store lease policy is invalid"
		return inspection, nil
	}
	inspection.PersistedLeasePolicy = &persisted
	inspection.LeasePolicyMatches = persisted == requested
	if !inspection.LeasePolicyMatches {
		inspection.State = MaintenanceStateCurrent
		inspection.Reason = "lease_policy_mismatch"
		inspection.SafeDetail = "requested lease policy does not match the store policy"
		return inspection, nil
	}
	inspection.State = MaintenanceStateCurrent
	inspection.Reason = "store_current"
	inspection.SafeDetail = "store schema is current"
	return inspection, nil
}

func readSchemaIdentity(ctx context.Context, q sqlRunner) (storage.StoreSchemaIdentity, error) {
	version, err := metaValue(ctx, q, "schema_version")
	if err != nil {
		return storage.StoreSchemaIdentity{}, err
	}
	fingerprint, err := metaValue(ctx, q, "schema_fingerprint")
	if err != nil {
		if errors.Is(err, storage.ErrMetadataNotFound) && legacySchemaVersionWithoutFingerprint(version) {
			return storage.StoreSchemaIdentity{Version: version}, nil
		}
		return storage.StoreSchemaIdentity{}, err
	}
	return storage.StoreSchemaIdentity{Version: version, Fingerprint: fingerprint}, nil
}

func schemaFingerprint(version string) (string, bool) {
	switch version {
	case "3", "4", "5", "6", "7", "8", "9", "10":
		return "", true
	case schemaVersion11:
		return schemaFingerprintVersion11, true
	case schemaVersion12:
		return schemaFingerprintVersion12, true
	case schemaVersion13:
		return schemaFingerprintVersion13, true
	case schemaVersion14:
		return schemaFingerprintVersion14, true
	case schemaVersion15:
		return schemaFingerprintVersion15, true
	case schemaVersion:
		return schemaFingerprintVersion16, true
	default:
		return "", false
	}
}

func currentSchemaIdentity() storage.StoreSchemaIdentity {
	return storage.StoreSchemaIdentity{Version: schemaVersion, Fingerprint: schemaFingerprintVersion16}
}

func migratableSchemaSources() []storage.StoreSchemaMigrationSource {
	err := unsupportedSchemaError("", "")
	var unsupported *storage.UnsupportedStoreSchemaError
	if errors.As(err, &unsupported) {
		return append([]storage.StoreSchemaMigrationSource(nil), unsupported.Migratable...)
	}
	return nil
}

func migrationSteps(observed storage.StoreSchemaIdentity) []MigrationStep {
	version, err := strconv.Atoi(observed.Version)
	if err != nil || version < 3 || version >= 16 {
		return nil
	}
	steps := make([]MigrationStep, 0, 16-version)
	from := observed
	for next := version + 1; next <= 16; next++ {
		to, ok := schemaIdentityForVersion(strconv.Itoa(next))
		if !ok {
			return nil
		}
		steps = append(steps, MigrationStep{
			From: from,
			To:   to,
			Code: fmt.Sprintf("migrate_v%d_to_v%d", next-1, next),
		})
		from = to
	}
	return steps
}

func schemaIdentityForVersion(version string) (storage.StoreSchemaIdentity, bool) {
	fingerprint, ok := schemaFingerprint(version)
	if !ok {
		return storage.StoreSchemaIdentity{}, false
	}
	return storage.StoreSchemaIdentity{Version: version, Fingerprint: fingerprint}, true
}

func schemaIdentityMatchesExpected(observed, expected storage.StoreSchemaIdentity) bool {
	if expected.Version == "" && expected.Fingerprint == "" {
		return true
	}
	return observed == expected
}

func openMaintenanceDB(path, mode string) (*sql.DB, error) {
	u := &url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Set("mode", mode)
	if mode == "ro" {
		// Immutable read-only connections avoid creating WAL/SHM sidecars. The
		// maintenance reader never uses this connection for a live migration.
		query.Set("immutable", "1")
	}
	u.RawQuery = query.Encode()
	db, err := sql.Open(driverName, u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func failedInspection(requested sessiontree.LeasePolicy, err error) MaintenanceInspection {
	inspection := MaintenanceInspection{
		Current: currentSchemaIdentity(), Migratable: migratableSchemaSources(), RequestedLeasePolicy: requested,
	}
	switch classifyMaintenanceError(err) {
	case MaintenanceErrorBusy:
		inspection.State = MaintenanceStateBusy
		inspection.Reason = string(MaintenanceErrorBusy)
		inspection.SafeDetail = "store is currently busy"
		inspection.Retryable = true
		inspection.SafeToRetry = true
	case MaintenanceErrorPermission:
		inspection.State = MaintenanceStatePermissionDenied
		inspection.Reason = string(MaintenanceErrorPermission)
		inspection.SafeDetail = "store cannot be read with current permissions"
	case MaintenanceErrorCorrupt:
		inspection.State = MaintenanceStateCorrupt
		inspection.Reason = string(MaintenanceErrorCorrupt)
		inspection.SafeDetail = "store data is not readable as a valid SQLite database"
	default:
		inspection.State = MaintenanceStateIOError
		inspection.Reason = string(MaintenanceErrorIO)
		inspection.SafeDetail = "store could not be inspected"
	}
	return inspection
}

func maintenanceErrorForInspection(inspection MaintenanceInspection) error {
	reason := MaintenanceErrorReason(inspection.Reason)
	switch inspection.State {
	case MaintenanceStateBusy:
		reason = MaintenanceErrorBusy
	case MaintenanceStatePermissionDenied:
		reason = MaintenanceErrorPermission
	case MaintenanceStateIOError:
		reason = MaintenanceErrorIO
	case MaintenanceStateCorrupt, MaintenanceStateDrifted:
		reason = MaintenanceErrorCorrupt
	}
	if reason == "" {
		reason = MaintenanceErrorInvalidRequest
	}
	return maintenanceFailure(reason, inspection.Retryable, inspection.SafeToRetry, fmt.Errorf("sqlite store state %q is not migratable", inspection.State))
}

func maintenanceFailure(reason MaintenanceErrorReason, retryable, safeToRetry bool, err error) error {
	return &MaintenanceError{Reason: reason, Retryable: retryable, SafeToRetry: safeToRetry, Err: err}
}

func classifyMaintenanceError(err error) MaintenanceErrorReason {
	var maintenanceErr *MaintenanceError
	if errors.As(err, &maintenanceErr) {
		return maintenanceErr.Reason
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return MaintenanceErrorCancelled
	}
	if errors.Is(err, os.ErrPermission) {
		return MaintenanceErrorPermission
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 0xff {
		case 3, 8:
			return MaintenanceErrorPermission
		case 5, 6:
			return MaintenanceErrorBusy
		case 11, 26:
			return MaintenanceErrorCorrupt
		}
	}
	return MaintenanceErrorIO
}
