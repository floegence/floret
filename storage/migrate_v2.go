package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/floegence/floret/v3/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v3/internal/storage"
	legacy "github.com/floegence/floret/v3/internal/storage/sqlite"
	"github.com/floegence/floret/v3/internal/storagecodec"
	"github.com/floegence/floret/v3/storage/spi"
)

const (
	migrationBackendNamespace  = "floret.domain"
	migrationSystemNamespace   = "floret.system"
	migrationSchemaVersion     = "3"
	migrationSchemaFingerprint = "sha256:53e8fd256bfa05b6f31f73b8230455fd28d6bb4f3be1fce7d94a9af9b5838d28"
	migrationPlanVersion       = 1
	legacyMigrationAlgorithm   = "floret-v2.2-to-v3/1"
	legacyMigrationProjection  = "floret-v3-domain/1"
	migrationAlgorithmVersion  = "floret-v2.2-to-v3/2"
	migrationProjectionVersion = "floret-v3-domain/2"
)

var (
	// ErrMigrationConflict reports that the source, immutable plan, or committed
	// target no longer describes the same migration operation.
	ErrMigrationConflict = errors.New("floret v2 to v3 migration conflict")
)

type migrationStage string

const (
	migrationStageTransactionStarted   migrationStage = "transaction_started"
	migrationStageSourceValidated      migrationStage = "source_validated"
	migrationStageSourceExported       migrationStage = "source_exported"
	migrationStageLegacyDropped        migrationStage = "legacy_dropped"
	migrationStageBackendSchemaCreated migrationStage = "backend_schema_created"
	migrationStageMetadataWritten      migrationStage = "metadata_written"
	migrationStageRecordsWritten       migrationStage = "records_written"
	migrationStageContentValidated     migrationStage = "content_validated"
	migrationStageBeforeCommit         migrationStage = "before_commit"
)

type migrationCheckpoint func(migrationStage) error

// MigrationSchemaError reports a database outside the exact supported v2.2
// source schema or exact v3 migration receipt.
type MigrationSchemaError struct {
	Version     string
	Fingerprint string
	Reason      string
}

// Error describes the rejected migration schema.
func (failure *MigrationSchemaError) Error() string {
	if failure == nil {
		return "invalid floret migration schema"
	}
	return fmt.Sprintf("invalid floret migration schema: version %q fingerprint %q: %s", failure.Version, failure.Fingerprint, failure.Reason)
}

// UnsupportedLegacyContentError reports valid legacy content for which the
// frozen conversion table has no unique v3 representation.
type UnsupportedLegacyContentError struct {
	Kind   string `json:"kind"`
	Count  int    `json:"count"`
	Reason string `json:"reason"`
}

func (failure *UnsupportedLegacyContentError) Error() string {
	if failure == nil {
		return "unsupported legacy Floret content"
	}
	return fmt.Sprintf("unsupported legacy Floret content %q (count %d): %s", failure.Kind, failure.Count, failure.Reason)
}

// V2MigrationPreflightRequest identifies a read-only representability check.
// CoordinatorCommitment is opaque to Floret and binds a product-level joint
// upgrade journal to the resulting immutable plan.
type V2MigrationPreflightRequest struct {
	Path                  string
	OperationID           string
	CoordinatorCommitment string
}

type v2MigrationPlanWire struct {
	Version               int    `json:"version"`
	AlgorithmVersion      string `json:"algorithm_version"`
	ProjectionVersion     string `json:"projection_version"`
	OperationID           string `json:"operation_id"`
	CoordinatorCommitment string `json:"coordinator_commitment"`
	SourceVersion         string `json:"source_version"`
	SourceFingerprint     string `json:"source_fingerprint"`
	SourceSemanticHash    string `json:"source_semantic_hash"`
	TargetSemanticHash    string `json:"target_semantic_hash"`
	Threads               int    `json:"threads"`
	Entries               int    `json:"entries"`
	PlanHash              string `json:"plan_hash"`
}

// V2MigrationPlan is a content-addressed, immutable migration plan. Its fields
// are intentionally private so callers cannot construct an unchecked plan.
// Strict JSON round-tripping is supported for offline coordination.
type V2MigrationPlan struct {
	wire v2MigrationPlanWire
}

func (plan V2MigrationPlan) OperationID() string           { return plan.wire.OperationID }
func (plan V2MigrationPlan) CoordinatorCommitment() string { return plan.wire.CoordinatorCommitment }
func (plan V2MigrationPlan) SourceSemanticHash() string    { return plan.wire.SourceSemanticHash }
func (plan V2MigrationPlan) TargetSemanticHash() string    { return plan.wire.TargetSemanticHash }
func (plan V2MigrationPlan) PlanHash() string              { return plan.wire.PlanHash }
func (plan V2MigrationPlan) ThreadCount() int              { return plan.wire.Threads }
func (plan V2MigrationPlan) EntryCount() int               { return plan.wire.Entries }
func (plan V2MigrationPlan) MarshalJSON() ([]byte, error) {
	if err := validateV2MigrationPlan(plan.wire); err != nil {
		return nil, err
	}
	return json.Marshal(plan.wire)
}
func (plan *V2MigrationPlan) UnmarshalJSON(data []byte) error {
	if plan == nil {
		return errors.New("decode v2 migration plan into nil receiver")
	}
	var wire v2MigrationPlanWire
	if err := decodeStrictMigrationJSON(data, &wire); err != nil {
		return err
	}
	if err := validateV2MigrationPlan(wire); err != nil {
		return err
	}
	plan.wire = wire
	return nil
}

type V2MigrationPreviewRequest struct {
	Path string
	Plan V2MigrationPlan
}

type V2MigrationPreview struct {
	PlanHash           string `json:"plan_hash"`
	SourceSemanticHash string `json:"source_semantic_hash"`
	TargetSemanticHash string `json:"target_semantic_hash"`
	Threads            int    `json:"threads"`
	Entries            int    `json:"entries"`
}

type V2MigrationApplyRequest struct {
	Path string
	Plan V2MigrationPlan
}

// V2MigrationReceipt proves the exact source, target, plan, and opaque
// coordinator commitment that were committed atomically by Floret.
type V2MigrationReceipt struct {
	OperationID           string `json:"operation_id"`
	Replayed              bool   `json:"replayed"`
	PlanHash              string `json:"plan_hash"`
	SourceSemanticHash    string `json:"source_semantic_hash"`
	TargetSemanticHash    string `json:"target_semantic_hash"`
	CoordinatorCommitment string `json:"coordinator_commitment"`
	Threads               int    `json:"threads"`
	Entries               int    `json:"entries"`
}

type migrationReceipt struct {
	Version               int    `json:"version"`
	AlgorithmVersion      string `json:"algorithm_version"`
	ProjectionVersion     string `json:"projection_version"`
	OperationID           string `json:"operation_id"`
	PlanHash              string `json:"plan_hash"`
	CoordinatorCommitment string `json:"coordinator_commitment"`
	SourceVersion         string `json:"source_version"`
	SourceFingerprint     string `json:"source_fingerprint"`
	Threads               int    `json:"threads"`
	Entries               int    `json:"entries"`
	SourceSemanticHash    string `json:"source_semantic_hash"`
	TargetSemanticHash    string `json:"target_semantic_hash"`
}

type migrationLogicalSchema struct {
	Version     string `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

var (
	migrationSessionKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))
	migrationPromptKey  = storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))
)

type analyzedV2Migration struct {
	exported              legacy.ExportedV16State
	sessionEnvelope       []byte
	legacySession         []byte
	legacySessionEnvelope []byte
	inventoryKey          []byte
	inventoryValue        []byte
	promptEnvelope        []byte
	forkRecords           []migratedForkRecord
	sourceHash            string
	legacySourceHash      string
	targetHash            string
	legacyTargetHash      string
}

func validateMigrationPath(ctx context.Context, value string) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("%w: migration context is required", spi.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path := strings.TrimSpace(value)
	if path == "" || path == ":memory:" || path != value {
		return "", fmt.Errorf("%w: migration requires a canonical SQLite file path", spi.ErrInvalidArgument)
	}
	return path, nil
}

func validateV2MigrationIdentity(ctx context.Context, pathValue, operationValue, commitmentValue string) (string, string, string, error) {
	path, err := validateMigrationPath(ctx, pathValue)
	if err != nil {
		return "", "", "", err
	}
	operationID, commitment := strings.TrimSpace(operationValue), strings.TrimSpace(commitmentValue)
	if operationID == "" || operationID != operationValue || commitment == "" || commitment != commitmentValue {
		return "", "", "", fmt.Errorf("%w: migration requires canonical operation and coordinator commitment identities", spi.ErrInvalidArgument)
	}
	return path, operationID, commitment, nil
}

func openMigrationConnection(ctx context.Context, path string, writable bool) (*sql.Conn, func(), error) {
	database, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		return nil, nil, err
	}
	database.SetMaxOpenConns(1)
	connection, err := database.Conn(ctx)
	if err != nil {
		database.Close()
		return nil, nil, err
	}
	closeConnection := func() { connection.Close(); database.Close() }
	if _, err := connection.ExecContext(ctx, `PRAGMA busy_timeout = 0`); err != nil {
		closeConnection()
		return nil, nil, err
	}
	transaction := "BEGIN"
	if writable {
		transaction = "BEGIN IMMEDIATE"
	}
	if _, err := connection.ExecContext(ctx, transaction); err != nil {
		closeConnection()
		return nil, nil, err
	}
	return connection, func() { _, _ = connection.ExecContext(context.Background(), `ROLLBACK`); closeConnection() }, nil
}

func analyzeV2Migration(ctx context.Context, runner legacy.V16Runner) (analyzedV2Migration, error) {
	if err := legacy.VerifyV16Runner(ctx, runner); err != nil {
		return analyzedV2Migration{}, migrationSchemaError(err)
	}
	exported, err := legacy.ExportV16State(ctx, runner)
	if err != nil {
		return analyzedV2Migration{}, fmt.Errorf("export Floret v2.2 state: %w", err)
	}
	sessionEnvelope, err := storagecodec.EncodeEnvelope("sessiontree", exported.Session)
	if err != nil {
		return analyzedV2Migration{}, err
	}
	legacySession, err := projectCurrentSessionToLegacyV3(exported.Session)
	if err != nil {
		return analyzedV2Migration{}, err
	}
	legacySessionEnvelope, err := storagecodec.EncodeEnvelope("sessiontree", legacySession)
	if err != nil {
		return analyzedV2Migration{}, err
	}
	memory, err := sessiontree.DecodeMemoryState(exported.Session, time.Now)
	if err != nil {
		return analyzedV2Migration{}, fmt.Errorf("decode migrated session-tree inventory source: %w", err)
	}
	inventoryKey, inventoryValue, err := sessiontree.EncodeBackendRootThreadInventoryRecord(memory)
	if err != nil {
		return analyzedV2Migration{}, fmt.Errorf("encode migrated root-thread inventory: %w", err)
	}
	promptEnvelope, err := storagecodec.EncodeEnvelope("prompt", exported.Prompt)
	if err != nil {
		return analyzedV2Migration{}, err
	}
	forkRecords, err := encodeMigratedForkOperations(exported.ForkOperations)
	if err != nil {
		return analyzedV2Migration{}, err
	}
	return analyzedV2Migration{
		exported: exported, sessionEnvelope: sessionEnvelope, legacySession: legacySession,
		legacySessionEnvelope: legacySessionEnvelope, inventoryKey: inventoryKey, inventoryValue: inventoryValue,
		promptEnvelope: promptEnvelope, forkRecords: forkRecords,
		sourceHash:       sourceSemanticHash(exported.Session, exported.Prompt, forkRecords),
		legacySourceHash: sourceSemanticHash(legacySession, exported.Prompt, forkRecords),
		targetHash:       targetSemanticHash(exported.Session, inventoryKey, inventoryValue, exported.Prompt, forkRecords),
		legacyTargetHash: legacyTargetSemanticHash(legacySession, exported.Prompt, forkRecords),
	}, nil
}

func projectCurrentSessionToLegacyV3(current []byte) ([]byte, error) {
	const currentPrefix = `{"version":4,`
	if !bytes.HasPrefix(current, []byte(currentPrefix)) {
		return nil, errors.New("canonical migrated session-tree state does not start at version 4")
	}
	legacy := make([]byte, 0, len(current))
	legacy = append(legacy, `{"version":3,`...)
	legacy = append(legacy, current[len(currentPrefix):]...)
	return legacy, nil
}

func sourceSemanticHash(session, prompt []byte, forkRecords []migratedForkRecord) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("floret-v2.2-source-semantic/1\x00"))
	writeMigrationSemanticContent(hash, session, prompt, forkRecords)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func writeMigrationSemanticContent(hash io.Writer, session, prompt []byte, forkRecords []migratedForkRecord) {
	_, _ = hash.Write(session)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(prompt)
	for _, record := range forkRecords {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(record.key)
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(record.value)
	}
}

func migrationPlanHash(wire v2MigrationPlanWire) string {
	wire.PlanHash = ""
	encoded, _ := json.Marshal(wire)
	sum := sha256.Sum256(append([]byte("floret-v3-migration-plan/1\x00"), encoded...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validateV2MigrationPlan(wire v2MigrationPlanWire) error {
	version, fingerprint := legacy.V16Identity()
	if wire.Version != migrationPlanVersion || !supportedMigrationContract(wire.AlgorithmVersion, wire.ProjectionVersion) ||
		wire.SourceVersion != version || wire.SourceFingerprint != fingerprint ||
		strings.TrimSpace(wire.OperationID) == "" || wire.OperationID != strings.TrimSpace(wire.OperationID) ||
		strings.TrimSpace(wire.CoordinatorCommitment) == "" || wire.CoordinatorCommitment != strings.TrimSpace(wire.CoordinatorCommitment) ||
		wire.Threads < 0 || wire.Entries < 0 || !validSemanticHash(wire.SourceSemanticHash) ||
		!validSemanticHash(wire.TargetSemanticHash) || !validSemanticHash(wire.PlanHash) || migrationPlanHash(wire) != wire.PlanHash {
		return fmt.Errorf("%w: invalid or tampered v2 migration plan", spi.ErrInvalidArgument)
	}
	return nil
}

func supportedMigrationContract(algorithm, projection string) bool {
	return algorithm == migrationAlgorithmVersion && projection == migrationProjectionVersion ||
		algorithm == legacyMigrationAlgorithm && projection == legacyMigrationProjection
}

func validSemanticHash(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func matchMigrationPlan(plan v2MigrationPlanWire, analyzed analyzedV2Migration) error {
	sourceHash := analyzed.sourceHash
	targetHash := analyzed.targetHash
	if plan.AlgorithmVersion == legacyMigrationAlgorithm && plan.ProjectionVersion == legacyMigrationProjection {
		sourceHash = analyzed.legacySourceHash
		targetHash = analyzed.legacyTargetHash
	}
	if plan.SourceSemanticHash != sourceHash || plan.TargetSemanticHash != targetHash ||
		plan.Threads != analyzed.exported.Threads || plan.Entries != analyzed.exported.Entries {
		return ErrMigrationConflict
	}
	return nil
}

func previewFromPlan(plan v2MigrationPlanWire) V2MigrationPreview {
	return V2MigrationPreview{PlanHash: plan.PlanHash, SourceSemanticHash: plan.SourceSemanticHash, TargetSemanticHash: plan.TargetSemanticHash, Threads: plan.Threads, Entries: plan.Entries}
}

func classifyLegacyMigrationError(err error) error {
	var unsupported *legacy.UnsupportedV16ContentError
	if errors.As(err, &unsupported) {
		return &UnsupportedLegacyContentError{Kind: unsupported.Kind, Count: unsupported.Count, Reason: unsupported.Reason}
	}
	return err
}

func decodeStrictMigrationJSON(data []byte, target any) error {
	if err := rejectDuplicateMigrationJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON data")
		}
		return err
	}
	return nil
}

func rejectDuplicateMigrationJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate JSON key %q", key)
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON data")
		}
		return err
	}
	return nil
}

// PreflightV2Migration verifies representability and returns an immutable,
// content-addressed plan without mutating the source.
func PreflightV2Migration(ctx context.Context, request V2MigrationPreflightRequest) (V2MigrationPlan, error) {
	path, operationID, commitment, err := validateV2MigrationIdentity(ctx, request.Path, request.OperationID, request.CoordinatorCommitment)
	if err != nil {
		return V2MigrationPlan{}, err
	}
	connection, closeConnection, err := openMigrationConnection(ctx, path, false)
	if err != nil {
		return V2MigrationPlan{}, err
	}
	defer closeConnection()
	analyzed, err := analyzeV2Migration(ctx, connection)
	if err != nil {
		return V2MigrationPlan{}, classifyLegacyMigrationError(err)
	}
	version, fingerprint := legacy.V16Identity()
	wire := v2MigrationPlanWire{
		Version: migrationPlanVersion, AlgorithmVersion: migrationAlgorithmVersion, ProjectionVersion: migrationProjectionVersion,
		OperationID: operationID, CoordinatorCommitment: commitment, SourceVersion: version, SourceFingerprint: fingerprint,
		SourceSemanticHash: analyzed.sourceHash, TargetSemanticHash: analyzed.targetHash,
		Threads: analyzed.exported.Threads, Entries: analyzed.exported.Entries,
	}
	wire.PlanHash = migrationPlanHash(wire)
	return V2MigrationPlan{wire: wire}, nil
}

// PreviewV2Migration verifies that the source still matches plan and returns
// the exact target semantic summary without writing.
func PreviewV2Migration(ctx context.Context, request V2MigrationPreviewRequest) (V2MigrationPreview, error) {
	path, err := validateMigrationPath(ctx, request.Path)
	if err != nil {
		return V2MigrationPreview{}, err
	}
	if err := validateV2MigrationPlan(request.Plan.wire); err != nil {
		return V2MigrationPreview{}, err
	}
	connection, closeConnection, err := openMigrationConnection(ctx, path, false)
	if err != nil {
		return V2MigrationPreview{}, err
	}
	defer closeConnection()
	analyzed, err := analyzeV2Migration(ctx, connection)
	if err != nil {
		return V2MigrationPreview{}, classifyLegacyMigrationError(err)
	}
	if err := matchMigrationPlan(request.Plan.wire, analyzed); err != nil {
		return V2MigrationPreview{}, err
	}
	return previewFromPlan(request.Plan.wire), nil
}

// ApplyV2Migration atomically applies an unchanged immutable plan. Exact
// replays return the original receipt; any source, plan, or target drift is a
// permanent migration conflict.
func ApplyV2Migration(ctx context.Context, request V2MigrationApplyRequest) (result V2MigrationReceipt, resultErr error) {
	return applyV2Migration(ctx, request, nil)
}

func applyV2Migration(ctx context.Context, request V2MigrationApplyRequest, checkpoint migrationCheckpoint) (result V2MigrationReceipt, resultErr error) {
	if ctx == nil {
		return result, fmt.Errorf("%w: migration context is required", spi.ErrInvalidArgument)
	}
	path, err := validateMigrationPath(ctx, request.Path)
	if err != nil {
		return result, err
	}
	if err := validateV2MigrationPlan(request.Plan.wire); err != nil {
		return result, err
	}
	database, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		return result, err
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	connection, err := database.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA busy_timeout = 0`); err != nil {
		return result, err
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return result, err
	}
	if _, err := connection.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return result, err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackErr := rollbackMigration(connection)
			if recovered := recover(); recovered != nil {
				panic(recovered)
			}
			if rollbackErr != nil {
				resultErr = errors.Join(resultErr, rollbackErr)
			}
		}
	}()
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageTransactionStarted); err != nil {
		return result, err
	}

	replayed, err := replayV3Migration(ctx, connection, request.Plan.wire)
	if err != nil {
		return result, err
	}
	if replayed != nil {
		if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageBeforeCommit); err != nil {
			return result, err
		}
		if _, err := connection.ExecContext(ctx, `COMMIT`); err != nil {
			return result, err
		}
		committed = true
		return resultFromReceipt(*replayed, true), nil
	}
	if err := legacy.VerifyV16Runner(ctx, connection); err != nil {
		return result, migrationSchemaError(err)
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageSourceValidated); err != nil {
		return result, err
	}
	analyzed, err := analyzeV2Migration(ctx, connection)
	if err != nil {
		return result, classifyLegacyMigrationError(err)
	}
	if err := matchMigrationPlan(request.Plan.wire, analyzed); err != nil {
		return result, err
	}
	plan := request.Plan.wire
	receipt := migrationReceipt{
		Version: migrationPlanVersion, AlgorithmVersion: plan.AlgorithmVersion, ProjectionVersion: plan.ProjectionVersion,
		OperationID: plan.OperationID, PlanHash: plan.PlanHash, CoordinatorCommitment: plan.CoordinatorCommitment,
		SourceVersion: plan.SourceVersion, SourceFingerprint: plan.SourceFingerprint, Threads: plan.Threads, Entries: plan.Entries,
		SourceSemanticHash: plan.SourceSemanticHash, TargetSemanticHash: plan.TargetSemanticHash,
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageSourceExported); err != nil {
		return result, err
	}
	sessionEnvelope, inventoryKey, inventoryValue := analyzed.sessionEnvelope, analyzed.inventoryKey, analyzed.inventoryValue
	if plan.AlgorithmVersion == legacyMigrationAlgorithm && plan.ProjectionVersion == legacyMigrationProjection {
		sessionEnvelope, inventoryKey, inventoryValue = analyzed.legacySessionEnvelope, nil, nil
	}
	if err := replaceV16WithV3(ctx, connection, sessionEnvelope, inventoryKey, inventoryValue, analyzed.promptEnvelope, analyzed.forkRecords, receipt, checkpoint); err != nil {
		return result, err
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageBeforeCommit); err != nil {
		return result, err
	}
	if _, err := connection.ExecContext(ctx, `COMMIT`); err != nil {
		return result, err
	}
	committed = true
	return resultFromReceipt(receipt, false), nil
}

func runMigrationCheckpoint(ctx context.Context, checkpoint migrationCheckpoint, stage migrationStage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if checkpoint != nil {
		if err := checkpoint(stage); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func replayV3Migration(ctx context.Context, connection *sql.Conn, plan v2MigrationPlanWire) (*migrationReceipt, error) {
	var tableCount, backendTableCount int
	if err := connection.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return nil, err
	}
	if err := connection.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('floret_backend_metadata', 'floret_backend_records')`).Scan(&backendTableCount); err != nil {
		return nil, err
	}
	if tableCount != 2 || backendTableCount != 2 {
		return nil, nil
	}
	var physicalSchema, receiptJSON []byte
	if err := connection.QueryRowContext(ctx, `SELECT value FROM floret_backend_metadata WHERE name = 'physical_schema'`).Scan(&physicalSchema); err != nil {
		return nil, &MigrationSchemaError{Version: migrationSchemaVersion, Reason: "missing physical schema metadata"}
	}
	if !bytes.Equal(physicalSchema, []byte("1")) {
		return nil, &MigrationSchemaError{Version: migrationSchemaVersion, Reason: fmt.Sprintf("unsupported physical schema %q", physicalSchema)}
	}
	if err := connection.QueryRowContext(ctx, `SELECT value FROM floret_backend_metadata WHERE name = 'migration_v3_receipt'`).Scan(&receiptJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMigrationConflict
		}
		return nil, err
	}
	receipt, err := decodeMigrationReceipt(receiptJSON)
	if err != nil {
		return nil, &MigrationSchemaError{Version: migrationSchemaVersion, Reason: "invalid migration receipt"}
	}
	if receipt.OperationID != plan.OperationID || receipt.PlanHash != plan.PlanHash ||
		receipt.CoordinatorCommitment != plan.CoordinatorCommitment ||
		receipt.SourceSemanticHash != plan.SourceSemanticHash || receipt.TargetSemanticHash != plan.TargetSemanticHash {
		return nil, ErrMigrationConflict
	}
	if err := validateMigratedContent(ctx, connection, receipt); err != nil {
		return nil, &MigrationSchemaError{Version: migrationSchemaVersion, Fingerprint: migrationSchemaFingerprint, Reason: err.Error()}
	}
	return &receipt, nil
}

type migratedForkRecord struct {
	key   []byte
	value []byte
}

func replaceV16WithV3(ctx context.Context, connection *sql.Conn, sessionEnvelope, inventoryKey, inventoryValue, promptEnvelope []byte, forkRecords []migratedForkRecord, receipt migrationReceipt, checkpoint migrationCheckpoint) error {
	rows, err := connection.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return err
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			rows.Close()
			return err
		}
		if !validLegacyTableName(table) {
			rows.Close()
			return &MigrationSchemaError{Version: "16", Reason: fmt.Sprintf("invalid table name %q", table)}
		}
		tables = append(tables, table)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, table := range tables {
		if _, err := connection.ExecContext(ctx, `DROP TABLE "`+table+`"`); err != nil {
			return err
		}
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageLegacyDropped); err != nil {
		return err
	}
	statements := []string{
		`CREATE TABLE floret_backend_metadata (name TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID`,
		`CREATE TABLE floret_backend_records (namespace TEXT NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key)) WITHOUT ROWID`,
	}
	for _, statement := range statements {
		if _, err := connection.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageBackendSchemaCreated); err != nil {
		return err
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	logicalSchema, err := json.Marshal(migrationLogicalSchema{Version: migrationSchemaVersion, Fingerprint: migrationSchemaFingerprint})
	if err != nil {
		return err
	}
	if _, err := connection.ExecContext(ctx, `INSERT INTO floret_backend_metadata(name, value) VALUES ('physical_schema', CAST('1' AS BLOB)), ('migration_v3_receipt', ?)`, receiptJSON); err != nil {
		return err
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageMetadataWritten); err != nil {
		return err
	}
	records := []struct {
		namespace string
		key       []byte
		value     []byte
	}{
		{migrationSystemNamespace, []byte("logical-schema"), logicalSchema},
		{migrationBackendNamespace, migrationSessionKey, sessionEnvelope},
		{migrationBackendNamespace, migrationPromptKey, promptEnvelope},
	}
	if len(inventoryKey) > 0 {
		records = append(records, struct {
			namespace string
			key       []byte
			value     []byte
		}{migrationBackendNamespace, inventoryKey, inventoryValue})
	}
	for _, record := range records {
		if _, err := connection.ExecContext(ctx, `INSERT INTO floret_backend_records(namespace, key, value) VALUES (?, ?, ?)`, record.namespace, record.key, record.value); err != nil {
			return err
		}
	}
	for _, record := range forkRecords {
		if _, err := connection.ExecContext(ctx, `INSERT INTO floret_backend_records(namespace, key, value) VALUES (?, ?, ?)`, migrationBackendNamespace, record.key, record.value); err != nil {
			return err
		}
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageRecordsWritten); err != nil {
		return err
	}
	if err := validateMigratedContent(ctx, connection, receipt); err != nil {
		return err
	}
	return runMigrationCheckpoint(ctx, checkpoint, migrationStageContentValidated)
}

func validateMigratedContent(ctx context.Context, connection *sql.Conn, receipt migrationReceipt) error {
	var logicalSchemaJSON, sessionEnvelope, promptEnvelope []byte
	if err := connection.QueryRowContext(ctx, `SELECT value FROM floret_backend_records WHERE namespace = ? AND key = ?`, migrationSystemNamespace, []byte("logical-schema")).Scan(&logicalSchemaJSON); err != nil {
		return err
	}
	logicalSchema, err := decodeMigrationLogicalSchema(logicalSchemaJSON)
	if err != nil {
		return err
	}
	if logicalSchema.Version != migrationSchemaVersion || logicalSchema.Fingerprint != migrationSchemaFingerprint {
		return fmt.Errorf("logical schema identity is %q/%q", logicalSchema.Version, logicalSchema.Fingerprint)
	}
	if err := connection.QueryRowContext(ctx, `SELECT value FROM floret_backend_records WHERE namespace = ? AND key = ?`, migrationBackendNamespace, migrationSessionKey).Scan(&sessionEnvelope); err != nil {
		return err
	}
	if err := connection.QueryRowContext(ctx, `SELECT value FROM floret_backend_records WHERE namespace = ? AND key = ?`, migrationBackendNamespace, migrationPromptKey).Scan(&promptEnvelope); err != nil {
		return err
	}
	session, err := storagecodec.DecodeEnvelope(sessionEnvelope, "sessiontree")
	if err != nil {
		return err
	}
	memory, err := sessiontree.DecodeMemoryState(session, time.Now)
	if err != nil {
		return fmt.Errorf("invalid session-tree state: %w", err)
	}
	inventoryKey, expectedInventory, err := sessiontree.EncodeBackendRootThreadInventoryRecord(memory)
	if err != nil {
		return fmt.Errorf("derive root-thread inventory: %w", err)
	}
	var inventoryValue []byte
	inventoryFound := true
	if err := connection.QueryRowContext(ctx, `SELECT value FROM floret_backend_records WHERE namespace = ? AND key = ?`, migrationBackendNamespace, inventoryKey).Scan(&inventoryValue); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		inventoryFound = false
	}
	if inventoryFound && !bytes.Equal(inventoryValue, expectedInventory) {
		return errors.New("root-thread inventory does not match canonical session-tree state")
	}
	var counts struct {
		Version int                          `json:"version"`
		Threads map[string]json.RawMessage   `json:"threads"`
		Entries map[string][]json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(session, &counts); err != nil {
		return err
	}
	entries := 0
	for _, values := range counts.Entries {
		entries += len(values)
	}
	if len(counts.Threads) != receipt.Threads || entries != receipt.Entries {
		return fmt.Errorf("receipt counts are threads=%d entries=%d, content has threads=%d entries=%d", receipt.Threads, receipt.Entries, len(counts.Threads), entries)
	}
	prompt, err := storagecodec.DecodeEnvelope(promptEnvelope, "prompt")
	if err != nil {
		return err
	}
	forkRecords, err := readMigratedForkRecords(ctx, connection, inventoryKey)
	if err != nil {
		return err
	}
	var got string
	if receipt.AlgorithmVersion == legacyMigrationAlgorithm && receipt.ProjectionVersion == legacyMigrationProjection {
		legacySession := session
		switch counts.Version {
		case 3:
			if inventoryFound {
				return errors.New("legacy v3 migration target contains a premature root-thread inventory")
			}
		case 4:
			if !inventoryFound {
				return errors.New("upgraded v4 migration target is missing root-thread inventory")
			}
			legacySession, err = projectCurrentSessionToLegacyV3(session)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("legacy migration target has unsupported session-tree version %d", counts.Version)
		}
		got = legacyTargetSemanticHash(legacySession, prompt, forkRecords)
	} else {
		if counts.Version != 4 || !inventoryFound {
			return errors.New("current migration target requires complete v4 session-tree records")
		}
		got = targetSemanticHash(session, inventoryKey, inventoryValue, prompt, forkRecords)
	}
	if got != receipt.TargetSemanticHash {
		return &MigrationSchemaError{Version: migrationSchemaVersion, Reason: "migration content hash mismatch"}
	}
	return nil
}

func decodeMigrationLogicalSchema(encoded []byte) (migrationLogicalSchema, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var schema migrationLogicalSchema
	if err := decoder.Decode(&schema); err != nil {
		return schema, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return schema, errors.New("logical schema contains trailing data")
		}
		return schema, err
	}
	return schema, nil
}

func targetSemanticHash(session, inventoryKey, inventoryValue, prompt []byte, forkRecords []migratedForkRecord) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("floret-v3-target-semantic/2\x00"))
	_, _ = hash.Write(session)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(inventoryKey)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(inventoryValue)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(prompt)
	for _, record := range forkRecords {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(record.key)
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(record.value)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func legacyTargetSemanticHash(session, prompt []byte, forkRecords []migratedForkRecord) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("floret-v3-target-semantic/1\x00"))
	writeMigrationSemanticContent(hash, session, prompt, forkRecords)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func encodeMigratedForkOperations(operations []internalstorage.ForkOperationRecord) ([]migratedForkRecord, error) {
	records := make([]migratedForkRecord, 0, len(operations))
	for _, operation := range operations {
		payload, err := json.Marshal(operation)
		if err != nil {
			return nil, err
		}
		value, err := storagecodec.EncodeEnvelope("fork_operation", payload)
		if err != nil {
			return nil, err
		}
		key := storagecodec.Tuple(storagecodec.TupleString("fork"), storagecodec.TupleString(operation.OperationID))
		records = append(records, migratedForkRecord{key: key, value: value})
	}
	return records, nil
}

func readMigratedForkRecords(ctx context.Context, connection *sql.Conn, inventoryKey []byte) ([]migratedForkRecord, error) {
	rows, err := connection.QueryContext(ctx, `SELECT key, value FROM floret_backend_records
		WHERE namespace = ? AND key NOT IN (?, ?, ?) ORDER BY key`, migrationBackendNamespace, migrationSessionKey, inventoryKey, migrationPromptKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []migratedForkRecord
	for rows.Next() {
		var record migratedForkRecord
		if err := rows.Scan(&record.key, &record.value); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func decodeMigrationReceipt(encoded []byte) (migrationReceipt, error) {
	var receipt migrationReceipt
	if err := decodeStrictMigrationJSON(encoded, &receipt); err != nil {
		return receipt, err
	}
	version, fingerprint := legacy.V16Identity()
	if receipt.Version != migrationPlanVersion || !supportedMigrationContract(receipt.AlgorithmVersion, receipt.ProjectionVersion) ||
		strings.TrimSpace(receipt.OperationID) == "" ||
		receipt.SourceVersion != version || receipt.SourceFingerprint != fingerprint || receipt.Threads < 0 || receipt.Entries < 0 ||
		!validSemanticHash(receipt.PlanHash) || !validSemanticHash(receipt.SourceSemanticHash) || !validSemanticHash(receipt.TargetSemanticHash) {
		return receipt, errors.New("unsupported migration receipt")
	}
	return receipt, nil
}

func migrationSchemaError(err error) error {
	var unsupported *internalstorage.UnsupportedStoreSchemaError
	if errors.As(err, &unsupported) {
		return &MigrationSchemaError{
			Version: unsupported.Observed.Version, Fingerprint: unsupported.Observed.Fingerprint,
			Reason: "source must match exact Floret schema v16",
		}
	}
	return err
}

func resultFromReceipt(receipt migrationReceipt, replayed bool) V2MigrationReceipt {
	return V2MigrationReceipt{
		OperationID: receipt.OperationID, Replayed: replayed, PlanHash: receipt.PlanHash,
		SourceSemanticHash: receipt.SourceSemanticHash, TargetSemanticHash: receipt.TargetSemanticHash,
		CoordinatorCommitment: receipt.CoordinatorCommitment, Threads: receipt.Threads, Entries: receipt.Entries,
	}
}

func validLegacyTableName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if character != '_' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func rollbackMigration(connection *sql.Conn) error {
	_, err := connection.ExecContext(context.Background(), `ROLLBACK`)
	if err != nil && !strings.Contains(err.Error(), "no transaction is active") {
		return err
	}
	return nil
}
