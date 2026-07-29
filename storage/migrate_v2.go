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

	"github.com/floegence/floret/v2/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v2/internal/storage"
	legacy "github.com/floegence/floret/v2/internal/storage/sqlite"
	"github.com/floegence/floret/v2/internal/storagecodec"
)

const (
	migrationBackendNamespace  = "floret.domain"
	migrationSystemNamespace   = "floret.system"
	migrationSchemaVersion     = "2"
	migrationSchemaFingerprint = "sha256:3343ff9e64073d543e491de34b1d1aaee222ca7099f3d321de9c66f266b90e03"
)

var (
	// ErrMigrationConflict reports that a v2 store was produced by a different
	// migration operation.
	ErrMigrationConflict = errors.New("floret v2 migration operation conflict")
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

// MigrationSchemaError reports a database outside the one exact v16 source
// schema or exact v2 migration receipt accepted by MigrateV2.
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

// MigrateV2Request identifies one exact SQLite migration operation.
type MigrateV2Request struct {
	Path        string
	OperationID string
}

// MigrateV2Result reports the committed operation and whether it was replayed.
type MigrateV2Result struct {
	OperationID string `json:"operation_id"`
	Replayed    bool   `json:"replayed"`
	Threads     int    `json:"threads"`
	Entries     int    `json:"entries"`
	ContentHash string `json:"content_hash"`
}

type migrationReceipt struct {
	Version           int    `json:"version"`
	OperationID       string `json:"operation_id"`
	SourceVersion     string `json:"source_version"`
	SourceFingerprint string `json:"source_fingerprint"`
	Threads           int    `json:"threads"`
	Entries           int    `json:"entries"`
	ContentHash       string `json:"content_hash"`
}

type migrationLogicalSchema struct {
	Version     string `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

var (
	migrationSessionKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))
	migrationPromptKey  = storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))
)

// MigrateV2 atomically converts one exact schema-v16 SQLite store into the v2
// backend format. It never opens, repairs, or upgrades any other schema.
func MigrateV2(ctx context.Context, request MigrateV2Request) (result MigrateV2Result, resultErr error) {
	return migrateV2(ctx, request, nil)
}

func migrateV2(ctx context.Context, request MigrateV2Request, checkpoint migrationCheckpoint) (result MigrateV2Result, resultErr error) {
	if ctx == nil {
		return result, fmt.Errorf("%w: migration context is required", ErrInvalidArgument)
	}
	path := strings.TrimSpace(request.Path)
	operationID := strings.TrimSpace(request.OperationID)
	if path == "" || path == ":memory:" || operationID == "" || path != request.Path || operationID != request.OperationID {
		return result, fmt.Errorf("%w: migration requires a canonical file path and operation ID", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
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

	replayed, err := replayMigrationV2(ctx, connection, operationID)
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
	exported, err := legacy.ExportV16State(ctx, connection)
	if err != nil {
		return result, fmt.Errorf("export floret schema v16: %w", err)
	}
	sessionEnvelope, err := storagecodec.EncodeEnvelope("sessiontree", exported.Session)
	if err != nil {
		return result, err
	}
	promptEnvelope, err := storagecodec.EncodeEnvelope("prompt", exported.Prompt)
	if err != nil {
		return result, err
	}
	forkRecords, err := encodeMigratedForkOperations(exported.ForkOperations)
	if err != nil {
		return result, err
	}
	contentHash := migrationContentHash(exported.Session, exported.Prompt, forkRecords)
	version, fingerprint := legacy.V16Identity()
	receipt := migrationReceipt{
		Version: 1, OperationID: operationID, SourceVersion: version,
		SourceFingerprint: fingerprint, Threads: exported.Threads,
		Entries: exported.Entries, ContentHash: contentHash,
	}
	if err := runMigrationCheckpoint(ctx, checkpoint, migrationStageSourceExported); err != nil {
		return result, err
	}
	if err := replaceV16WithV2(ctx, connection, sessionEnvelope, promptEnvelope, forkRecords, receipt, checkpoint); err != nil {
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

func replayMigrationV2(ctx context.Context, connection *sql.Conn, operationID string) (*migrationReceipt, error) {
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
	if err := connection.QueryRowContext(ctx, `SELECT value FROM floret_backend_metadata WHERE name = 'migration_v2_receipt'`).Scan(&receiptJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMigrationConflict
		}
		return nil, err
	}
	receipt, err := decodeMigrationReceipt(receiptJSON)
	if err != nil {
		return nil, &MigrationSchemaError{Version: migrationSchemaVersion, Reason: "invalid migration receipt"}
	}
	if receipt.OperationID != operationID {
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

func replaceV16WithV2(ctx context.Context, connection *sql.Conn, sessionEnvelope, promptEnvelope []byte, forkRecords []migratedForkRecord, receipt migrationReceipt, checkpoint migrationCheckpoint) error {
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
	if _, err := connection.ExecContext(ctx, `INSERT INTO floret_backend_metadata(name, value) VALUES ('physical_schema', CAST('1' AS BLOB)), ('migration_v2_receipt', ?)`, receiptJSON); err != nil {
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
	if _, err := sessiontree.DecodeMemoryState(session, time.Now); err != nil {
		return fmt.Errorf("invalid session-tree state: %w", err)
	}
	var counts struct {
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
	forkRecords, err := readMigratedForkRecords(ctx, connection)
	if err != nil {
		return err
	}
	if got := migrationContentHash(session, prompt, forkRecords); got != receipt.ContentHash {
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

func migrationContentHash(session, prompt []byte, forkRecords []migratedForkRecord) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("floret-v2-migration\x00"))
	_, _ = hash.Write(session)
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

func readMigratedForkRecords(ctx context.Context, connection *sql.Conn) ([]migratedForkRecord, error) {
	rows, err := connection.QueryContext(ctx, `SELECT key, value FROM floret_backend_records
		WHERE namespace = ? AND key NOT IN (?, ?) ORDER BY key`, migrationBackendNamespace, migrationSessionKey, migrationPromptKey)
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
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var receipt migrationReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return receipt, errors.New("migration receipt contains trailing data")
		}
		return receipt, err
	}
	version, fingerprint := legacy.V16Identity()
	if receipt.Version != 1 || strings.TrimSpace(receipt.OperationID) == "" || receipt.SourceVersion != version ||
		receipt.SourceFingerprint != fingerprint || receipt.Threads < 0 || receipt.Entries < 0 || !strings.HasPrefix(receipt.ContentHash, "sha256:") {
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

func resultFromReceipt(receipt migrationReceipt, replayed bool) MigrateV2Result {
	return MigrateV2Result{
		OperationID: receipt.OperationID, Replayed: replayed, Threads: receipt.Threads,
		Entries: receipt.Entries, ContentHash: receipt.ContentHash,
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
