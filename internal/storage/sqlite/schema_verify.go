package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/floegence/floret/internal/storage"
)

type schemaColumnContract struct {
	Name       string
	Type       string
	NotNull    bool
	Default    string
	PrimaryKey int
}

type schemaIndexContract struct {
	Name    string
	Unique  bool
	Origin  string
	Partial bool
	Columns []string
}

type schemaForeignKeyContract struct {
	ID       int
	Sequence int
	Table    string
	From     string
	To       string
	OnUpdate string
	OnDelete string
	Match    string
}

type schemaTableContract struct {
	Columns     []schemaColumnContract
	Indexes     []schemaIndexContract
	ForeignKeys []schemaForeignKeyContract
}

type schemaObjectContract struct {
	Type      string
	Name      string
	TableName string
	SQL       string
}

type schemaContract struct {
	Tables  map[string]schemaTableContract
	Objects []schemaObjectContract
}

var (
	canonicalContractOnce sync.Once
	canonicalContract     schemaContract
	canonicalContractErr  error
	schema13ContractOnce  sync.Once
	schema13Contract      schemaContract
	schema13ContractErr   error
	schema14ContractOnce  sync.Once
	schema14Contract      schemaContract
	schema14ContractErr   error
	schema15ContractOnce  sync.Once
	schema15Contract      schemaContract
	schema15ContractErr   error
)

func verifySchemaVersion(ctx context.Context, q sqlRunner, version string) error {
	rawVersion, err := metaValue(ctx, q, "raw_encoder_version")
	if err != nil {
		if errors.Is(err, storage.ErrMetadataNotFound) {
			return unsupportedSchemaError(version, "")
		}
		return fmt.Errorf("read sqlite store raw encoder version: %w", err)
	}
	if rawVersion != rawEncoderVersion {
		return fmt.Errorf("%w: unsupported raw encoder version %q", unsupportedSchemaError(version, ""), rawVersion)
	}
	fingerprint, err := metaValue(ctx, q, "schema_fingerprint")
	if err != nil {
		if errors.Is(err, storage.ErrMetadataNotFound) {
			return unsupportedSchemaError(version, "")
		}
		return fmt.Errorf("read sqlite store schema fingerprint: %w", err)
	}
	var expectedFingerprint string
	switch version {
	case schemaVersion13:
		expectedFingerprint = schemaFingerprintVersion13
	case schemaVersion14:
		expectedFingerprint = schemaFingerprintVersion14
	case schemaVersion15:
		expectedFingerprint = schemaFingerprintVersion15
	case schemaVersion:
		expectedFingerprint = schemaFingerprintVersion16
	default:
		return unsupportedSchemaError(version, fingerprint)
	}
	if fingerprint != expectedFingerprint {
		return unsupportedSchemaError(version, fingerprint)
	}
	if err := verifySchemaContract(ctx, q, version); err != nil {
		return fmt.Errorf("%w: %v", unsupportedSchemaError(version, fingerprint), err)
	}
	return nil
}

func verifySchemaContract(ctx context.Context, q sqlRunner, version string) error {
	expected, err := expectedSchemaContract(version)
	if err != nil {
		return err
	}
	actual, err := readSchemaContract(ctx, q)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("sqlite store schema contract mismatch: %s", describeSchemaContractMismatch(actual, expected))
	}
	rows, err := q.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("sqlite store foreign key check failed")
	}
	return rows.Err()
}

func describeSchemaContractMismatch(actual, expected schemaContract) string {
	actualNames := make([]string, 0, len(actual.Tables))
	for name := range actual.Tables {
		actualNames = append(actualNames, name)
	}
	expectedNames := make([]string, 0, len(expected.Tables))
	for name := range expected.Tables {
		expectedNames = append(expectedNames, name)
	}
	sort.Strings(actualNames)
	sort.Strings(expectedNames)
	if !reflect.DeepEqual(actualNames, expectedNames) {
		return fmt.Sprintf("tables got %v want %v", actualNames, expectedNames)
	}
	for _, name := range expectedNames {
		if !reflect.DeepEqual(actual.Tables[name], expected.Tables[name]) {
			return fmt.Sprintf("table %q got %#v want %#v", name, actual.Tables[name], expected.Tables[name])
		}
	}
	limit := len(actual.Objects)
	if len(expected.Objects) < limit {
		limit = len(expected.Objects)
	}
	for index := 0; index < limit; index++ {
		if actual.Objects[index] != expected.Objects[index] {
			return fmt.Sprintf("object %d got %#v want %#v", index, actual.Objects[index], expected.Objects[index])
		}
	}
	return fmt.Sprintf("object count got %d want %d", len(actual.Objects), len(expected.Objects))
}

func expectedSchemaContract(version string) (schemaContract, error) {
	build := func(schema string) (schemaContract, error) {
		db, err := sql.Open(driverName, ":memory:")
		if err != nil {
			return schemaContract{}, err
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		if _, err := db.Exec(schema); err != nil {
			return schemaContract{}, fmt.Errorf("build canonical sqlite schema contract: %w", err)
		}
		return readSchemaContract(context.Background(), db)
	}
	switch version {
	case schemaVersion13:
		schema13ContractOnce.Do(func() {
			schema13Contract, schema13ContractErr = build(schemaVersion13SQL)
		})
		return cloneSchemaContract(schema13Contract), schema13ContractErr
	case schemaVersion14:
		schema14ContractOnce.Do(func() {
			schema14Contract, schema14ContractErr = build(schemaVersion14SQL)
		})
		return cloneSchemaContract(schema14Contract), schema14ContractErr
	case schemaVersion15:
		schema15ContractOnce.Do(func() {
			schema15Contract, schema15ContractErr = build(schemaVersion15SQL)
		})
		return cloneSchemaContract(schema15Contract), schema15ContractErr
	case schemaVersion:
		canonicalContractOnce.Do(func() {
			canonicalContract, canonicalContractErr = build(schemaSQL)
		})
		return cloneSchemaContract(canonicalContract), canonicalContractErr
	default:
		return schemaContract{}, fmt.Errorf("unsupported sqlite schema contract version %q", version)
	}
}

func readSchemaContract(ctx context.Context, q sqlRunner) (schemaContract, error) {
	tables, err := userTableNames(ctx, q)
	if err != nil {
		return schemaContract{}, err
	}
	contract := schemaContract{Tables: make(map[string]schemaTableContract, len(tables))}
	for _, tableName := range tables {
		table, err := readSchemaTableContract(ctx, q, tableName)
		if err != nil {
			return schemaContract{}, fmt.Errorf("read sqlite schema contract for table %q: %w", tableName, err)
		}
		contract.Tables[tableName] = table
	}
	contract.Objects, err = readSchemaObjectContracts(ctx, q)
	if err != nil {
		return schemaContract{}, err
	}
	return contract, nil
}

func readSchemaObjectContracts(ctx context.Context, q sqlRunner) ([]schemaObjectContract, error) {
	rows, err := q.QueryContext(ctx, `SELECT type, name, tbl_name, sql FROM sqlite_master
		WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL
		ORDER BY type, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var objects []schemaObjectContract
	for rows.Next() {
		var object schemaObjectContract
		if err := rows.Scan(&object.Type, &object.Name, &object.TableName, &object.SQL); err != nil {
			return nil, err
		}
		object.SQL = normalizeSchemaSQL(object.SQL)
		objects = append(objects, object)
	}
	return objects, rows.Err()
}

func normalizeSchemaSQL(value string) string {
	return strings.Join(strings.Fields(strings.TrimSuffix(strings.TrimSpace(value), ";")), " ")
}

func readSchemaTableContract(ctx context.Context, q sqlRunner, tableName string) (schemaTableContract, error) {
	quotedTable := quoteSchemaName(tableName)
	columns, err := q.QueryContext(ctx, `PRAGMA table_info(`+quotedTable+`)`)
	if err != nil {
		return schemaTableContract{}, err
	}
	table := schemaTableContract{}
	for columns.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := columns.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = columns.Close()
			return schemaTableContract{}, err
		}
		table.Columns = append(table.Columns, schemaColumnContract{
			Name:       name,
			Type:       strings.ToUpper(strings.TrimSpace(columnType)),
			NotNull:    notNull != 0,
			Default:    strings.TrimSpace(defaultValue.String),
			PrimaryKey: primaryKey,
		})
	}
	if err := columns.Err(); err != nil {
		_ = columns.Close()
		return schemaTableContract{}, err
	}
	if err := columns.Close(); err != nil {
		return schemaTableContract{}, err
	}

	indexes, err := q.QueryContext(ctx, `PRAGMA index_list(`+quotedTable+`)`)
	if err != nil {
		return schemaTableContract{}, err
	}
	for indexes.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := indexes.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			_ = indexes.Close()
			return schemaTableContract{}, err
		}
		table.Indexes = append(table.Indexes, schemaIndexContract{Name: name, Unique: unique != 0, Origin: origin, Partial: partial != 0})
	}
	if err := indexes.Err(); err != nil {
		_ = indexes.Close()
		return schemaTableContract{}, err
	}
	if err := indexes.Close(); err != nil {
		return schemaTableContract{}, err
	}
	for index := range table.Indexes {
		indexColumns, err := q.QueryContext(ctx, `PRAGMA index_info(`+quoteSchemaName(table.Indexes[index].Name)+`)`)
		if err != nil {
			return schemaTableContract{}, err
		}
		for indexColumns.Next() {
			var sequence, cid int
			var columnName sql.NullString
			if err := indexColumns.Scan(&sequence, &cid, &columnName); err != nil {
				_ = indexColumns.Close()
				return schemaTableContract{}, err
			}
			table.Indexes[index].Columns = append(table.Indexes[index].Columns, columnName.String)
		}
		if err := indexColumns.Err(); err != nil {
			_ = indexColumns.Close()
			return schemaTableContract{}, err
		}
		if err := indexColumns.Close(); err != nil {
			return schemaTableContract{}, err
		}
	}
	sort.Slice(table.Indexes, func(i, j int) bool { return table.Indexes[i].Name < table.Indexes[j].Name })

	foreignKeys, err := q.QueryContext(ctx, `PRAGMA foreign_key_list(`+quotedTable+`)`)
	if err != nil {
		return schemaTableContract{}, err
	}
	for foreignKeys.Next() {
		var foreignKey schemaForeignKeyContract
		if err := foreignKeys.Scan(&foreignKey.ID, &foreignKey.Sequence, &foreignKey.Table, &foreignKey.From, &foreignKey.To, &foreignKey.OnUpdate, &foreignKey.OnDelete, &foreignKey.Match); err != nil {
			_ = foreignKeys.Close()
			return schemaTableContract{}, err
		}
		table.ForeignKeys = append(table.ForeignKeys, foreignKey)
	}
	if err := foreignKeys.Err(); err != nil {
		_ = foreignKeys.Close()
		return schemaTableContract{}, err
	}
	if err := foreignKeys.Close(); err != nil {
		return schemaTableContract{}, err
	}
	return table, nil
}

func userTableNames(ctx context.Context, q sqlRunner) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, rows.Err()
}

func cloneSchemaContract(in schemaContract) schemaContract {
	out := schemaContract{
		Tables:  make(map[string]schemaTableContract, len(in.Tables)),
		Objects: append([]schemaObjectContract(nil), in.Objects...),
	}
	for name, table := range in.Tables {
		table.Columns = append([]schemaColumnContract(nil), table.Columns...)
		table.ForeignKeys = append([]schemaForeignKeyContract(nil), table.ForeignKeys...)
		var indexes []schemaIndexContract
		if table.Indexes != nil {
			indexes = make([]schemaIndexContract, len(table.Indexes))
			for index, item := range table.Indexes {
				item.Columns = append([]string(nil), item.Columns...)
				indexes[index] = item
			}
		}
		table.Indexes = indexes
		out.Tables[name] = table
	}
	return out
}

func quoteSchemaName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
