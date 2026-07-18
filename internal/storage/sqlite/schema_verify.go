package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
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

type schemaContract struct {
	Tables map[string]schemaTableContract
}

var (
	canonicalContractOnce sync.Once
	canonicalContract     schemaContract
	canonicalContractErr  error
)

func verifySchemaVersion(ctx context.Context, tx sqlRunner, version string) error {
	rawVersion, err := metaValue(ctx, tx, "raw_encoder_version")
	if err != nil {
		return fmt.Errorf("read raw encoder version: %w", err)
	}
	if rawVersion != rawEncoderVersion {
		return fmt.Errorf("unsupported sqlite store raw encoder version %q", rawVersion)
	}
	fingerprint, err := metaValue(ctx, tx, "schema_fingerprint")
	if err != nil {
		return fmt.Errorf("unsupported sqlite store schema fingerprint: %w", err)
	}
	switch version {
	case schemaVersion11:
		if fingerprint != schemaFingerprintVersion11 {
			return fmt.Errorf("unsupported sqlite store schema fingerprint %q", fingerprint)
		}
		return verifySchemaContract(ctx, tx, false)
	case schemaVersion:
		if fingerprint != schemaFingerprintVersion12 && fingerprint != canonicalSchemaFingerprint() {
			return fmt.Errorf("unsupported sqlite store schema fingerprint %q", fingerprint)
		}
		return verifySchemaContract(ctx, tx, true)
	default:
		return fmt.Errorf("unsupported sqlite store schema version %q; minimum supported version is %s", version, minimumSchemaVersion)
	}
}

func verifySchemaContract(ctx context.Context, tx sqlRunner, includeAgentTodos bool) error {
	expected, err := expectedSchemaContract(includeAgentTodos)
	if err != nil {
		return err
	}
	actual, err := readSchemaContract(ctx, tx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("sqlite store schema contract mismatch: got %#v, want %#v", actual, expected)
	}
	var unsupportedObjects []string
	rows, err := tx.QueryContext(ctx, `SELECT type || ':' || name FROM sqlite_master
		WHERE type IN ('view', 'trigger') AND name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var object string
		if err := rows.Scan(&object); err != nil {
			_ = rows.Close()
			return err
		}
		unsupportedObjects = append(unsupportedObjects, object)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(unsupportedObjects) > 0 {
		return fmt.Errorf("sqlite store contains unsupported schema objects %v", unsupportedObjects)
	}
	rows, err = tx.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("sqlite store foreign key check failed")
	}
	return rows.Err()
}

func expectedSchemaContract(includeAgentTodos bool) (schemaContract, error) {
	canonicalContractOnce.Do(func() {
		db, err := sql.Open(driverName, ":memory:")
		if err != nil {
			canonicalContractErr = err
			return
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		if _, err := db.Exec(schemaSQL); err != nil {
			canonicalContractErr = fmt.Errorf("build canonical sqlite schema contract: %w", err)
			return
		}
		canonicalContract, canonicalContractErr = readSchemaContract(context.Background(), db)
	})
	if canonicalContractErr != nil {
		return schemaContract{}, canonicalContractErr
	}
	out := cloneSchemaContract(canonicalContract)
	if !includeAgentTodos {
		delete(out.Tables, "agent_todo_states")
	}
	return out, nil
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
	return contract, nil
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
	out := schemaContract{Tables: make(map[string]schemaTableContract, len(in.Tables))}
	for name, table := range in.Tables {
		table.Columns = append([]schemaColumnContract(nil), table.Columns...)
		table.ForeignKeys = append([]schemaForeignKeyContract(nil), table.ForeignKeys...)
		indexes := make([]schemaIndexContract, len(table.Indexes))
		for index, item := range table.Indexes {
			item.Columns = append([]string(nil), item.Columns...)
			indexes[index] = item
		}
		table.Indexes = indexes
		out.Tables[name] = table
	}
	return out
}

func quoteSchemaName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
