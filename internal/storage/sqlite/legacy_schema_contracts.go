package sqlite

import (
	"context"
	_ "embed"
	"fmt"
	"reflect"
	"sync"
)

// These schemas are the verbatim schemaSQL values published by the listed tags.
// They are durable migration inputs, not alternate current schemas.
//
//go:embed testdata/schema-v3.sql
var legacySchemaVersion3SQL string // v0.1.0

//go:embed testdata/schema-v4.sql
var legacySchemaVersion4SQL string // v0.2.0

//go:embed testdata/schema-v5.sql
var legacySchemaVersion5SQL string // v0.3.0

//go:embed testdata/schema-v6.sql
var legacySchemaVersion6SQL string // v0.3.3

//go:embed testdata/schema-v7.sql
var legacySchemaVersion7SQL string // v0.3.17

//go:embed testdata/schema-v8.sql
var legacySchemaVersion8SQL string // v0.3.45

//go:embed testdata/schema-v9.sql
var legacySchemaVersion9SQL string // v0.3.76

//go:embed testdata/schema-v10.sql
var legacySchemaVersion10SQL string // v0.5.0

//go:embed testdata/schema-v11.sql
var legacyPublishedSchemaVersion11SQL string // v0.10.0

//go:embed testdata/schema-v12.sql
var legacyPublishedSchemaVersion12SQL string // v0.11.0

//go:embed testdata/schema-v13.sql
var legacyPublishedSchemaVersion13SQL string // v0.17.0

type legacySchemaContractCache struct {
	once     sync.Once
	contract schemaContract
	err      error
}

var legacySchemaContractCaches = map[string]*legacySchemaContractCache{
	"3": {}, "4": {}, "5": {}, "6": {}, "7": {}, "8": {}, "9": {}, "10": {},
	schemaVersion11: {}, schemaVersion12: {}, schemaVersion13: {},
}

func legacyPublishedSchemaSQL(version string) (string, bool) {
	switch version {
	case "3":
		return legacySchemaVersion3SQL, true
	case "4":
		return legacySchemaVersion4SQL, true
	case "5":
		return legacySchemaVersion5SQL, true
	case "6":
		return legacySchemaVersion6SQL, true
	case "7":
		return legacySchemaVersion7SQL, true
	case "8":
		return legacySchemaVersion8SQL, true
	case "9":
		return legacySchemaVersion9SQL, true
	case "10":
		return legacySchemaVersion10SQL, true
	case schemaVersion11:
		return legacyPublishedSchemaVersion11SQL, true
	case schemaVersion12:
		return legacyPublishedSchemaVersion12SQL, true
	case schemaVersion13:
		return legacyPublishedSchemaVersion13SQL, true
	default:
		return "", false
	}
}

func expectedLegacySchemaContract(version string) (schemaContract, error) {
	cache, ok := legacySchemaContractCaches[version]
	if !ok {
		return schemaContract{}, fmt.Errorf("unsupported legacy sqlite schema contract version %q", version)
	}
	schema, _ := legacyPublishedSchemaSQL(version)
	cache.once.Do(func() {
		cache.contract, cache.err = buildSchemaContract(schema)
	})
	return cloneSchemaContract(cache.contract), cache.err
}

func verifyLegacySchemaContract(ctx context.Context, q sqlRunner, version string) error {
	expected, err := expectedLegacySchemaContract(version)
	if err != nil {
		return err
	}
	actual, err := readSchemaContract(ctx, q)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("sqlite store legacy schema contract mismatch: %s", describeSchemaContractMismatch(actual, expected))
	}
	rows, err := q.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("sqlite store legacy foreign key check failed")
	}
	return rows.Err()
}
