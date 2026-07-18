package sqlite

import (
	"context"
	"fmt"
)

type schemaMigration struct {
	from        string
	to          string
	fingerprint string
	apply       func(context.Context, sqlRunner) error
}

var schemaMigrations = []schemaMigration{
	{from: "11", to: "12", fingerprint: canonicalSchemaFingerprint(), apply: migrateSchemaVersion11To12},
}

func migrateSchema(ctx context.Context, tx sqlRunner, current string) error {
	for current != schemaVersion {
		migration := findSchemaMigration(current)
		if migration == nil {
			return fmt.Errorf("unsupported sqlite store schema version %q; minimum supported version is %s", current, minimumSchemaVersion)
		}
		if err := verifySchemaVersion(ctx, tx, migration.from); err != nil {
			return fmt.Errorf("verify sqlite store schema v%s before migration: %w", migration.from, err)
		}
		if err := migration.apply(ctx, tx); err != nil {
			return fmt.Errorf("migrate sqlite store schema v%s to v%s: %w", migration.from, migration.to, err)
		}
		if err := updateSchemaMeta(ctx, tx, "schema_version", migration.to); err != nil {
			return fmt.Errorf("update sqlite store schema version to v%s: %w", migration.to, err)
		}
		if err := updateSchemaMeta(ctx, tx, "schema_fingerprint", migration.fingerprint); err != nil {
			return fmt.Errorf("update sqlite store schema fingerprint for v%s: %w", migration.to, err)
		}
		current = migration.to
	}
	return nil
}

func updateSchemaMeta(ctx context.Context, tx sqlRunner, key, value string) error {
	result, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = ?`, value, key)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return fmt.Errorf("schema metadata key %q updated %d rows, want 1", key, updated)
	}
	return nil
}

func findSchemaMigration(from string) *schemaMigration {
	for i := range schemaMigrations {
		if schemaMigrations[i].from == from {
			return &schemaMigrations[i]
		}
	}
	return nil
}

func migrateSchemaVersion11To12(ctx context.Context, tx sqlRunner) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE agent_todo_states (
	thread_id TEXT PRIMARY KEY,
	version INTEGER NOT NULL,
	items_json TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	updated_by_turn_id TEXT NOT NULL DEFAULT '',
	updated_by_run_id TEXT NOT NULL DEFAULT '',
	updated_by_tool_call_id TEXT NOT NULL DEFAULT '',
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
`)
	return err
}
