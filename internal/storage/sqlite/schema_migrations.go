package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

var schemaVersion13DropOrder = []string{
	"metadata_records",
	"tool_output_artifacts",
	"prompt_responses",
	"prompt_requests",
	"prompt_toolsets",
	"prompt_segments",
	"active_turn_leases",
	"agent_todo_states",
	"provider_states",
	"entries",
	"fork_operations",
	"threads",
	"schema_meta",
}

func migrateEmptySchemaVersion13(ctx context.Context, tx sqlRunner, leasePolicy sessiontree.LeasePolicy) error {
	if err := verifySchemaVersion(ctx, tx, schemaVersion13); err != nil {
		return fmt.Errorf("verify sqlite store schema v13 before migration: %w", err)
	}
	nonEmpty, err := nonEmptySchemaVersion13Tables(ctx, tx)
	if err != nil {
		return err
	}
	if len(nonEmpty) > 0 {
		return fmt.Errorf("%w: sqlite store schema v13 is not empty: tables %s contain data", unsupportedSchemaError(schemaVersion13, schemaFingerprintVersion13), strings.Join(nonEmpty, ", "))
	}
	for _, tableName := range schemaVersion13DropOrder {
		if _, err := tx.ExecContext(ctx, `DROP TABLE `+quoteSchemaName(tableName)); err != nil {
			return fmt.Errorf("drop sqlite schema v13 table %q: %w", tableName, err)
		}
	}
	if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("create sqlite store schema v14: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`, schemaVersion, rawEncoderVersion, schemaFingerprintVersion14); err != nil {
		return fmt.Errorf("write sqlite store schema v14 metadata: %w", err)
	}
	if err := persistLeasePolicy(ctx, tx, leasePolicy); err != nil {
		return err
	}
	return nil
}

func persistLeasePolicy(ctx context.Context, q sqlRunner, policy sessiontree.LeasePolicy) error {
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("validate sqlite store lease policy: %w", err)
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO authority_lease_policy(
		singleton, lease_ttl_ns, renew_interval_ns, clock_skew_allowance_ns
	) VALUES(1, ?, ?, ?)`, int64(policy.TTL), int64(policy.RenewInterval), int64(policy.ClockSkewAllowance)); err != nil {
		return fmt.Errorf("persist sqlite store lease policy: %w", err)
	}
	return nil
}

func verifyLeasePolicy(ctx context.Context, q sqlRunner, configured sessiontree.LeasePolicy) error {
	var ttl, renew, skew int64
	if err := q.QueryRowContext(ctx, `SELECT lease_ttl_ns, renew_interval_ns, clock_skew_allowance_ns
		FROM authority_lease_policy WHERE singleton = 1`).Scan(&ttl, &renew, &skew); err != nil {
		return fmt.Errorf("read sqlite store lease policy: %w", err)
	}
	persisted := sessiontree.LeasePolicy{
		TTL:                time.Duration(ttl),
		RenewInterval:      time.Duration(renew),
		ClockSkewAllowance: time.Duration(skew),
	}
	if persisted != configured {
		return &storage.StoreLeasePolicyMismatchError{Configured: configured, Persisted: persisted}
	}
	return nil
}

func nonEmptySchemaVersion13Tables(ctx context.Context, q sqlRunner) ([]string, error) {
	tables, err := userTableNames(ctx, q)
	if err != nil {
		return nil, err
	}
	var nonEmpty []string
	for _, tableName := range tables {
		if tableName == "schema_meta" {
			continue
		}
		var count int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+quoteSchemaName(tableName)).Scan(&count); err != nil {
			return nil, fmt.Errorf("count sqlite schema v13 table %q: %w", tableName, err)
		}
		if count > 0 {
			nonEmpty = append(nonEmpty, tableName)
		}
	}
	return nonEmpty, nil
}
