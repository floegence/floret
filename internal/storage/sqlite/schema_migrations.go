package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
)

type schemaMigration struct {
	from        string
	to          string
	fingerprint string
	apply       func(context.Context, sqlRunner) error
}

var schemaMigrations = []schemaMigration{
	{from: schemaVersion11, to: schemaVersion12, fingerprint: canonicalSchemaFingerprint(), apply: migrateSchemaVersion11To12},
	{from: schemaVersion12, to: schemaVersion, fingerprint: canonicalSchemaFingerprint(), apply: migrateSchemaVersion12To13},
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

type schema12ForkPlan struct {
	Version            int                    `json:"version"`
	OperationID        string                 `json:"operation_id"`
	RequestFingerprint string                 `json:"request_fingerprint"`
	PreparedAt         time.Time              `json:"prepared_at"`
	Root               schema12ForkPlanNode   `json:"root"`
	TerminalChildren   []schema12ForkPlanNode `json:"terminal_children,omitempty"`
}

type schema12ForkPlanNode struct {
	NodeID              string                           `json:"node_id"`
	SourceThreadID      string                           `json:"source_thread_id"`
	SourceEntryID       string                           `json:"source_entry_id,omitempty"`
	DestinationThreadID string                           `json:"destination_thread_id"`
	TurnIDMap           map[string]string                `json:"turn_id_map,omitempty"`
	RunIDMap            map[string]string                `json:"run_id_map,omitempty"`
	Turns               []schema12ForkedTurnRef          `json:"turns,omitempty"`
	MetadataPatch       *sessiontree.ForkDestinationMeta `json:"metadata_patch,omitempty"`
}

type schema12ForkedTurnRef struct {
	SourceTurnID      string    `json:"source_turn_id,omitempty"`
	SourceRunID       string    `json:"source_run_id,omitempty"`
	DestinationTurnID string    `json:"destination_turn_id,omitempty"`
	DestinationRunID  string    `json:"destination_run_id,omitempty"`
	CreatedAt         time.Time `json:"created_at,omitempty"`
}

type schema13ForkPlan struct {
	Version            int                    `json:"version"`
	OperationID        string                 `json:"operation_id"`
	RequestFingerprint string                 `json:"request_fingerprint"`
	PreparedAt         time.Time              `json:"prepared_at"`
	Root               schema13ForkPlanNode   `json:"root"`
	TerminalChildren   []schema13ForkPlanNode `json:"terminal_children,omitempty"`
}

type schema13ForkPlanNode struct {
	NodeID              string                           `json:"node_id"`
	SourceThreadID      string                           `json:"source_thread_id"`
	SourceEntryID       string                           `json:"source_entry_id,omitempty"`
	DestinationThreadID string                           `json:"destination_thread_id"`
	TurnIDMap           map[string]string                `json:"turn_id_map,omitempty"`
	RunIDMap            map[string]string                `json:"run_id_map,omitempty"`
	DestinationMeta     *sessiontree.ForkDestinationMeta `json:"destination_meta,omitempty"`
}

type schema12ThreadAuthority struct {
	ID                  string
	ParentThreadID      string
	ParentTurnID        string
	ForkedFromThreadID  string
	ForkedFromEntryID   string
	ForkOperationID     string
	ForkOperationNodeID string
	TaskName            string
	TaskDescription     string
	AgentPath           string
	HostProfileRef      string
	ForkMode            string
	Closed              bool
	Status              string
}

func migrateSchemaVersion12To13(ctx context.Context, tx sqlRunner) error {
	handled, err := migrateSchema12ForkOperations(ctx, tx)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, parent_thread_id, parent_turn_id,
		forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, status
		FROM threads ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		meta, err := scanSchema12ThreadAuthority(rows)
		if err != nil {
			return err
		}
		if _, ok := handled[meta.ID]; ok {
			continue
		}
		if meta.ForkOperationID != "" || meta.ForkOperationNodeID != "" {
			return fmt.Errorf("schema v12 thread %q has fork operation markers not owned by a persisted plan", meta.ID)
		}
		if meta.ParentThreadID == "" {
			if err := validateSchema12RootAuthority(meta); err != nil {
				return err
			}
			continue
		}
		if meta.AgentPath != "" || meta.TaskName != "" {
			if meta.AgentPath == "" || meta.TaskName == "" {
				return fmt.Errorf("schema v12 thread %q has incomplete subagent identity", meta.ID)
			}
			continue
		}
		if meta.ParentTurnID != "" || meta.TaskDescription != "" || meta.HostProfileRef != "" || meta.ForkMode != "" || meta.Closed || meta.Status != "" || meta.ForkedFromThreadID != meta.ParentThreadID {
			return fmt.Errorf("schema v12 thread %q has ambiguous parent authority", meta.ID)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE threads SET parent_thread_id = '', parent_turn_id = '' WHERE id = ?`, meta.ID); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	graph, err := loadThreadAuthorityGraph(ctx, tx)
	if err != nil {
		return err
	}
	if err := sessiontree.ValidateThreadAuthorityGraph(graph); err != nil {
		return fmt.Errorf("validate migrated schema v13 thread authority: %w", err)
	}
	return nil
}

func migrateSchema12ForkOperations(ctx context.Context, tx sqlRunner) (map[string]struct{}, error) {
	handled := map[string]struct{}{}
	rows, err := tx.QueryContext(ctx, `SELECT operation_id, state, plan_json, result_json FROM fork_operations ORDER BY operation_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var operationID, state, rawPlan, rawResult string
		if err := rows.Scan(&operationID, &state, &rawPlan, &rawResult); err != nil {
			return nil, err
		}
		if state != "prepared" && state != "completed" && state != "failed" {
			return nil, fmt.Errorf("schema v12 fork operation %q has invalid state %q", operationID, state)
		}
		plan, err := decodeSchema12ForkPlan(rawPlan)
		if err != nil {
			return nil, fmt.Errorf("decode schema v12 fork operation %q: %w", operationID, err)
		}
		if plan.Version != 1 || strings.TrimSpace(plan.OperationID) != operationID || strings.TrimSpace(plan.RequestFingerprint) == "" || plan.PreparedAt.IsZero() {
			return nil, fmt.Errorf("schema v12 fork operation %q has invalid plan identity", operationID)
		}
		nodes := append([]schema12ForkPlanNode{plan.Root}, plan.TerminalChildren...)
		planned := map[string]struct{}{}
		for index, node := range nodes {
			if strings.TrimSpace(node.NodeID) == "" || strings.TrimSpace(node.SourceThreadID) == "" || strings.TrimSpace(node.DestinationThreadID) == "" {
				return nil, fmt.Errorf("schema v12 fork operation %q has incomplete node identity", operationID)
			}
			if index == 0 {
				if node.NodeID != "root" || node.MetadataPatch != nil {
					return nil, fmt.Errorf("schema v12 fork operation %q has invalid root node", operationID)
				}
			} else {
				patch := node.MetadataPatch
				if patch == nil || strings.TrimSpace(patch.ParentThreadID) != plan.Root.DestinationThreadID || strings.TrimSpace(patch.TaskName) == "" || strings.TrimSpace(patch.AgentPath) == "" {
					return nil, fmt.Errorf("schema v12 terminal child %q has invalid ownership patch", node.DestinationThreadID)
				}
			}
			if _, duplicate := planned[node.DestinationThreadID]; duplicate {
				return nil, fmt.Errorf("schema v12 fork destination %q is owned by multiple plan nodes", node.DestinationThreadID)
			}
			planned[node.DestinationThreadID] = struct{}{}
			meta, found, err := loadSchema12ThreadAuthority(ctx, tx, node.DestinationThreadID)
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}
			if meta.ForkOperationID != operationID || meta.ForkOperationNodeID != node.NodeID || meta.ForkedFromThreadID != node.SourceThreadID || meta.ForkedFromEntryID != node.SourceEntryID {
				continue
			}
			if index == 0 {
				switch {
				case meta.ParentThreadID == "":
					if err := validateSchema12RootAuthority(meta); err != nil {
						return nil, err
					}
				case isSchema12UnpatchedForkDestination(meta, node):
					if _, err := tx.ExecContext(ctx, `UPDATE threads SET parent_thread_id = '', parent_turn_id = '' WHERE id = ?`, node.DestinationThreadID); err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("schema v12 root fork destination %q has ambiguous authority", node.DestinationThreadID)
				}
			} else {
				patch := node.MetadataPatch
				switch {
				case schema12DestinationAuthorityMatches(meta, patch):
				case isSchema12UnpatchedForkDestination(meta, node):
					if _, err := tx.ExecContext(ctx, `UPDATE threads SET
						parent_thread_id = ?, parent_turn_id = ?, task_name = ?, task_description = ?,
						agent_path = ?, host_profile_ref = ?, fork_mode = ?, closed = ?, status = ?
						WHERE id = ?`,
						strings.TrimSpace(patch.ParentThreadID), strings.TrimSpace(patch.ParentTurnID), strings.TrimSpace(patch.TaskName), strings.TrimSpace(patch.TaskDescription),
						strings.TrimSpace(patch.AgentPath), strings.TrimSpace(patch.HostProfileRef), strings.TrimSpace(patch.ForkMode), boolInt(patch.Closed), strings.TrimSpace(patch.Status),
						node.DestinationThreadID); err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("schema v12 terminal child %q has ambiguous authority", node.DestinationThreadID)
				}
			}
			handled[node.DestinationThreadID] = struct{}{}
		}
		planJSON, err := json.Marshal(upgradeSchema12ForkPlan(plan))
		if err != nil {
			return nil, err
		}
		resultJSON, err := upgradeSchema12ForkResult(rawResult, state, operationID, plan.Root.DestinationThreadID)
		if err != nil {
			return nil, fmt.Errorf("upgrade schema v12 fork operation %q result: %w", operationID, err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fork_operations SET plan_json = ?, result_json = ? WHERE operation_id = ?`, string(planJSON), resultJSON, operationID); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return handled, nil
}

func schema12DestinationAuthorityMatches(meta schema12ThreadAuthority, destination *sessiontree.ForkDestinationMeta) bool {
	if destination == nil {
		return false
	}
	return meta.ParentThreadID == strings.TrimSpace(destination.ParentThreadID) &&
		meta.ParentTurnID == strings.TrimSpace(destination.ParentTurnID) &&
		meta.TaskName == strings.TrimSpace(destination.TaskName) &&
		meta.TaskDescription == strings.TrimSpace(destination.TaskDescription) &&
		meta.AgentPath == strings.TrimSpace(destination.AgentPath) &&
		meta.HostProfileRef == strings.TrimSpace(destination.HostProfileRef) &&
		meta.ForkMode == strings.TrimSpace(destination.ForkMode)
}

func isSchema12UnpatchedForkDestination(meta schema12ThreadAuthority, node schema12ForkPlanNode) bool {
	return meta.ParentThreadID == node.SourceThreadID &&
		meta.ParentTurnID == "" &&
		meta.TaskName == "" &&
		meta.TaskDescription == "" &&
		meta.AgentPath == "" &&
		meta.HostProfileRef == "" &&
		meta.ForkMode == "" &&
		!meta.Closed &&
		meta.Status == ""
}

func upgradeSchema12ForkPlan(plan schema12ForkPlan) schema13ForkPlan {
	convert := func(node schema12ForkPlanNode) schema13ForkPlanNode {
		return schema13ForkPlanNode{
			NodeID:              node.NodeID,
			SourceThreadID:      node.SourceThreadID,
			SourceEntryID:       node.SourceEntryID,
			DestinationThreadID: node.DestinationThreadID,
			TurnIDMap:           node.TurnIDMap,
			RunIDMap:            node.RunIDMap,
			DestinationMeta:     node.MetadataPatch,
		}
	}
	children := make([]schema13ForkPlanNode, 0, len(plan.TerminalChildren))
	for _, child := range plan.TerminalChildren {
		children = append(children, convert(child))
	}
	return schema13ForkPlan{
		Version:            2,
		OperationID:        plan.OperationID,
		RequestFingerprint: plan.RequestFingerprint,
		PreparedAt:         plan.PreparedAt,
		Root:               convert(plan.Root),
		TerminalChildren:   children,
	}
}

func upgradeSchema12ForkResult(raw, state, operationID, destinationThreadID string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		if state == "completed" {
			return "", errors.New("completed fork operation result is empty")
		}
		return "", nil
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	var result map[string]json.RawMessage
	if err := decoder.Decode(&result); err != nil {
		return "", err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return "", errors.New("fork result contains trailing JSON value")
		}
		return "", err
	}
	for key := range result {
		if key != "operation_id" && key != "thread" && key != "turns" {
			return "", fmt.Errorf("fork result contains unknown field %q", key)
		}
	}
	if len(result["operation_id"]) == 0 || len(result["thread"]) == 0 {
		return "", errors.New("fork result identity is incomplete")
	}
	var resultOperationID string
	if err := json.Unmarshal(result["operation_id"], &resultOperationID); err != nil {
		return "", err
	}
	var summary struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(result["thread"], &summary); err != nil {
		return "", err
	}
	if strings.TrimSpace(resultOperationID) != operationID || strings.TrimSpace(summary.ID) != destinationThreadID {
		return "", errors.New("fork result identity conflicts with persisted plan")
	}
	delete(result, "turns")
	upgraded, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(upgraded), nil
}

func decodeSchema12ForkPlan(raw string) (schema12ForkPlan, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var plan schema12ForkPlan
	if err := decoder.Decode(&plan); err != nil {
		return schema12ForkPlan{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return schema12ForkPlan{}, fmt.Errorf("fork plan contains trailing JSON value")
		}
		return schema12ForkPlan{}, err
	}
	return plan, nil
}

func loadSchema12ThreadAuthority(ctx context.Context, q sqlRunner, threadID string) (schema12ThreadAuthority, bool, error) {
	row := q.QueryRowContext(ctx, `SELECT id, parent_thread_id, parent_turn_id,
		forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, status
		FROM threads WHERE id = ?`, threadID)
	meta, err := scanSchema12ThreadAuthority(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return schema12ThreadAuthority{}, false, nil
		}
		return schema12ThreadAuthority{}, false, err
	}
	return meta, true, nil
}

func scanSchema12ThreadAuthority(scanner rowScanner) (schema12ThreadAuthority, error) {
	var meta schema12ThreadAuthority
	var closed int
	if err := scanner.Scan(
		&meta.ID, &meta.ParentThreadID, &meta.ParentTurnID,
		&meta.ForkedFromThreadID, &meta.ForkedFromEntryID, &meta.ForkOperationID, &meta.ForkOperationNodeID,
		&meta.TaskName, &meta.TaskDescription, &meta.AgentPath, &meta.HostProfileRef, &meta.ForkMode, &closed, &meta.Status,
	); err != nil {
		return schema12ThreadAuthority{}, err
	}
	meta.Closed = closed != 0
	return meta, nil
}

func validateSchema12RootAuthority(meta schema12ThreadAuthority) error {
	if meta.ParentTurnID != "" || meta.TaskName != "" || meta.TaskDescription != "" || meta.AgentPath != "" || meta.HostProfileRef != "" || meta.ForkMode != "" || meta.Closed || meta.Status != "" {
		return fmt.Errorf("schema v12 root thread %q contains subagent ownership metadata", meta.ID)
	}
	return nil
}
