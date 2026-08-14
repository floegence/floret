package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/v4/internal/session/artifact"
	"github.com/floegence/floret/v4/internal/sessiontree"
)

const ForkOperationPlanVersion = 5

type ForkOperationPlan struct {
	Version            int                     `json:"version"`
	OperationID        string                  `json:"operation_id"`
	RequestFingerprint string                  `json:"request_fingerprint"`
	PreparedAt         time.Time               `json:"prepared_at"`
	Root               ForkOperationPlanNode   `json:"root"`
	TerminalChildren   []ForkOperationPlanNode `json:"terminal_children,omitempty"`
}

type ForkOperationPlanNode struct {
	NodeID              string                           `json:"node_id"`
	SourceThreadID      string                           `json:"source_thread_id"`
	SourceEntryID       string                           `json:"source_entry_id,omitempty"`
	SourceLeafEntryID   string                           `json:"source_leaf_entry_id,omitempty"`
	DestinationThreadID string                           `json:"destination_thread_id"`
	TurnIDMap           map[string]string                `json:"turn_id_map,omitempty"`
	RunIDMap            map[string]string                `json:"run_id_map,omitempty"`
	DestinationMeta     *sessiontree.ForkDestinationMeta `json:"destination_meta,omitempty"`
	ArtifactClosure     artifact.Closure                 `json:"artifact_closure"`
}

var (
	ErrForkOperationNotFound = errors.New("fork operation not found")
	ErrForkOperationConflict = errors.New("fork operation conflicts with existing record")
)

type ForkOperationState string

const (
	ForkOperationPrepared  ForkOperationState = "prepared"
	ForkOperationCompleted ForkOperationState = "completed"
	ForkOperationFailed    ForkOperationState = "failed"
)

func (s ForkOperationState) Valid() bool {
	switch s {
	case ForkOperationPrepared, ForkOperationCompleted, ForkOperationFailed:
		return true
	default:
		return false
	}
}

type ForkOperationRecord struct {
	OperationID        string
	RequestFingerprint string
	SourceThreadIDs    []string
	AuthorityThreadIDs []string
	State              ForkOperationState
	Plan               json.RawMessage
	Result             json.RawMessage
	ErrorCode          string
	ErrorMessage       string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	FinishedAt         time.Time
}

func ValidatePreparedForkOperation(rec ForkOperationRecord) error {
	if err := ValidateForkOperationRecord(rec); err != nil {
		return err
	}
	if rec.State != ForkOperationPrepared || len(rec.Result) != 0 || rec.ErrorCode != "" || rec.ErrorMessage != "" || !rec.FinishedAt.IsZero() {
		return errors.New("prepared fork operation contains terminal outcome")
	}
	plan, err := DecodeForkOperationPlan(rec.Plan)
	if err != nil {
		return err
	}
	if err := validateForkOperationPlanRecord(rec, plan); err != nil {
		return err
	}
	return nil
}

func DecodeForkOperationPlan(data json.RawMessage) (ForkOperationPlan, error) {
	var plan ForkOperationPlan
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return ForkOperationPlan{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return ForkOperationPlan{}, errors.New("unexpected trailing fork operation plan value")
		}
		return ForkOperationPlan{}, err
	}
	return plan, nil
}

func ValidateForkOperationPlan(plan ForkOperationPlan, operationID, fingerprint string) error {
	if plan.Version != ForkOperationPlanVersion ||
		strings.TrimSpace(plan.OperationID) != strings.TrimSpace(operationID) ||
		strings.TrimSpace(plan.RequestFingerprint) != strings.TrimSpace(fingerprint) ||
		plan.PreparedAt.IsZero() {
		return ErrForkOperationConflict
	}
	nodes := append([]ForkOperationPlanNode{plan.Root}, plan.TerminalChildren...)
	seenSources := make(map[string]struct{}, len(nodes))
	seenDestinations := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		sourceID := strings.TrimSpace(node.SourceThreadID)
		destinationID := strings.TrimSpace(node.DestinationThreadID)
		if strings.TrimSpace(node.NodeID) == "" || sourceID == "" || destinationID == "" {
			return ErrForkOperationConflict
		}
		if err := artifact.ValidateClosure(node.ArtifactClosure); err != nil ||
			node.ArtifactClosure.SourceThreadID != sourceID || node.ArtifactClosure.DestinationThreadID != destinationID {
			return ErrForkOperationConflict
		}
		if _, duplicate := seenSources[sourceID]; duplicate {
			return ErrForkOperationConflict
		}
		if _, duplicate := seenDestinations[destinationID]; duplicate {
			return ErrForkOperationConflict
		}
		seenSources[sourceID] = struct{}{}
		seenDestinations[destinationID] = struct{}{}
		if index == 0 {
			if node.NodeID != "root" || node.DestinationMeta != nil {
				return ErrForkOperationConflict
			}
			continue
		}
		meta := node.DestinationMeta
		if meta == nil || node.NodeID != fmt.Sprintf("terminal-child-%d", index) ||
			strings.TrimSpace(meta.ParentThreadID) != strings.TrimSpace(plan.Root.DestinationThreadID) ||
			strings.TrimSpace(meta.TaskName) == "" || strings.TrimSpace(meta.AgentPath) == "" {
			return ErrForkOperationConflict
		}
	}
	return nil
}

func ForkOperationPlanSourceThreadIDs(plan ForkOperationPlan) []string {
	nodes := append([]ForkOperationPlanNode{plan.Root}, plan.TerminalChildren...)
	values := make([]string, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, strings.TrimSpace(node.SourceThreadID))
	}
	return values
}

func ForkOperationPlanAuthorityThreadIDs(plan ForkOperationPlan) []string {
	nodes := append([]ForkOperationPlanNode{plan.Root}, plan.TerminalChildren...)
	values := make([]string, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, strings.TrimSpace(node.SourceThreadID), strings.TrimSpace(node.DestinationThreadID))
	}
	return values
}

func ForkOperationPlanNodes(plan ForkOperationPlan) []sessiontree.ForkOptions {
	planNodes := append([]ForkOperationPlanNode{plan.Root}, plan.TerminalChildren...)
	nodes := make([]sessiontree.ForkOptions, 0, len(planNodes))
	for _, node := range planNodes {
		originKey := strings.Join([]string{"legacy-fork", plan.OperationID, node.NodeID}, ":")
		nodes = append(nodes, sessiontree.ForkOptions{
			SourceThreadID: node.SourceThreadID, EntryID: node.SourceEntryID, EntryIDPinned: true,
			ExpectedSourceLeafID: node.SourceLeafEntryID, Position: sessiontree.ForkAt,
			NewThreadID: node.DestinationThreadID, OriginRequestKey: originKey,
			OriginFingerprint: sessiontree.StableHash(originKey + "\x00" + plan.RequestFingerprint),
			Now:               plan.PreparedAt, TurnIDMap: node.TurnIDMap, RunIDMap: node.RunIDMap, DestinationMeta: node.DestinationMeta,
			ArtifactClosure: artifact.CloneClosure(node.ArtifactClosure),
		})
	}
	return nodes
}

func ValidateForkOperationCommitNodes(plan ForkOperationPlan, nodes []sessiontree.ForkOptions) error {
	expected := ForkOperationPlanNodes(plan)
	if len(expected) != len(nodes) {
		return ErrForkOperationConflict
	}
	for index := range expected {
		left, right := expected[index], nodes[index]
		if left.SourceThreadID != right.SourceThreadID || left.EntryID != right.EntryID || left.EntryIDPinned != right.EntryIDPinned ||
			left.ExpectedSourceLeafID != right.ExpectedSourceLeafID || left.Position != right.Position || left.NewThreadID != right.NewThreadID ||
			left.OriginRequestKey != right.OriginRequestKey || left.OriginFingerprint != right.OriginFingerprint || !left.Now.Equal(right.Now) ||
			!reflect.DeepEqual(left.TurnIDMap, right.TurnIDMap) || !reflect.DeepEqual(left.RunIDMap, right.RunIDMap) ||
			!reflect.DeepEqual(left.DestinationMeta, right.DestinationMeta) || !artifact.EqualClosure(left.ArtifactClosure, right.ArtifactClosure) {
			return ErrForkOperationConflict
		}
	}
	return nil
}

func validateForkOperationPlanRecord(rec ForkOperationRecord, plan ForkOperationPlan) error {
	if err := ValidateForkOperationPlan(plan, rec.OperationID, rec.RequestFingerprint); err != nil {
		return err
	}
	if !exactStringSlicesEqual(rec.SourceThreadIDs, ForkOperationPlanSourceThreadIDs(plan)) ||
		!exactStringSlicesEqual(rec.AuthorityThreadIDs, ForkOperationPlanAuthorityThreadIDs(plan)) {
		return ErrForkOperationConflict
	}
	return nil
}

func ValidateForkOperationRecord(rec ForkOperationRecord) error {
	if strings.TrimSpace(rec.OperationID) == "" {
		return errors.New("fork operation id is required")
	}
	if strings.TrimSpace(rec.RequestFingerprint) == "" {
		return errors.New("fork operation request fingerprint is required")
	}
	cleanSources := cleanForkOperationIDs(rec.SourceThreadIDs)
	cleanAuthority := cleanForkOperationIDs(rec.AuthorityThreadIDs)
	if len(cleanSources) == 0 {
		return errors.New("fork operation source thread ids are required")
	}
	if len(cleanAuthority) == 0 {
		return errors.New("fork operation authority thread ids are required")
	}
	if !exactStringSlicesEqual(rec.SourceThreadIDs, cleanSources) || !exactStringSlicesEqual(rec.AuthorityThreadIDs, cleanAuthority) {
		return errors.New("fork operation thread ids must be trimmed and unique")
	}
	authoritySet := make(map[string]struct{}, len(cleanAuthority))
	for _, threadID := range cleanAuthority {
		authoritySet[threadID] = struct{}{}
	}
	for _, threadID := range cleanSources {
		if _, ok := authoritySet[threadID]; !ok {
			return errors.New("fork operation authority thread ids must include every source")
		}
	}
	if !rec.State.Valid() {
		return errors.New("fork operation state is invalid")
	}
	if len(rec.Plan) == 0 || !json.Valid(rec.Plan) {
		return errors.New("fork operation plan must be valid json")
	}
	plan, err := DecodeForkOperationPlan(rec.Plan)
	if err != nil {
		return err
	}
	if err := validateForkOperationPlanRecord(rec, plan); err != nil {
		return err
	}
	if rec.CreatedAt.IsZero() || rec.UpdatedAt.IsZero() {
		return errors.New("fork operation timestamps are required")
	}
	switch rec.State {
	case ForkOperationPrepared:
		if len(rec.Result) != 0 || rec.ErrorCode != "" || rec.ErrorMessage != "" || !rec.FinishedAt.IsZero() {
			return errors.New("prepared fork operation contains terminal outcome")
		}
	case ForkOperationCompleted:
		if len(rec.Result) == 0 || !json.Valid(rec.Result) || rec.ErrorCode != "" || rec.ErrorMessage != "" || rec.FinishedAt.IsZero() {
			return errors.New("completed fork operation outcome is invalid")
		}
	case ForkOperationFailed:
		if len(rec.Result) != 0 || strings.TrimSpace(rec.ErrorCode) == "" || strings.TrimSpace(rec.ErrorMessage) == "" || rec.FinishedAt.IsZero() {
			return errors.New("failed fork operation outcome is invalid")
		}
	}
	return nil
}

func cloneForkOperationRecord(rec ForkOperationRecord) ForkOperationRecord {
	rec.SourceThreadIDs = append([]string(nil), rec.SourceThreadIDs...)
	rec.AuthorityThreadIDs = append([]string(nil), rec.AuthorityThreadIDs...)
	rec.Plan = append(json.RawMessage(nil), rec.Plan...)
	rec.Result = append(json.RawMessage(nil), rec.Result...)
	return rec
}

func forkOperationRecordsEqual(left, right ForkOperationRecord) bool {
	return left.OperationID == right.OperationID &&
		left.RequestFingerprint == right.RequestFingerprint &&
		stringSlicesEqual(left.SourceThreadIDs, right.SourceThreadIDs) &&
		stringSlicesEqual(left.AuthorityThreadIDs, right.AuthorityThreadIDs) &&
		left.State == right.State &&
		jsonEqual(left.Plan, right.Plan) &&
		jsonEqual(left.Result, right.Result) &&
		left.ErrorCode == right.ErrorCode &&
		left.ErrorMessage == right.ErrorMessage &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt) &&
		left.FinishedAt.Equal(right.FinishedAt)
}

func cleanForkOperationIDs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func stringSlicesEqual(left, right []string) bool {
	left = cleanForkOperationIDs(left)
	right = cleanForkOperationIDs(right)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func exactStringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func jsonEqual(left, right json.RawMessage) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right)
	}
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftJSON, _ := json.Marshal(leftValue)
	rightJSON, _ := json.Marshal(rightValue)
	return string(leftJSON) == string(rightJSON)
}
