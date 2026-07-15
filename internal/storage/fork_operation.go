package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

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
	State              ForkOperationState
	Plan               json.RawMessage
	Result             json.RawMessage
	ErrorCode          string
	ErrorMessage       string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	FinishedAt         time.Time
}

type ForkOperationStore interface {
	PrepareForkOperation(context.Context, ForkOperationRecord) (ForkOperationRecord, bool, error)
	ForkOperation(context.Context, string) (ForkOperationRecord, error)
	UpdateForkOperation(context.Context, ForkOperationRecord) error
}

type MemoryForkOperationStore struct {
	mu      sync.Mutex
	records map[string]ForkOperationRecord
}

func NewMemoryForkOperationStore() *MemoryForkOperationStore {
	return &MemoryForkOperationStore{records: map[string]ForkOperationRecord{}}
}

func (s *MemoryForkOperationStore) PrepareForkOperation(_ context.Context, rec ForkOperationRecord) (ForkOperationRecord, bool, error) {
	if err := ValidatePreparedForkOperation(rec); err != nil {
		return ForkOperationRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[rec.OperationID]; ok {
		return cloneForkOperationRecord(existing), false, nil
	}
	s.records[rec.OperationID] = cloneForkOperationRecord(rec)
	return cloneForkOperationRecord(rec), true, nil
}

func (s *MemoryForkOperationStore) ForkOperation(_ context.Context, operationID string) (ForkOperationRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[strings.TrimSpace(operationID)]
	if !ok {
		return ForkOperationRecord{}, ErrForkOperationNotFound
	}
	return cloneForkOperationRecord(rec), nil
}

func (s *MemoryForkOperationStore) UpdateForkOperation(_ context.Context, rec ForkOperationRecord) error {
	if err := ValidateForkOperationRecord(rec); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.records[rec.OperationID]
	if !ok {
		return ErrForkOperationNotFound
	}
	if existing.RequestFingerprint != rec.RequestFingerprint || !jsonEqual(existing.Plan, rec.Plan) {
		return ErrForkOperationConflict
	}
	if existing.State != ForkOperationPrepared && !forkOperationRecordsEqual(existing, rec) {
		return ErrForkOperationConflict
	}
	s.records[rec.OperationID] = cloneForkOperationRecord(rec)
	return nil
}

func ValidatePreparedForkOperation(rec ForkOperationRecord) error {
	if err := ValidateForkOperationRecord(rec); err != nil {
		return err
	}
	if rec.State != ForkOperationPrepared || len(rec.Result) != 0 || rec.ErrorCode != "" || rec.ErrorMessage != "" || !rec.FinishedAt.IsZero() {
		return errors.New("prepared fork operation contains terminal outcome")
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
	if !rec.State.Valid() {
		return errors.New("fork operation state is invalid")
	}
	if len(rec.Plan) == 0 || !json.Valid(rec.Plan) {
		return errors.New("fork operation plan must be valid json")
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
	rec.Plan = append(json.RawMessage(nil), rec.Plan...)
	rec.Result = append(json.RawMessage(nil), rec.Result...)
	return rec
}

func forkOperationRecordsEqual(left, right ForkOperationRecord) bool {
	return left.OperationID == right.OperationID &&
		left.RequestFingerprint == right.RequestFingerprint &&
		left.State == right.State &&
		jsonEqual(left.Plan, right.Plan) &&
		jsonEqual(left.Result, right.Result) &&
		left.ErrorCode == right.ErrorCode &&
		left.ErrorMessage == right.ErrorMessage &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt) &&
		left.FinishedAt.Equal(right.FinishedAt)
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
