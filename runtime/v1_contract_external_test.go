package runtime_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v2/config"
	floretruntime "github.com/floegence/floret/v2/runtime"
)

func TestProviderHostOptionFamiliesRequireScopedConstructors(t *testing.T) {
	cfg := config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done", SystemPrompt: "test"}
	if _, err := floretruntime.NewThreadCompactionHostOptions(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := floretruntime.NewSubAgentHostOptions(cfg, floretruntime.WithSubAgentRunTimeout(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := floretruntime.NewThreadCompactionHostOptions(cfg, floretruntime.ThreadCompactionOption{}); err == nil {
		t.Fatal("zero thread compaction option was accepted")
	}
	if _, err := floretruntime.NewSubAgentHostOptions(cfg, floretruntime.SubAgentOption{}); err == nil {
		t.Fatal("zero SubAgent option was accepted")
	}
	if _, err := floretruntime.NewThreadCompactionHostOptions(
		cfg,
		floretruntime.WithThreadCompactionLoopLimits(floretruntime.LoopLimits{NoProgressLimit: 1}),
		floretruntime.WithThreadCompactionLoopLimits(floretruntime.LoopLimits{NoProgressLimit: 2}),
	); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate compaction category error = %v", err)
	}
	if _, err := floretruntime.NewSubAgentHostOptions(
		cfg,
		floretruntime.WithSubAgentRunTimeout(time.Second),
		floretruntime.WithSubAgentRunTimeout(2*time.Second),
	); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate SubAgent category error = %v", err)
	}
	if _, err := floretruntime.NewThreadCompactionHostOptions(cfg, floretruntime.WithThreadCompactionModelGateway(nil, floretruntime.ModelGatewayIdentity{}, floretruntime.ModelGatewayCapabilities{})); err == nil {
		t.Fatal("nil compaction gateway was accepted")
	}
	if _, err := floretruntime.NewSubAgentHostOptions(cfg, floretruntime.WithSubAgentDynamicToolSurface(nil)); err == nil {
		t.Fatal("nil SubAgent dynamic tool surface was accepted")
	}
}

func TestV1RootDTOValidatorsRejectUnknownAndContradictoryState(t *testing.T) {
	now := time.Now().UTC()
	snapshot := floretruntime.SubAgentSnapshot{
		ThreadID: "child", ParentThreadID: "root", Path: "/child", TaskName: "child",
		ForkMode: floretruntime.SubAgentForkNone, Status: floretruntime.SubAgentStatusIdle,
		CreatedAt: now, UpdatedAt: now, CanSendInput: true, CanClose: true,
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
	snapshot.Status = floretruntime.SubAgentStatus("unknown")
	if err := snapshot.Validate(); err == nil {
		t.Fatal("unknown SubAgent status was accepted")
	}
	if err := (floretruntime.WaitSubAgentsResult{Snapshots: []floretruntime.SubAgentSnapshot{snapshot}}).Validate(); err == nil {
		t.Fatal("invalid SubAgent wait item was accepted")
	}
	if err := (floretruntime.ThreadAgentTodoState{ThreadID: "thread", Version: 1}).Validate(); err == nil {
		t.Fatal("versioned todo state without update identity was accepted")
	}
	if err := (floretruntime.ThreadDetailEvents{GeneratedAt: now}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (floretruntime.SQLiteStoreInspection{}).Validate(); err == nil {
		t.Fatal("zero Store inspection was accepted")
	}
	if err := (floretruntime.ForkThreadResult{}).Validate(); err == nil {
		t.Fatal("zero fork result was accepted")
	}
	if err := (floretruntime.RecoverInterruptedTurnResult{}).Validate(); err == nil {
		t.Fatal("zero interrupted recovery result was accepted")
	}
	if err := (floretruntime.PendingToolSettlementTarget{}).Validate(); err == nil {
		t.Fatal("zero pending tool settlement target was accepted")
	}
	if err := (floretruntime.ArtifactContent{}).Validate(); err == nil {
		t.Fatal("zero artifact content was accepted")
	}
}

func TestContractErrorSupportsErrorsIsAndErrorsAs(t *testing.T) {
	cause := errors.New("invalid projection")
	err := &floretruntime.ContractError{Contract: "turn projection", Err: cause}
	var contract *floretruntime.ContractError
	if !errors.Is(err, floretruntime.ErrAuthorityCorrupt) || !errors.Is(err, cause) || !errors.As(err, &contract) {
		t.Fatalf("contract classification failed: %v", err)
	}
	if contract.Contract != "turn projection" {
		t.Fatalf("contract = %q", contract.Contract)
	}
}
