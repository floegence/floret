package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryInterruptedTurnRecoveryRejectsCorruptResolvedFinish(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*MemoryRepo, string)
	}{
		{
			name: "missing terminal",
			mutate: func(repo *MemoryRepo, key string) {
				finish := repo.turnFinishes[key]
				finish.TerminalEntryID = "missing-terminal"
				repo.turnFinishes[key] = finish
			},
		},
		{
			name: "admission run drift",
			mutate: func(repo *MemoryRepo, key string) {
				admission := repo.turnAdmissions[key]
				admission.RunID = "different-run"
				repo.turnAdmissions[key] = admission
			},
		},
		{
			name: "turn started reference drift",
			mutate: func(repo *MemoryRepo, key string) {
				admission := repo.turnAdmissions[key]
				admission.TurnStartedID = "terminal"
				repo.turnAdmissions[key] = admission
			},
		},
		{
			name: "generation rollback",
			mutate: func(repo *MemoryRepo, _ string) {
				repo.leaseGeneration["thread"] = 0
			},
		},
		{
			name: "active finished generation",
			mutate: func(repo *MemoryRepo, key string) {
				repo.leases["thread"] = repo.turnAdmissions[key].Lease
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
				Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted,
				OutcomeFingerprint: "finish", Now: now.Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(repo, turnAdmissionKey("thread", "turn"))

			request := RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}
			if err := repo.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
			}
			if _, err := repo.RecoverInterruptedTurn(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

func TestInterruptedTurnRecoveryFingerprintIncludesCompleteEffectFacts(t *testing.T) {
	lease := TurnLease{
		ThreadID: "thread", Purpose: TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner", Generation: 1,
		AcquiredAt: time.Date(2026, 7, 21, 2, 30, 0, 0, time.UTC), RenewedAt: time.Date(2026, 7, 21, 2, 30, 0, 0, time.UTC),
		ExpiresAt: time.Date(2026, 7, 21, 2, 31, 0, 0, time.UTC),
	}
	prepared := InterruptedTurnRecoveryEffect{
		EffectAttemptID: "effect-1", ToolCallID: "call-1", State: EffectAttemptPrepared,
	}
	dispatching := InterruptedTurnRecoveryEffect{
		EffectAttemptID: "effect-2", ToolCallID: "call-2", State: EffectAttemptDispatching,
	}
	base, err := InterruptedTurnRecoveryFingerprint(
		lease, "", "run", TurnFailed, TurnFailureEffectOutcomeUnknown, InterruptedTurnEffectOutcomeUnknownMessage,
		nil, []InterruptedTurnRecoveryEffect{prepared, dispatching},
	)
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := InterruptedTurnRecoveryFingerprint(
		lease, "", "run", TurnFailed, TurnFailureEffectOutcomeUnknown, InterruptedTurnEffectOutcomeUnknownMessage,
		nil, []InterruptedTurnRecoveryEffect{dispatching, prepared},
	)
	if err != nil {
		t.Fatal(err)
	}
	if reordered != base {
		t.Fatalf("effect fact order changed fingerprint: base=%s reordered=%s", base, reordered)
	}
	dispatching.State = EffectAttemptPrepared
	changed, err := InterruptedTurnRecoveryFingerprint(
		lease, "", "run", TurnFailed, TurnFailureEffectOutcomeUnknown, InterruptedTurnEffectOutcomeUnknownMessage,
		nil, []InterruptedTurnRecoveryEffect{prepared, dispatching},
	)
	if err != nil {
		t.Fatal(err)
	}
	if changed == base {
		t.Fatal("effect state change did not change recovery fingerprint")
	}
}

func TestInterruptedTurnRecoveryFingerprintIncludesExistingFailureFacts(t *testing.T) {
	lease := TurnLease{
		ThreadID: "thread", Purpose: TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner", Generation: 1,
		AcquiredAt: time.Date(2026, 7, 21, 2, 45, 0, 0, time.UTC), RenewedAt: time.Date(2026, 7, 21, 2, 45, 0, 0, time.UTC),
		ExpiresAt: time.Date(2026, 7, 21, 2, 46, 0, 0, time.UTC),
	}
	proof := InterruptedTurnRecoveryFailureProof{EntryID: "failure", Message: "provider failed", RawHash: StableHash("raw")}
	base, err := InterruptedTurnRecoveryFingerprint(lease, "", "run", TurnFailed, TurnFailureProvider, "", &proof, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*InterruptedTurnRecoveryFailureProof)
	}{
		{name: "entry id", mutate: func(value *InterruptedTurnRecoveryFailureProof) { value.EntryID = "other-failure" }},
		{name: "message", mutate: func(value *InterruptedTurnRecoveryFailureProof) { value.Message = "other message" }},
		{name: "raw hash", mutate: func(value *InterruptedTurnRecoveryFailureProof) { value.RawHash = StableHash("other raw") }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			changedProof := proof
			testCase.mutate(&changedProof)
			changed, err := InterruptedTurnRecoveryFingerprint(lease, "", "run", TurnFailed, TurnFailureProvider, "", &changedProof, nil)
			if err != nil {
				t.Fatal(err)
			}
			if changed == base {
				t.Fatalf("%s change did not change recovery fingerprint", testCase.name)
			}
		})
	}
}

func TestMemoryInterruptedTurnRecoveryBindsExistingFailureProofForTypedAndLegacyFailures(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		metadata map[string]string
		wantCode string
	}{
		{name: "typed", metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureProvider}, wantCode: TurnFailureProvider},
		{name: "legacy", wantCode: TurnFailureLegacyUnclassified},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			current := time.Date(2026, time.July, 21, 9, 0, 0, 0, time.UTC)
			repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
			})
			if err != nil {
				t.Fatal(err)
			}
			failure, err := repo.Append(ContextWithTurnLease(ctx, admitted.Lease), Entry{
				ThreadID: "thread", TurnID: "turn", Type: EntryRunFailure,
				Error: "canonical provider failure", Metadata: testCase.metadata,
			}, AppendOptions{Now: current.Add(time.Second)})
			if err != nil {
				t.Fatal(err)
			}
			meta, err := repo.Thread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			path, err := repo.Path(ctx, "thread", meta.LeafID)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := DeriveInterruptedTurnRecoveryPlan(path, admitted.Lease, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			if plan.FailureCode != testCase.wantCode || plan.FailureMessage != "" || plan.SourceFailure == nil ||
				plan.SourceFailure.EntryID != failure.ID || plan.SourceFailure.Message != failure.Error || plan.SourceFailure.RawHash != failure.RawHash {
				t.Fatalf("plan=%#v failure=%#v", plan, failure)
			}
			current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
			request := RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: current}
			recovered, err := repo.RecoverInterruptedTurn(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Status != TurnFailed || recovered.Failure != nil || recovered.Terminal.ParentID != failure.ID ||
				recovered.Terminal.Metadata[TurnFailureCodeMetadataKey] != testCase.wantCode ||
				recovered.Terminal.Metadata[InterruptedTurnRecoverySourceFailureEntryKey] != failure.ID ||
				recovered.Terminal.Metadata[InterruptedTurnRecoverySourceFailureRawHashKey] != failure.RawHash {
				t.Fatalf("recovered=%#v failure=%#v", recovered, failure)
			}
			replayed, err := repo.RecoverInterruptedTurn(ctx, request)
			if err != nil || !replayed.Replayed || replayed.OutcomeFingerprint != recovered.OutcomeFingerprint ||
				replayed.Terminal.Metadata[InterruptedTurnRecoverySourceFailureEntryKey] != failure.ID {
				t.Fatalf("replayed=%#v err=%v", replayed, err)
			}
		})
	}
}

func TestMemoryInterruptedTurnRecoveryKeepsUnknownEffectPriorityWhileBindingExistingFailure(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, time.July, 21, 9, 15, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
	})
	if err != nil {
		t.Fatal(err)
	}
	failure, err := repo.Append(ContextWithTurnLease(ctx, admitted.Lease), Entry{
		ThreadID: "thread", TurnID: "turn", Type: EntryRunFailure, Error: "provider failure",
		Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureProvider},
	}, AppendOptions{Now: current.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
		Lease: admitted.Lease,
		Invocation: EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "tool", ArgumentHash: "arguments",
		},
		RequestFingerprint: "effect", Now: current.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID,
		RequestFingerprint: "effect", ObservedHeartbeat: admitted.Lease.Heartbeat,
		AuthorizationProofHash: "proof", Now: current.Add(3 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	recovered, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: current})
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != TurnFailed || recovered.Failure == nil || recovered.Failure.Error != InterruptedTurnEffectOutcomeUnknownMessage ||
		recovered.Terminal.Metadata[TurnFailureCodeMetadataKey] != TurnFailureEffectOutcomeUnknown ||
		recovered.Terminal.Metadata[InterruptedTurnRecoverySourceFailureEntryKey] != failure.ID ||
		recovered.Terminal.Metadata[InterruptedTurnRecoverySourceFailureRawHashKey] != failure.RawHash {
		t.Fatalf("recovered=%#v failure=%#v", recovered, failure)
	}
}

func TestMemoryInterruptedTurnRecoveryReplayRejectsExistingFailureTamper(t *testing.T) {
	for _, failureCase := range []struct {
		name     string
		metadata map[string]string
	}{
		{name: "typed", metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureProvider}},
		{name: "legacy"},
	} {
		for _, tamperCase := range []struct {
			name   string
			mutate func(*MemoryRepo, Entry, Entry)
		}{
			{name: "id", mutate: func(repo *MemoryRepo, failure, terminal Entry) {
				entries := repo.entries["thread"]
				for index := range entries {
					switch entries[index].ID {
					case failure.ID:
						entries[index].ID = "tampered-failure-id"
					case terminal.ID:
						entries[index].ParentID = "tampered-failure-id"
					}
				}
				repo.entries["thread"] = entries
				delete(repo.entryOrdinals["thread"], failure.ID)
				repo.entryOrdinals["thread"]["tampered-failure-id"] = repo.entryOrdinals["thread"][terminal.ID] - 1
				repo.entryDepths["thread"]["tampered-failure-id"] = failure.PathDepth
			}},
			{name: "canonical ancestry", mutate: func(repo *MemoryRepo, failure, terminal Entry) {
				entries := repo.entries["thread"]
				for index := range entries {
					if entries[index].ID == terminal.ID {
						entries[index].ParentID = failure.ParentID
						break
					}
				}
				repo.entries["thread"] = entries
			}},
			{name: "message", mutate: func(repo *MemoryRepo, failure, _ Entry) {
				mutateMemoryRecoveryFailure(repo, failure.ID, func(entry *Entry) { entry.Error = "tampered message" })
			}},
			{name: "raw", mutate: func(repo *MemoryRepo, failure, _ Entry) {
				mutateMemoryRecoveryFailure(repo, failure.ID, func(entry *Entry) { entry.Raw = entry.Raw + " " })
			}},
			{name: "raw hash", mutate: func(repo *MemoryRepo, failure, _ Entry) {
				mutateMemoryRecoveryFailure(repo, failure.ID, func(entry *Entry) { entry.RawHash = StableHash("tampered") })
			}},
			{name: "self consistent raw and hash", mutate: func(repo *MemoryRepo, failure, _ Entry) {
				mutateMemoryRecoveryFailure(repo, failure.ID, func(entry *Entry) {
					entry.Metadata = cloneStringMap(entry.Metadata)
					if entry.Metadata == nil {
						entry.Metadata = map[string]string{}
					}
					entry.Metadata["tampered"] = "true"
					entry.Raw = rawForEntry(*entry)
					entry.RawHash = stableHash(entry.Raw)
				})
			}},
		} {
			t.Run(failureCase.name+"/"+tamperCase.name, func(t *testing.T) {
				ctx := context.Background()
				current := time.Date(2026, time.July, 21, 9, 30, 0, 0, time.UTC)
				repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
				if err != nil {
					t.Fatal(err)
				}
				if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
					t.Fatal(err)
				}
				admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
					ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
					Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
				})
				if err != nil {
					t.Fatal(err)
				}
				failure, err := repo.Append(ContextWithTurnLease(ctx, admitted.Lease), Entry{
					ThreadID: "thread", TurnID: "turn", Type: EntryRunFailure,
					Error: "canonical failure", Metadata: failureCase.metadata,
				}, AppendOptions{Now: current.Add(time.Second)})
				if err != nil {
					t.Fatal(err)
				}
				current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
				request := RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: current}
				recovered, err := repo.RecoverInterruptedTurn(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				if replayed, err := repo.RecoverInterruptedTurn(ctx, request); err != nil || !replayed.Replayed {
					t.Fatalf("clean replay=%#v err=%v", replayed, err)
				}
				tamperCase.mutate(repo, failure, recovered.Terminal)
				if err := repo.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
				}
				if _, err := repo.RecoverInterruptedTurn(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
				}
			})
		}
	}
}

func mutateMemoryRecoveryFailure(repo *MemoryRepo, failureID string, mutate func(*Entry)) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	entries := repo.entries["thread"]
	for index := range entries {
		if entries[index].ID == failureID {
			mutate(&entries[index])
			break
		}
	}
	repo.entries["thread"] = entries
}

func TestMemoryInterruptedTurnResolutionRejectsRecoveryFailureLinkDrift(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 45, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	now = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	recovered, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: now})
	if err != nil || recovered.Failure == nil {
		t.Fatalf("recovered=%#v err=%v", recovered, err)
	}
	entries := repo.entries["thread"]
	for index := range entries {
		if entries[index].ID != recovered.Terminal.ID {
			continue
		}
		entries[index].ParentID = admitted.TurnStarted.ID
		entries[index].Raw = rawForEntry(entries[index])
		entries[index].RawHash = stableHash(entries[index].Raw)
	}
	repo.entries["thread"] = entries
	if err := repo.ValidateInterruptedTurnResolution(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryInterruptedTurnResolutionReturnsDeletedForTombstone(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 50, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DeleteRootTree(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if err := repo.ValidateInterruptedTurnResolution(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrThreadDeleted", err)
	}
}

func TestMemoryInterruptedTurnRecoveryRejectsCorruptAdmissionBeforeMutation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 13, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*MemoryRepo, string)
	}{
		{name: "missing", mutate: func(repo *MemoryRepo, key string) { delete(repo.turnAdmissions, key) }},
		{name: "run drift", mutate: func(repo *MemoryRepo, key string) {
			admission := repo.turnAdmissions[key]
			admission.RunID = "different-run"
			repo.turnAdmissions[key] = admission
		}},
		{name: "lease drift", mutate: func(repo *MemoryRepo, key string) {
			admission := repo.turnAdmissions[key]
			admission.Lease.Heartbeat++
			repo.turnAdmissions[key] = admission
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			current := now
			repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
			})
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(repo, turnAdmissionKey("thread", "turn"))
			current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
			beforeEntries := len(repo.entries["thread"])
			beforeLease := repo.leases["thread"]
			beforeGeneration := repo.leaseGeneration["thread"]

			if _, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
			if !SameTurnLease(repo.leases["thread"], beforeLease) || repo.leaseGeneration["thread"] != beforeGeneration ||
				len(repo.entries["thread"]) != beforeEntries {
				t.Fatalf("corrupt admission mutated authority: lease=%#v generation=%d entries=%d", repo.leases["thread"], repo.leaseGeneration["thread"], len(repo.entries["thread"]))
			}
		})
	}
}

func TestMemoryInterruptedTurnRecoveryRejectsEffectRunDriftBeforeMutation(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, time.July, 20, 13, 15, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
		Lease: admitted.Lease,
		Invocation: EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "tool", ArgumentHash: "arguments",
		},
		RequestFingerprint: "effect-request", Now: current,
	})
	if err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	attempt := repo.effectAttempts[prepared.Attempt.EffectAttemptID]
	attempt.Invocation.RunID = "different-run"
	repo.effectAttempts[attempt.EffectAttemptID] = attempt
	beforeEntries := len(repo.entries["thread"])
	beforeLease := repo.leases["thread"]
	beforeGeneration := repo.leaseGeneration["thread"]
	repo.mu.Unlock()

	current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	if _, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if !SameTurnLease(repo.leases["thread"], beforeLease) || repo.leaseGeneration["thread"] != beforeGeneration ||
		len(repo.entries["thread"]) != beforeEntries || repo.effectAttempts[attempt.EffectAttemptID].State != EffectAttemptPrepared {
		t.Fatalf("effect run drift mutated authority: lease=%#v generation=%d entries=%d attempt=%#v",
			repo.leases["thread"], repo.leaseGeneration["thread"], len(repo.entries["thread"]), repo.effectAttempts[attempt.EffectAttemptID])
	}
}

func TestMemoryInterruptedTurnRecoveryReplayRejectsEffectRunDrift(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, time.July, 20, 13, 20, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
		Lease: admitted.Lease,
		Invocation: EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "tool", ArgumentHash: "arguments",
		},
		RequestFingerprint: "effect-request", Now: current,
	})
	if err != nil {
		t.Fatal(err)
	}
	current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	if _, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	attempt := repo.effectAttempts[prepared.Attempt.EffectAttemptID]
	attempt.Invocation.RunID = "different-run"
	repo.effectAttempts[attempt.EffectAttemptID] = attempt
	repo.mu.Unlock()

	request := RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}
	if err := repo.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := repo.RecoverInterruptedTurn(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryInterruptedTurnRecoveryReplayRejectsApprovalAuthorityDrift(t *testing.T) {
	initial := time.Date(2026, time.July, 20, 13, 25, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name      string
		submitted bool
		mutate    func(*MemoryRepo, ApprovalRecord)
	}{
		{name: "requested cancellation id", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			record.DecisionID = "different-cancellation"
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "requested approval state", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			record.State = ApprovalFailed
			record.Reason = ApprovalReasonAuthorizationUnavailable
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "queue current", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			queue := repo.approvalQueues[record.RootThreadID]
			queue.CurrentApprovalID = record.ApprovalID
			repo.approvalQueues[record.RootThreadID] = queue
		}},
		{name: "queue revision", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			queue := repo.approvalQueues[record.RootThreadID]
			queue.Revision = 2
			repo.approvalQueues[record.RootThreadID] = queue
		}},
		{name: "submitted receipt state", submitted: true, mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			decision := repo.approvalDecisions[record.DecisionID]
			decision.Receipt.State = ApprovalFailed
			decision.Receipt.Reason = ApprovalReasonAuthorizationUnavailable
			repo.approvalDecisions[record.DecisionID] = decision
		}},
		{name: "submitted receipt revision", submitted: true, mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			decision := repo.approvalDecisions[record.DecisionID]
			decision.Receipt.ApprovalRevision--
			repo.approvalDecisions[record.DecisionID] = decision
		}},
		{name: "submitted receipt timestamp", submitted: true, mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			decision := repo.approvalDecisions[record.DecisionID]
			decision.Receipt.ResolvedAt = decision.Receipt.SubmittedAt
			repo.approvalDecisions[record.DecisionID] = decision
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			current := initial
			repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: initial,
			})
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, initial))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			if testCase.submitted {
				resolved, err := repo.ResolveApproval(ctx, ResolveApprovalRequest{
					DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
					ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
					ExpectedApprovalRevision: record.Revision, Decision: ApprovalDecisionApprove, Now: initial.Add(time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				record = resolved.Approval
			}
			if testCase.name == "queue revision" {
				repo.mu.Lock()
				queue := repo.approvalQueues[record.RootThreadID]
				queue.Revision = 8
				repo.approvalQueues[record.RootThreadID] = queue
				repo.mu.Unlock()
			}
			current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
			if _, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: current}); err != nil {
				t.Fatal(err)
			}
			repo.mu.Lock()
			record = repo.approvals[record.ApprovalID]
			testCase.mutate(repo, record)
			repo.mu.Unlock()

			request := RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}
			if err := repo.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
			}
			if _, err := repo.RecoverInterruptedTurn(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

func memoryInterruptedApprovalPrepare(lease TurnLease, now time.Time) PrepareApprovalBatchRequest {
	item := ApprovalPreflightItem{
		EffectRequestFingerprint: "effect-call", ApprovalRequestFingerprint: "approval-call",
		Invocation: EffectInvocationIdentity{
			ThreadID: lease.ThreadID, TurnID: lease.TurnID, RunID: "run", ToolCallID: "call", ToolName: "write_file", ArgumentHash: "args",
		},
		ToolKind: "local", Step: 1, BatchSize: 1, Resources: []ApprovalResource{{Kind: "file", Value: "notes.md"}}, Effects: []string{"write"}, Destructive: true,
	}
	item.EffectAttemptID = ApprovalEffectAttemptID(item.Invocation)
	item.RequestedEntry = Entry{
		ID: ApprovalRequestedEntryID(item.EffectAttemptID), ThreadID: lease.ThreadID, TurnID: lease.TurnID,
		Type: EntryCustom, Metadata: map[string]string{"approval_state": "requested"},
	}
	return PrepareApprovalBatchRequest{Lease: lease, Now: now, Items: []ApprovalPreflightItem{item}}
}
