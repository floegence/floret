package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInheritedTurnLeaseContextObservesHeartbeatRenewal(t *testing.T) {
	now := time.Now().UTC()
	initial := TurnLease{
		ThreadID: "thread", Purpose: TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner",
		Generation: 1, Heartbeat: 0, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	authorityCtx := ContextWithTurnLease(context.Background(), initial)
	dispatchCtx, err := ContextWithInheritedTurnLease(context.Background(), authorityCtx)
	if err != nil {
		t.Fatal(err)
	}
	renewed := initial
	renewed.Heartbeat++
	renewed.RenewedAt = now.Add(time.Second)
	renewed.ExpiresAt = now.Add(time.Minute + time.Second)
	if err := UpdateTurnLeaseContext(authorityCtx, initial, renewed); err != nil {
		t.Fatal(err)
	}
	got, ok := TurnLeaseFromContext(dispatchCtx)
	if !ok || !SameTurnLease(got, renewed) {
		t.Fatalf("inherited lease=%#v ok=%v, want renewed lease %#v", got, ok, renewed)
	}
}

func TestInheritedTurnLeaseContextRejectsMissingAuthority(t *testing.T) {
	if _, err := ContextWithInheritedTurnLease(context.Background(), context.Background()); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("inherit missing authority err=%v, want ErrStaleAuthority", err)
	}
}

func TestTurnLeaseSuccessorRejectsDifferentAuthorityLineage(t *testing.T) {
	now := time.Now().UTC()
	proof := TurnLease{
		ThreadID: "thread", Purpose: TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner",
		Generation: 1, Heartbeat: 1, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	current := proof
	current.Heartbeat++
	current.RenewedAt = now.Add(time.Second)
	current.ExpiresAt = now.Add(time.Minute + time.Second)
	if err := ValidateTurnLeaseSuccessor(proof, current); err != nil {
		t.Fatalf("valid heartbeat successor: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*TurnLease)
	}{
		{name: "thread", mutate: func(lease *TurnLease) { lease.ThreadID = "other" }},
		{name: "turn", mutate: func(lease *TurnLease) { lease.TurnID = "other" }},
		{name: "owner", mutate: func(lease *TurnLease) { lease.OwnerID = "other" }},
		{name: "generation", mutate: func(lease *TurnLease) { lease.Generation++ }},
		{name: "acquisition", mutate: func(lease *TurnLease) { lease.AcquiredAt = lease.AcquiredAt.Add(time.Nanosecond) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			drifted := current
			test.mutate(&drifted)
			if err := ValidateTurnLeaseSuccessor(proof, drifted); !errors.Is(err, ErrStaleAuthority) {
				t.Fatalf("drifted successor err=%v, want ErrStaleAuthority", err)
			}
		})
	}
}
