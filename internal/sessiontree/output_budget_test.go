package sessiontree

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestOutputBudgetSharesForkPrefixesAndReclaimsOnDeletion(t *testing.T) {
	backend := newMigrationTestBackend()
	ctx := t.Context()
	apply := func(owner, source string, from, to int64, want bool) {
		t.Helper()
		if err := backend.Update(ctx, func(tx spi.WriteTx) error {
			allowed, err := changeOutputCharge(tx, outputCharge{owner, source, to}, from)
			if err == nil && allowed != want {
				t.Fatalf("admitted %t, want %t", allowed, want)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	usage := func(want int64) {
		t.Helper()
		if err := backend.View(ctx, func(tx spi.ReadTx) error {
			got, err := outputStoreUsage(tx)
			if err == nil && got != want {
				t.Fatalf("charged %d, want %d", got, want)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	remove := func(owner string) {
		t.Helper()
		if err := backend.Update(ctx, func(tx spi.WriteTx) error { return releaseThreadOutputCharges(ctx, tx, owner) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return putOutputRecord(tx, outputUsageKey(), outputStoreMaxBytes-10) }); err != nil {
		t.Fatal(err)
	}
	apply("original", "source", 0, 10, true)
	apply("fork", "source", 0, 4, true)
	apply("deeper-fork", "source", 0, 7, true)
	apply("other", "different", 0, 1, false)
	usage(outputStoreMaxBytes)
	apply("original", "source", 10, 9, true)
	usage(outputStoreMaxBytes - 1)
	apply("original", "source", 9, 6, true)
	usage(outputStoreMaxBytes - 3)
	remove("original")
	usage(outputStoreMaxBytes - 3)
	apply("other", "different", 0, 3, true)
	apply("fork", "source", 4, 7, true)
	remove("deeper-fork")
	usage(outputStoreMaxBytes)
	remove("fork")
	usage(outputStoreMaxBytes - 7)
	remove("other")
	usage(outputStoreMaxBytes - 10)
	failure := errors.New("injected after accounting")
	err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if ok, err := changeOutputCharge(tx, outputCharge{"rolled-back", "other", 9}, 0); err != nil || !ok {
			t.Fatalf("injection: %t %v", ok, err)
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	usage(outputStoreMaxBytes - 10)
}
