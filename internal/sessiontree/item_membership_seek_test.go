package sessiontree

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestMembershipRelativeSeekMatchesCanonicalOrder(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 300)
	initial, err := repo.ItemMembershipWindow(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 10, Direction: PathWindowAfter})
	if err != nil || len(initial.Members) < 10 {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	for _, offset := range []int{5, -5} {
		anchor, want := initial.Members[1].ID, initial.Members[6].ID
		if offset < 0 {
			anchor, want = want, anchor
		}
		page, err := repo.ItemMembershipWindow(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", AnchorID: anchor, AnchorOffset: offset, Direction: PathWindowAround, Limit: 1})
		if err != nil || len(page.Members) != 1 || page.Members[0].ID != want {
			t.Fatalf("offset=%d page=%+v want=%s err=%v", offset, page, want, err)
		}
	}
}

func TestRelativeMembershipRejectsMixedSelection(t *testing.T) {
	_, backend := projectionSourcesFixture(t, 10)
	for _, repo := range []ItemMembershipReader{backend, NewMemoryRepo()} {
		_, err := repo.ItemMembershipWindow(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, ItemIDs: []string{"item"}, AnchorID: "anchor", AnchorOffset: 1, Direction: PathWindowAround})
		if !errors.Is(err, spi.ErrInvalidArgument) {
			t.Fatalf("mixed selection error = %v", err)
		}
	}
}
