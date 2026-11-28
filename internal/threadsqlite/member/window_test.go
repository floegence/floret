package member

import (
	"database/sql"
	"errors"
	"slices"
	"testing"
)

func TestMemberReadMemoEndsWithSnapshot(t *testing.T) {
	file := newTestMember(t, "thread", 1)
	if err := file.transaction(t.Context(), func(journal *Journal) error {
		if _, err := journal.AppendEntry(EntryEvent{ID: "root", Depth: 1, Kind: "user_message", Payload: []byte(`{}`)}); err != nil {
			return err
		}
		end := journal.RestrictToReads()
		if err := journal.Sources(SourceSelection{}, 0, func(event EntryEvent) (bool, error) {
			event.Payload[0] = 'x'
			return false, nil
		}); err != nil {
			return err
		}
		byID, err := journal.Entry("root")
		if err != nil || string(byID.Payload) != `{}` {
			t.Fatalf("scan result shared with memo: %q, %v", byID.Payload, err)
		}
		byID.Payload[0] = 'y'
		byNumber, err := journal.EntryNumber(0)
		if err != nil || string(byNumber.Payload) != `{}` {
			t.Fatalf("keyed result shared with memo: %q, %v", byNumber.Payload, err)
		}
		end()
		if _, err := journal.Entry("root"); !errors.Is(err, sql.ErrTxDone) {
			t.Fatalf("retained cached identity: %v", err)
		}
		if _, err := journal.EntryNumber(0); !errors.Is(err, sql.ErrTxDone) {
			t.Fatalf("retained cached ordinal: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBranchedAncestryUsesOnlyCanonicalParents(t *testing.T) {
	file := newTestMember(t, "thread", 1)
	err := file.transaction(t.Context(), func(journal *Journal) error {
		paths := [][]string{{"root"}, {"root", "left"}, {"root", "left", "left3"}, {"root", "right"}, {"root", "right", "right3"}, {"root", "left", "left3", "left4"}, {"new"}, {"new", "child"}, {"root", "right", "right3", "right4"}}
		for index, path := range paths {
			id := path[len(path)-1]
			parent := ""
			if len(path) > 1 {
				parent = path[len(path)-2]
			}
			if _, err := journal.AppendEntry(EntryEvent{ID: id, ParentID: parent, Depth: int64(len(path)), Number: int64(index), Kind: "user_message", Payload: []byte(`{}`)}); err != nil {
				return err
			}
		}
		for _, path := range paths {
			id := path[len(path)-1]
			for after := 0; after <= len(path)+1; after++ {
				var got []string
				if err := journal.VisitPathAfter(id, int64(after), func(event EntryEvent) error { got = append(got, event.ID); return nil }); err != nil {
					return err
				}
				if !slices.Equal(got, path[min(after, len(path)):]) {
					t.Fatalf("%s after %d: %v", id, after, got)
				}
				if after < len(path) {
					ancestor, err := journal.Ancestor(id, int64(after+1))
					if err != nil || ancestor.ID != path[after] {
						t.Fatalf("%s at %d: %v, %v", id, after+1, ancestor.ID, err)
					}
				}
			}
		}
		if _, err := journal.tx.Exec(`UPDATE history_events SET path_depth=4 WHERE event_id='right3'`); err != nil {
			return err
		}
		if err := journal.VisitPathAfter("right4", 0, func(EntryEvent) error { return nil }); !errors.Is(err, ErrContentIntegrity) {
			t.Fatalf("invalid depth: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCompactionAnchorUsesVisibilityAtObservation(t *testing.T) {
	file := newTestMember(t, "thread", 1)
	if err := file.transaction(t.Context(), func(journal *Journal) error {
		ids := []string{"question", "reserve", "compact", "reveal", "settle"}
		parent := ""
		for ordinal, id := range ids {
			_, err := journal.AppendEntry(EntryEvent{ID: id, ParentID: parent, Number: int64(ordinal), Depth: int64(ordinal + 1), Kind: "user_message", Payload: []byte(`{}`)})
			if err != nil {
				return err
			}
			parent = id
		}
		for _, item := range []WindowItem{{ID: "question", First: "question", Last: "settle", Role: "tool", Summary: []byte(`{}`)}, {ID: "input", First: "reserve", Last: "reveal", Role: "interaction", Summary: []byte(`{}`)}} {
			if err := journal.PutWindow(item, true); err != nil {
				return err
			}
		}
		for depth, want := range map[int]string{1: "", 3: "question", 5: "input"} {
			got, err := journal.WindowAnchorBefore(depth)
			if err != nil || got != want {
				t.Fatalf("depth %d: got %q, want %q (%v)", depth, got, want, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
