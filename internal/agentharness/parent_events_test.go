package agentharness

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

type parentAdmissionProbe struct {
	sessiontree.Repo
	queries            []sessiontree.ParentEventQuery
	pending            bool
	queryErr, admitErr error
	admissions         int
}

func (r *parentAdmissionProbe) ParentEvents(_ context.Context, q sessiontree.ParentEventQuery) (sessiontree.ParentEventPage, error) {
	r.queries = append(r.queries, q)
	if r.queryErr != nil {
		return sessiontree.ParentEventPage{}, r.queryErr
	}
	page := sessiontree.ParentEventPage{}
	if r.pending {
		page.Events = []sessiontree.ParentEventRecord{{ParentEvent: sessiontree.ParentEvent{ID: "receipt"}}}
	}
	return page, nil
}
func (r *parentAdmissionProbe) ReportToParent(context.Context, sessiontree.ParentReportRequest) (sessiontree.ParentEvent, error) {
	panic("unexpected report")
}
func (r *parentAdmissionProbe) AdmitParentEvents(_ context.Context, thread, turn, run string) (sessiontree.Entry, error) {
	if thread != "parent" || turn != "turn" || run != "run" {
		panic("wrong authority")
	}
	r.admissions++
	return sessiontree.Entry{ID: "admitted", ParentID: "prior", Message: session.Message{Role: session.User, Content: "canonical receipt"}}, r.admitErr
}
func TestParentAdmissionChecksBoundedPendingEvidenceAtEveryRequest(t *testing.T) {
	repo := &parentAdmissionProbe{}
	p := &turnProjection{thread: &Thread{id: "parent", harness: &AgentHarness{options: Options{Repo: repo}}}, turnID: "turn", runID: "run"}
	for range 3 {
		got, err := p.admitParentEvents(t.Context())
		if err != nil || got.EntryID != "" || repo.admissions != 0 {
			t.Fatalf("empty request entered full admission: %+v %v calls=%d", got, err, repo.admissions)
		}
	}
	repo.pending = true
	got, err := p.admitParentEvents(t.Context())
	if err != nil || got.EntryID != "admitted" || got.ParentEntryID != "prior" || got.Content != "canonical receipt" || repo.admissions != 1 {
		t.Fatalf("late receipt lost: %+v %v", got, err)
	}
	if len(repo.queries) != 4 {
		t.Fatalf("pending state was cached: %d", len(repo.queries))
	}
	for _, q := range repo.queries {
		if q.ThreadID != "parent" || !q.PendingOnly || q.Limit != 1 {
			t.Fatalf("unbounded query: %+v", q)
		}
	}
}
func TestParentAdmissionPreservesQueryAndAtomicAdmissionFailures(t *testing.T) {
	cause := errors.New("source unavailable")
	for _, queryFailure := range []bool{true, false} {
		repo := &parentAdmissionProbe{pending: true}
		if queryFailure {
			repo.queryErr = cause
		} else {
			repo.admitErr = cause
		}
		p := &turnProjection{thread: &Thread{id: "parent", harness: &AgentHarness{options: Options{Repo: repo}}}, turnID: "turn", runID: "run"}
		got, err := p.admitParentEvents(t.Context())
		if !errors.Is(err, cause) || got.EntryID != "" {
			t.Fatalf("failure lost: %+v %v", got, err)
		}
		if queryFailure && repo.admissions != 0 {
			t.Fatal("query failure authorized admission")
		}
	}
}
