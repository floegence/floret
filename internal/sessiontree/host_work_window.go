package sessiontree

import (
	"cmp"
	"encoding/json"
	"slices"
	"sort"

	"github.com/floegence/floret/v7/storage/spi"
)

// A window contains only unobserved work identities in binding order. The
// journal remains authoritative for notice text and every admission decision.
type workAdmissionWindow struct {
	leaf     string
	sequence int
	bound    map[string]windowWorkBinding
	ready    []string
}

type windowWorkBinding struct {
	order int
	stop  HostWorkExecution
}

func stopWorkExecution(work HostWork) HostWorkExecution {
	if !work.Inherited && (work.State == "bound" || work.State == "completed" && work.Notify) {
		return HostWorkExecution{work.Invocation.TurnID, work.Invocation.RunID}
	}
	return HostWorkExecution{}
}

func (window *workAdmissionWindow) stopSources(tx spi.ReadTx, thread string) ([]HostWorkExecution, error) {
	var bindings []windowWorkBinding
	for _, binding := range window.bound {
		if binding.stop.TurnID != "" {
			bindings = append(bindings, binding)
		}
	}
	slices.SortFunc(bindings, func(first, second windowWorkBinding) int { return cmp.Compare(first.order, second.order) })
	var sources []HostWorkExecution
	seen := map[HostWorkExecution]bool{}
	for _, binding := range bindings {
		if seen[binding.stop] {
			continue
		}
		seen[binding.stop] = true
		turn, err := readTurnEntryLocator(tx, thread, binding.stop.TurnID)
		if err != nil {
			return nil, err
		}
		if turn.Cancellation == "" {
			sources = append(sources, binding.stop)
		} else if entry, err := readCanonicalEntryMetadata(tx, thread, turn.Cancellation); err != nil || entry.Type != EntryCancelRequested || entry.TurnID != binding.stop.TurnID {
			if err != nil {
				return nil, err
			}
			return nil, ErrAuthorityCorrupt
		}
	}
	return sources, nil
}

func admissionWindowFromWorks(leaf string, works []HostWork, admitted map[string]bool) *workAdmissionWindow {
	window := &workAdmissionWindow{leaf: leaf, bound: make(map[string]windowWorkBinding)}
	for _, work := range works {
		if admitted[work.ID] {
			continue
		}
		window.sequence++
		window.bound[work.ID] = windowWorkBinding{window.sequence, stopWorkExecution(work)}
		if work.State != "bound" {
			window.ready = append(window.ready, work.ID)
		}
	}
	return window
}

func (window *workAdmissionWindow) settle(id string) {
	order, found := window.bound[id]
	if !found {
		return
	}
	position := sort.Search(len(window.ready), func(index int) bool { return window.bound[window.ready[index]].order >= order.order })
	window.ready = append(window.ready, "")
	copy(window.ready[position+1:], window.ready[position:])
	window.ready[position] = id
}

func (window *workAdmissionWindow) remove(id string) {
	order, exists := window.bound[id]
	if !exists {
		return
	}
	index := sort.Search(len(window.ready), func(index int) bool { return window.bound[window.ready[index]].order >= order.order })
	if index < len(window.ready) && window.ready[index] == id {
		copy(window.ready[index:], window.ready[index+1:])
		window.ready[len(window.ready)-1] = ""
		window.ready = window.ready[:len(window.ready)-1]
	}
	delete(window.bound, id)
}

func (mutation *canonicalJournalMutation) nextHostWorkNotice(turn, run string) (Entry, error) {
	thread := mutation.meta.ID
	window := mutation.repo.workAdmissions[thread]
	if window == nil || window.leaf != mutation.meta.LeafID {
		path, err := canonicalHostWorkEntries(mutation.ctx, mutation.tx, mutation.meta)
		if err != nil {
			return Entry{}, err
		}
		works, admitted, err := hostWorksFromPath(path)
		if err != nil {
			return Entry{}, err
		}
		window = admissionWindowFromWorks(mutation.meta.LeafID, works, admitted)
		mutation.workWindow = window
		mutation.validatedWork = true
		return hostWorkAdmissionForWorks(works, admitted, thread, turn, run)
	}
	var works []HostWork
	size := 0
	for _, id := range window.ready {
		entry, err := hostWorkOnBranch(mutation.ctx, mutation.tx, thread, mutation.meta.LeafID, id, true)
		if err != nil {
			return Entry{}, err
		}
		work, err := decodeHostWork(entry)
		if err != nil {
			return Entry{}, err
		}
		works = append(works, work)
		size += len(hostWorkNotice(work))
		if size >= 8192 {
			break
		}
	}
	mutation.validatedWork = true
	return hostWorkAdmissionForWorks(works, nil, thread, turn, run)
}

// Called under the repository lock only after the storage transaction commits.
func (repo *BackendRepo) advanceWorkAdmission(mutation *canonicalJournalMutation) {
	thread := mutation.meta.ID
	window := repo.workAdmissions[thread]
	if mutation.workWindow != nil {
		window = mutation.workWindow
	}
	if window == nil {
		return
	}
	for _, entry := range mutation.entries {
		if entry.ParentID != window.leaf {
			delete(repo.workAdmissions, thread)
			return
		}
		switch entry.Type {
		case EntryHostWork:
			work, err := decodeHostWork(entry)
			if !mutation.validatedWork || err != nil {
				delete(repo.workAdmissions, thread)
				return
			}
			if work.State == "bound" {
				window.sequence++
				window.bound[work.ID] = windowWorkBinding{window.sequence, stopWorkExecution(work)}
			} else {
				if binding, found := window.bound[work.ID]; found {
					binding.stop = stopWorkExecution(work)
					window.bound[work.ID] = binding
				}
				window.settle(work.ID)
			}
		case EntryToolResult:
			if result := entry.Message.ToolResult; result != nil && result.ObservedHostWork != "" {
				if _, found := window.bound[result.ObservedHostWork]; !found {
					delete(repo.workAdmissions, thread)
					return
				}
				window.remove(result.ObservedHostWork)
			}
		case EntryHostWorkAdmitted:
			var ids []string
			if !mutation.validatedWork || json.Unmarshal(entry.Payload, &ids) != nil {
				delete(repo.workAdmissions, thread)
				return
			}
			for _, id := range ids {
				window.remove(id)
			}
		}
		window.leaf = entry.ID
	}
	if window.leaf != mutation.meta.LeafID || len(window.bound) > 8192 {
		delete(repo.workAdmissions, thread)
		return
	}
	if mutation.workWindow == nil {
		return
	}
	repo.rememberWorkWindow(thread, window)
}

func (repo *BackendRepo) rememberWorkWindow(thread string, window *workAdmissionWindow) {
	if len(window.bound) > 8192 {
		return
	}
	if repo.workAdmissions == nil {
		repo.workAdmissions = make(map[string]*workAdmissionWindow)
	}
	if len(repo.workAdmissions) >= queryContinuationCacheLimit {
		for previous := range repo.workAdmissions {
			delete(repo.workAdmissions, previous)
			break
		}
	}
	repo.workAdmissions[thread] = window
}
