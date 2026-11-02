package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"

	"github.com/floegence/floret/v7/internal/session/artifact"
)

func (r *MemoryRepo) HostWorks(ctx context.Context, id string) ([]HostWork, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[id]
	if !ok {
		return nil, ErrThreadNotFound
	}
	path, err := r.canonicalPathLocked(ctx, id, meta.LeafID)
	if err != nil {
		return nil, err
	}
	works, _, err := hostWorksFromPath(path)
	return works, err
}

func (r *MemoryRepo) StopHostWork(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[id]
	if !ok {
		return ErrThreadNotFound
	}
	path, err := r.canonicalPathLocked(ctx, id, meta.LeafID)
	if err != nil {
		return err
	}
	entries, err := hostWorkStops(path, id)
	if err != nil {
		return err
	}
	checkpoint := r.checkpointJournalAppendLocked(id)
	for _, entry := range entries {
		if _, err := r.appendFactLocked(ctx, entry, AppendOptions{}, true); err != nil {
			checkpoint.restore(r)
			return err
		}
	}
	return nil
}

func (r *BackendRepo) StopHostWork(ctx context.Context, id string) error {
	if err := r.prepareSparseSources(ctx, id); err != nil {
		return err
	}
	return r.canonicalJournalUpdate(ctx, id, func(m *canonicalJournalMutation) error {
		path, err := hostWorkPath(m)
		if err != nil {
			return err
		}
		entries, err := hostWorkStops(path, id)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if _, err := m.appendFact(entry, AppendOptions{}, true); err != nil {
				return err
			}
		}
		return nil
	}, func(memory *MemoryRepo) error { return memory.StopHostWork(ctx, id) })
}
func (r *MemoryRepo) BindHostWork(ctx context.Context, w HostWork) (HostWork, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[w.Invocation.ThreadID]
	if !ok {
		return HostWork{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return HostWork{}, err
	}
	path, err := r.canonicalPathLocked(ctx, meta.ID, meta.LeafID)
	if err != nil {
		return HostWork{}, err
	}
	if r.turnCancellationRequestedLocked(meta.ID, w.Invocation.TurnID) {
		return HostWork{}, context.Canceled
	}
	entry, err := bindHostWorkEntry(w, path)
	if err != nil {
		return HostWork{}, err
	}
	_, err = r.appendLocked(ctx, entry, AppendOptions{ID: entry.ID})
	return w, err
}
func (r *MemoryRepo) SettleHostWork(ctx context.Context, req HostWorkSettlement) (HostWork, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[req.ThreadID]
	if !ok {
		return HostWork{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return HostWork{}, err
	}
	path, err := r.canonicalPathLocked(ctx, meta.ID, meta.LeafID)
	if err != nil {
		return HostWork{}, err
	}
	entry, w, replayed, err := settleHostWorkEntry(req, path)
	if err != nil || replayed {
		return w, err
	}
	entry, err = r.appendFactLocked(ctx, entry, AppendOptions{ID: entry.ID}, true)
	if err != nil {
		return HostWork{}, err
	}
	if w.Output != nil {
		r.setArtifactLocked(artifactRecordKey(meta.ID, w.Output.ID), artifact.Record{ThreadID: meta.ID, Ref: *w.Output, Text: req.Output.Text, CanonicalEntryID: entry.ID, CreatedAt: entry.CreatedAt})
	}
	return w, nil
}
func (r *MemoryRepo) AdmitHostWork(ctx context.Context, thread, turn, run string) (Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[thread]
	if !ok {
		return Entry{}, ErrThreadNotFound
	}
	active, live := r.runtimeActiveTurnLocked(thread)
	id, err := r.activeRunIdentityLocked(thread, turn)
	if err != nil || !live || active != turn || id != run || r.turnCancellationRequestedLocked(thread, turn) {
		return Entry{}, ErrStaleAuthority
	}
	path, err := r.canonicalPathLocked(ctx, thread, meta.LeafID)
	if err != nil {
		return Entry{}, err
	}
	entry, err := hostWorkAdmission(path, thread, turn, run)
	if err != nil || entry.ID == "" {
		return entry, err
	}
	return r.appendLocked(ctx, entry, AppendOptions{ID: entry.ID})
}

func hostWorkPath(m *canonicalJournalMutation) ([]Entry, error) {
	return canonicalHostWorkPath(m.ctx, m.tx, m.meta)
}

func canonicalHostWorkPath(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) ([]Entry, error) {
	var path []Entry
	if meta.LeafID == "" {
		return path, nil
	}
	leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
	if err != nil {
		return nil, err
	}
	err = visitSparseSources(ctx, tx, meta.ID, "host_work", func(e Entry) error {
		loc, err := readEntryLocator(tx, meta.ID, e.ID)
		if err != nil {
			return err
		}
		if loc.Depth > leaf.Depth {
			return nil
		}
		ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, loc.Depth)
		if err != nil {
			return err
		}
		if ancestor.ID == e.ID {
			if e.Type == EntryHostWorkAdmitted || e.Type == EntryHostWork {
				e, err = materializeCanonicalEntry(canonicalStoredEntry{Entry: e, Fields: e.bodyFields}, func(ref canonicalBodyRef) (string, error) {
					if ref.Bytes > HostWorkOutputMaxBytes {
						return "", ErrAuthorityCorrupt
					}
					return materializeCanonicalBody(ctx, tx, ref)
				})
				if err != nil {
					return err
				}
			}
			path = append(path, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	works, _, err := hostWorksFromPath(path)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, w := range works {
		if seen[w.Invocation.TurnID] {
			continue
		}
		seen[w.Invocation.TurnID] = true
		turn, err := readTurnEntryLocator(tx, meta.ID, w.Invocation.TurnID)
		if err != nil {
			return nil, err
		}
		if turn.Cancellation != "" {
			e, err := readCanonicalEntryMetadata(tx, meta.ID, turn.Cancellation)
			if err != nil {
				return nil, err
			}
			path = append(path, e)
		}
	}
	active, live, err := canonicalActiveTurn(tx, meta.ID)
	if err != nil {
		return nil, err
	}
	if live {
		path = append(path, Entry{Type: EntryTurnMarker, TurnID: active, TurnStatus: TurnStarted})
	}
	return path, nil
}
func (r *BackendRepo) HostWorks(ctx context.Context, id string) ([]HostWork, error) {
	if err := r.prepareSparseSources(ctx, id); err != nil {
		return nil, err
	}
	var works []HostWork
	err := r.canonicalThreadView(ctx, id, func(tx spi.ReadTx, meta ThreadMeta) error {
		path, err := canonicalHostWorkPath(ctx, tx, meta)
		if err != nil {
			return err
		}
		works, _, err = hostWorksFromPath(path)
		return err
	}, func(memory *MemoryRepo) error { var err error; works, err = memory.HostWorks(ctx, id); return err })
	return works, err
}
func (r *BackendRepo) BindHostWork(ctx context.Context, w HostWork) (HostWork, error) {
	if err := r.prepareSparseSources(ctx, w.Invocation.ThreadID); err != nil {
		return HostWork{}, err
	}
	err := r.canonicalJournalUpdate(ctx, w.Invocation.ThreadID, func(m *canonicalJournalMutation) error {
		if err := lifecycleRejectsWrite(m.meta); err != nil {
			return err
		}
		cancelled, err := canonicalTurnAuthority(m.tx, m.meta.ID, w.Invocation.TurnID, w.Invocation.RunID)
		if err != nil {
			return err
		}
		if cancelled {
			return context.Canceled
		}
		path, err := hostWorkPath(m)
		if err != nil {
			return err
		}
		attempt, err := readCanonicalEffectAttempt(m.tx, m.meta.ID, effectAttemptID(w.Invocation))
		if err != nil {
			return err
		}
		attemptEntry, err := effectAttemptEntry(attempt, "")
		if err != nil {
			return err
		}
		path = append(path, attemptEntry)
		entry, err := bindHostWorkEntry(w, path)
		if err != nil {
			return err
		}
		_, err = m.append(entry, AppendOptions{ID: entry.ID})
		return err
	}, func(memory *MemoryRepo) error { _, err := memory.BindHostWork(ctx, w); return err })
	return w, err
}
func (r *BackendRepo) SettleHostWork(ctx context.Context, req HostWorkSettlement) (HostWork, error) {
	if err := r.prepareSparseSources(ctx, req.ThreadID); err != nil {
		return HostWork{}, err
	}
	var result HostWork
	err := r.canonicalJournalUpdate(ctx, req.ThreadID, func(m *canonicalJournalMutation) error {
		if err := lifecycleRejectsWrite(m.meta); err != nil {
			return err
		}
		path, err := hostWorkPath(m)
		if err != nil {
			return err
		}
		entry, w, replayed, err := settleHostWorkEntry(req, path)
		if err != nil {
			return err
		}
		result = w
		if replayed {
			return nil
		}
		entry, err = m.appendFact(entry, AppendOptions{ID: entry.ID}, true)
		if err != nil {
			return err
		}
		if w.Output != nil {
			key := artifactRecordKey(m.meta.ID, w.Output.ID)
			record := artifact.Record{ThreadID: m.meta.ID, Ref: *w.Output, Text: req.Output.Text, CanonicalEntryID: entry.ID, CreatedAt: entry.CreatedAt}
			if err := putBackendDomainRecordContext(ctx, m.tx, backendDomainV32Format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0, record); err != nil {
				return errors.Join(ErrHostWorkOutputStorage, err)
			}
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.SettleHostWork(ctx, req)
		return err
	})
	return result, err
}
func (r *BackendRepo) AdmitHostWork(ctx context.Context, thread, turn, run string) (Entry, error) {
	if err := r.prepareSparseSources(ctx, thread); err != nil {
		return Entry{}, err
	}
	var result Entry
	err := r.canonicalJournalUpdate(ctx, thread, func(m *canonicalJournalMutation) error {
		cancelled, err := canonicalTurnAuthority(m.tx, thread, turn, run)
		if err != nil {
			return err
		}
		if cancelled {
			return ErrStaleAuthority
		}
		path, err := hostWorkPath(m)
		if err != nil {
			return err
		}
		entry, err := hostWorkAdmission(path, thread, turn, run)
		if err != nil || entry.ID == "" {
			return err
		}
		result, err = m.append(entry, AppendOptions{ID: entry.ID})
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.AdmitHostWork(ctx, thread, turn, run)
		return err
	})
	return result, err
}

func validateHostWorkEntry(entry Entry) error {
	if entry.Type == EntryHostWork {
		_, err := decodeHostWork(entry)
		return err
	}
	if entry.Type == EntryHostWorkAdmitted {
		var ids []string
		if json.Unmarshal(entry.Payload, &ids) != nil || len(ids) == 0 || entry.Message.Kind != "host_work" || entry.Message.Role != "user" || entry.ThreadID == "" || entry.TurnID == "" || entry.RunID == "" || entry.Message.Content == "" || len(entry.Message.Content) > 8192 {
			return ErrAuthorityCorrupt
		}
		raw, _ := json.Marshal(ids)
		if entry.ID != "host-work-admitted:"+StableHash(string(raw))[:32] {
			return ErrAuthorityCorrupt
		}
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if id == "" || seen[id] {
				return ErrAuthorityCorrupt
			}
			seen[id] = true
		}
	}
	return nil
}

func (r *MemoryRepo) HostWorkContinuationKey(ctx context.Context, id string) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[id]
	if !ok {
		return "", false, ErrThreadNotFound
	}
	path, err := r.canonicalPathLocked(ctx, id, meta.LeafID)
	if err != nil {
		return "", false, err
	}
	return HostWorkContinuation(path)
}
func (r *BackendRepo) HostWorkContinuationKey(ctx context.Context, id string) (string, bool, error) {
	if err := r.prepareSparseSources(ctx, id); err != nil {
		return "", false, err
	}
	var key string
	var eligible bool
	err := r.canonicalThreadView(ctx, id, func(tx spi.ReadTx, meta ThreadMeta) error {
		path, err := canonicalHostWorkPath(ctx, tx, meta)
		if err != nil {
			return err
		}
		key, eligible, err = HostWorkContinuation(path)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		key, eligible, err = memory.HostWorkContinuationKey(ctx, id)
		return err
	})
	return key, eligible, err
}
