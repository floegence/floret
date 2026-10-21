package sessiontree

// journalAppendCheckpoint retains immutable slice headers and the few mutable
// facts owned by an append transaction. Rollback visits only its appended tail;
// historical payloads and index maps are never copied for error handling.
type journalAppendCheckpoint struct {
	threadID string
	journal  domainJournal
	meta     domainValue[ThreadMeta]
	provider domainValue[ProviderStateRecord]
	sequence int64
}

func (r *MemoryRepo) checkpointJournalAppendLocked(threadID string) journalAppendCheckpoint {
	meta, found := r.threads[threadID]
	provider, providerFound := r.providerStates[threadID]
	return journalAppendCheckpoint{threadID: threadID, journal: readDomainJournal(r, threadID), meta: domainValue[ThreadMeta]{meta, found}, provider: domainValue[ProviderStateRecord]{provider, providerFound}, sequence: r.seq}
}

func (checkpoint journalAppendCheckpoint) restore(r *MemoryRepo) {
	id, previous := checkpoint.threadID, checkpoint.journal
	current := r.entries[id]
	for _, entry := range current[len(previous.entries):] {
		delete(r.entryOrdinals[id], entry.ID)
		delete(r.entryDepths[id], entry.ID)
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			positions := r.turnRequests[id][entry.RequestKey]
			end := len(positions)
			for end > 0 && positions[end-1] >= len(previous.entries) {
				end--
			}
			if end == 0 {
				delete(r.turnRequests[id], entry.RequestKey)
			} else {
				r.turnRequests[id][entry.RequestKey] = positions[:end]
			}
		}
		for _, source := range []string{entry.Message.ContentSourceID, entry.Message.ReasoningSourceID} {
			locators := r.outputSources[id][source]
			end := len(locators)
			for end > 0 && locators[end-1] >= len(previous.entries) {
				end--
			}
			if end == 0 {
				delete(r.outputSources[id], source)
			} else {
				r.outputSources[id][source] = locators[:end]
			}
		}
		if entry.TurnID == "" {
			continue
		}
		marker := r.turnMarkers[id][entry.TurnID]
		endPositions := len(marker.positions)
		for endPositions > 0 && marker.positions[endPositions-1].through > len(previous.entries) {
			endPositions--
		}
		if endPositions == 0 {
			delete(r.turnMarkers[id], entry.TurnID)
		} else {
			marker.positions = marker.positions[:endPositions]
			marker.turnMarkerPosition = marker.positions[endPositions-1]
			r.turnMarkers[id][entry.TurnID] = marker
		}
		ordinals := r.turnEntryOrdinals[id][entry.TurnID]
		end := len(ordinals)
		for end > 0 && ordinals[end-1] >= len(previous.entries) {
			end--
		}
		if end == 0 {
			delete(r.turnEntryOrdinals[id], entry.TurnID)
			delete(r.turnEntryCounts[id], entry.TurnID)
		} else {
			r.turnEntryOrdinals[id][entry.TurnID] = ordinals[:end]
			r.turnEntryCounts[id][entry.TurnID] = end
		}
	}
	clear(current[len(previous.entries):])
	previous.install(r, id)
	if checkpoint.meta.found {
		r.setThreadLocked(id, checkpoint.meta.value)
	} else {
		r.deleteThreadLocked(id)
	}
	if checkpoint.provider.found {
		r.setProviderStateLocked(id, checkpoint.provider.value)
	} else {
		r.deleteProviderStateLocked(id)
	}
	r.seq = checkpoint.sequence
}
