package sessiontree

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

func canonicalQueueSource(entry Entry) (string, bool) {
	switch entry.Type {
	case EntryQueueAdded:
		var item struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(entry.Payload, &item) == nil {
			return item.ID, true
		}
	case EntryQueueDeleted, EntryQueuePromoted:
		var id string
		if json.Unmarshal(entry.Payload, &id) == nil {
			return id, true
		}
	}
	return "", false
}

func canonicalQueueContains(tx spi.ReadTx, threadID, queueID, beforeEntryID string) (bool, error) {
	sourceID := ""
	before := -1
	if beforeEntryID != "" {
		boundary, err := readEntryLocator(tx, threadID, beforeEntryID)
		if err != nil {
			return false, err
		}
		sourceID, before = boundary.QueuePrevious, boundary.Ordinal
	} else {
		raw, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("queue", threadID, queueID))
		if errors.Is(err, spi.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		sourceID = string(raw)
	}
	if sourceID == "" {
		return false, nil
	}
	node, err := readEntryLocator(tx, threadID, sourceID)
	if err != nil || before >= 0 && node.Ordinal >= before {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	entry, err := readLocatedEntry(tx, backendDomainV32Format, threadID, node)
	if err != nil {
		return false, err
	}
	if id, ok := canonicalQueueSource(entry); !ok || id != queueID {
		return false, ErrAuthorityCorrupt
	}
	return entry.Type == EntryQueueAdded, nil
}

func canonicalActiveTurn(tx spi.ReadTx, threadID string) (string, bool, error) {
	prefix := backendDomainKey("active_turn", threadID)
	page, err := tx.Scan(spi.ScanRequest{Namespace: backendEntryIndexNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 2})
	if err != nil {
		return "", false, err
	}
	if len(page.Records) != 1 || page.HasMore {
		return "", false, nil
	}
	row := page.Records[0]
	entry, err := readCanonicalEntryMetadata(tx, threadID, string(row.Value))
	if err != nil || entry.Type != EntryTurnMarker || entry.TurnID == "" || !bytes.Equal(row.Key, backendDomainKey("active_turn", threadID, entry.TurnID)) {
		return "", false, errors.Join(ErrAuthorityCorrupt, err)
	}
	switch entry.TurnStatus {
	case TurnStarted, TurnSavePoint, TurnWaiting:
		return entry.TurnID, true, nil
	default:
		return "", false, ErrAuthorityCorrupt
	}
}

func canonicalTurnAuthority(tx spi.ReadTx, threadID, turnID, runID string) (bool, error) {
	active, running, err := canonicalActiveTurn(tx, threadID)
	if err != nil {
		return false, err
	}
	if !running || active != turnID {
		return false, ErrStaleAuthority
	}
	turn, err := readTurnEntryLocator(tx, threadID, turnID)
	if err != nil {
		return false, err
	}
	if turn.Identity == "" {
		return false, ErrStaleAuthority
	}
	identity, err := readCanonicalEntryMetadata(tx, threadID, turn.Identity)
	if err != nil || identity.TurnID != turnID || identity.Type != EntryTurnMarker && identity.Type != EntryCancelRequested {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if strings.TrimSpace(identity.RunID) != runID {
		return false, ErrStaleAuthority
	}
	if turn.Cancellation != "" {
		cancel, err := readCanonicalEntryMetadata(tx, threadID, turn.Cancellation)
		if err != nil || cancel.Type != EntryCancelRequested || cancel.TurnID != turnID {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		return true, nil
	}
	return false, nil
}
