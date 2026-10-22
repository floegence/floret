package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

func canonicalPendingParentEvents(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, limit int) ([]ParentEvent, error) {
	if meta.ParentThreadID == "" {
		return nil, nil
	}
	if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, meta.ParentThreadID); errors.Is(err, ErrThreadDeleted) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	// Recovery follows journal order, including interleaved turns. Turn-key
	// ordering is unrelated to the order in which the child asked for input.
	prefix := backendDomainKey(backendDomainRecordEntry, meta.ID)
	var after []byte
	var result []ParentEvent
	var pendingTurn string
	var pending map[string]bool
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainV32Namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return nil, err
		}
		for _, row := range page.Records {
			record, err := decodeBackendDomainRecord(backendDomainV32Format, row.Value)
			if err != nil || record.ThreadID != meta.ID || !bytes.Equal(row.Key, backendDomainEntryKey(meta.ID, record.Ordinal)) {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			stored, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
			if err != nil {
				return nil, err
			}
			entry := stored.Entry
			entry.bodyFields = stored.Fields
			id := ""
			switch entry.Type {
			case EntryInteractionAsked:
				var ok bool
				id, ok = strings.CutPrefix(entry.ID, "interaction-requested:")
				if !ok {
					continue
				}
			case EntryToolCall:
				if entry.Message.ControlSignal != nil {
					id = entry.Message.ControlSignal.CallID
				}
			case EntryToolResult:
				id = ToolInputInteractionID(entry.ID)
			}
			if id == "" {
				continue
			}
			if pending == nil || pendingTurn != entry.TurnID {
				entries, err := canonicalRuntimeTurnEntries(ctx, tx, meta.ID, entry.TurnID)
				if err != nil {
					return nil, err
				}
				pendingTurn, pending = entry.TurnID, map[string]bool{}
				for _, interaction := range runtimePendingInteractionsIn(entries, entry.TurnID) {
					pending[interaction.ID] = true
				}
			}
			if !pending[id] {
				continue
			}
			event, found, err := canonicalAutomaticParentEvent(ctx, tx, meta, entry)
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}
			if _, err := readCanonicalEntryMetadata(tx, event.ParentThreadID, event.ID); errors.Is(err, ErrEntryNotFound) {
				result = append(result, event)
				delete(pending, id)
				if len(result) == limit {
					return result, nil
				}
			} else if err != nil {
				return nil, err
			}
		}
		if !page.HasMore {
			return result, nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return nil, ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}

func (repo *BackendRepo) BootstrapParentEvents(ctx context.Context) error {
	if repo == nil || ctx == nil {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.BootstrapParentEvents(ctx)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return repo.update(ctx, func(memory *MemoryRepo) error { return memory.BootstrapParentEvents(ctx) })
	}
	if !repo.parentEventsEnabled() {
		return nil
	}
	prefix := backendDomainKey(backendDomainRecordThread)
	var after []byte
	for {
		var page spi.ScanPage
		err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
			var err error
			page, err = tx.Scan(spi.ScanRequest{Namespace: backendDomainV32Namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: 1})
			return err
		})
		if err != nil || len(page.Records) == 0 {
			return err
		}
		record, err := decodeBackendDomainRecord(backendDomainV32Format, page.Records[0].Value)
		if err != nil || record.Kind != backendDomainRecordThread || !bytes.Equal(page.Records[0].Key, backendDomainKey(backendDomainRecordThread, record.ID)) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		for {
			reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: backendDomainV32Format, metadata: map[string]migrationMetadataRead{}}
			changed, err := repo.prepareParentEvents(ctx, reader, record.ID)
			if errors.Is(err, spi.ErrConflict) {
				continue
			}
			if err != nil {
				return err
			}
			if !changed {
				break
			}
		}
		after = bytes.Clone(page.Records[0].Key)
	}
}
