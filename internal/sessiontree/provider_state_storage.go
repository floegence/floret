package sessiontree

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func currentProviderState(tx spi.ReadTx, thread string) (ProviderStateRecord, bool, error) {
	var state ProviderStateRecord
	var raw []byte
	if journal := member.From(tx); journal != nil {
		if journal.Thread() != thread {
			return state, false, ErrInvalidThreadAuthority
		}
		content, found, err := journal.Continuation()
		if err != nil || !found {
			return state, false, err
		}
		raw = content
	} else {
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordProviderState, thread), backendDomainRecordProviderState, thread, "", 0)
		if err != nil || !found {
			return state, false, err
		}
		raw = record.Value
	}
	if err := storagecodec.DecodeJSON(raw, &state); err != nil || state.ThreadID != thread || validateProviderStateRecord(state) != nil {
		return ProviderStateRecord{}, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	return state, true, nil
}

func storeProviderState(ctx context.Context, tx spi.WriteTx, state ProviderStateRecord) error {
	if journal := member.From(tx); journal != nil {
		if journal.Thread() != state.ThreadID {
			return ErrInvalidThreadAuthority
		}
		if err := validateProviderStateRecord(state); err != nil {
			return err
		}
		raw, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return journal.StoreContinuation(state.UpdatedAt.UnixNano(), raw)
	}
	return putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordProviderState, state.ThreadID), backendDomainRecordProviderState, state.ThreadID, "", 0, state)
}

func clearProviderState(tx spi.WriteTx, thread string) error {
	if journal := member.From(tx); journal != nil {
		if journal.Thread() != thread {
			return ErrInvalidThreadAuthority
		}
		return journal.ClearContinuation()
	}
	err := tx.Delete(backendDomainV32Namespace, backendDomainKey(backendDomainRecordProviderState, thread))
	if errors.Is(err, spi.ErrNotFound) {
		return nil
	}
	return err
}
