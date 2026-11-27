package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const backendDomainV43Version = 43

// Parent keys start with a tuple string; this ordinal cannot alias a thread.
var childMembershipStateKey = storagecodec.Tuple(storagecodec.TupleOrdinal(0))

type childMembershipState struct {
	Phase   string `json:"phase"`
	AfterID string `json:"after_id,omitempty"`
}

func readChildMembershipState(tx spi.ReadTx) (childMembershipState, error) {
	raw, err := tx.Get(childMembershipNamespace, childMembershipStateKey)
	if errors.Is(err, spi.ErrNotFound) {
		return childMembershipState{}, ErrAuthorityCorrupt
	}
	if err != nil {
		return childMembershipState{}, err
	}
	var s childMembershipState
	if err := storagecodec.DecodeJSON(raw, &s); err != nil || (s.Phase != "ready" && s.Phase != "backfill") || s.Phase == "ready" && s.AfterID != "" || strings.TrimSpace(s.AfterID) != s.AfterID {
		return childMembershipState{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return s, nil
}

func putChildMembershipState(tx spi.WriteTx, s childMembershipState) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return tx.Put(childMembershipNamespace, childMembershipStateKey, raw)
}

func initChildMembership(tx spi.WriteTx, phase string) error {
	found, err := backendNamespaceHasRecords(tx, childMembershipNamespace)
	if err != nil {
		return err
	}
	if found {
		return ErrAuthorityCorrupt
	}
	return putChildMembershipState(tx, childMembershipState{Phase: phase})
}

func migrateBackendDomainV42ToV43(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prefix := backendDomainKey(backendDomainRecordThread)
	page, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainV32Namespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 1})
	if err != nil {
		return err
	}
	phase := "backfill"
	if len(page.Records) == 0 {
		if page.HasMore {
			return ErrAuthorityCorrupt
		}
		phase = "ready"
	}
	if err := initChildMembership(tx, phase); err != nil {
		return err
	}
	return advanceUnchangedDomainRecords(ctx, tx, source, backendDomainV42Version, backendDomainV43Version)
}

// During backfill, the worker owns keys strictly after its committed cursor;
// foreground creation, fork, publication and deletion own the covered prefix.
// Both consult the same cursor in the transaction that changes canonical meta.
func updateChildMembership(tx spi.WriteTx, id, before, after string) error {
	if before == after {
		return nil
	}
	m, err := readV32Manifest(tx)
	if err != nil || m.Version < backendDomainV43Version {
		return err
	}
	s, err := readChildMembershipState(tx)
	if err != nil {
		return err
	}
	if s.Phase == "backfill" && (s.AfterID == "" || bytes.Compare(backendDomainKey(backendDomainRecordThread, id), backendDomainKey(backendDomainRecordThread, s.AfterID)) > 0) {
		return nil
	}
	if before != "" {
		if err := changeChildMembership(tx, before, id, false); err != nil {
			return err
		}
	}
	if after != "" {
		return changeChildMembership(tx, after, id, true)
	}
	return nil
}

func (repo *BackendRepo) advanceChildMembership(ctx context.Context, tx spi.WriteTx) (bool, error) {
	s, err := readChildMembershipState(tx)
	if err != nil || s.Phase == "ready" {
		return false, err
	}
	last, more, err := repo.backfillChildMembershipPage(ctx, tx, s.AfterID)
	if err != nil {
		return false, err
	}
	s = childMembershipState{Phase: "ready"}
	if more {
		s = childMembershipState{Phase: "backfill", AfterID: last}
	}
	return more, putChildMembershipState(tx, s)
}
