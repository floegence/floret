//go:build floret_layout_cutover

package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"

	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

type OfflineScopeProof struct {
	Records [promptCategoryCount]uint64
	Bytes   uint64
	Digest  [32]byte
}

func WalkOfflinePromptOwners(ctx context.Context, source spi.ReadTx, visit func(scope, thread string) error) error {
	prefix := storagecodec.Tuple(storagecodec.TupleString("scope"))
	return scanPromptRecords(source, prefix, prefixEnd(prefix), func(row spi.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var counter promptScopeCounters
		if err := decodePromptValue(row.Value, "prompt_scope", &counter); err != nil || counter.ScopeID == "" || !bytes.Equal(row.Key, promptScopeKey(counter.ScopeID)) {
			return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
		}
		for _, category := range []int{promptRequest, promptResponse, promptToolset, promptSegment} {
			if counter.Counts[category] == 0 {
				continue
			}
			key := promptRecordKey(promptRecord{ScopeID: counter.ScopeID, Category: category, Ordinal: 1})
			raw, err := source.Get(promptRecordsNamespace, key)
			if err != nil {
				return err
			}
			var record promptRecord
			if err := decodePromptValue(raw, "prompt_record", &record); err != nil || !bytes.Equal(promptRecordKey(record), key) {
				return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
			}
			author, err := promptRecordAuthor(record)
			if err != nil {
				return err
			}
			return visit(counter.ScopeID, author)
		}
		return spi.ErrConflict
	})
}

func OfflinePromptScope(ctx context.Context, source spi.ReadTx, target spi.WriteTx, scope, thread string) (OfflineScopeProof, error) {
	digest := sha256.New()
	digest.Write([]byte("floret.offline.prompt-scope.v1\x00"))
	return inspectOfflineScope(ctx, source, target, scope, thread, digest)
}

func VerifyDeletedPromptScope(ctx context.Context, source spi.ReadTx, scope, thread string) error {
	if scope != thread {
		return spi.ErrConflict
	}
	_, err := inspectOfflineScope(ctx, source, nil, scope, thread, nil)
	return err
}

func RemoveDeletedPromptScope(ctx context.Context, tx spi.WriteTx, thread string) (uint64, error) {
	if thread == "" {
		return 0, spi.ErrInvalidArgument
	}
	var removed uint64
	_, err := tx.Get(promptRecordsNamespace, promptScopeKey(thread))
	if err == nil {
		proof, err := inspectOfflineScope(ctx, tx, nil, thread, thread, nil)
		if err != nil {
			return 0, err
		}
		for _, count := range proof.Records {
			removed += count
		}
		if removed == 0 {
			return 0, sessiontree.ErrAuthorityCorrupt
		}
		if err := deletePromptScope(tx, thread); err != nil {
			return 0, err
		}
	} else if !errors.Is(err, spi.ErrNotFound) {
		return 0, err
	}
	if _, err := tx.Get(promptRecordsNamespace, promptScopeKey(thread)); !errors.Is(err, spi.ErrNotFound) {
		return 0, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	for _, part := range offlineScopeRanges(thread) {
		page, err := tx.Scan(spi.ScanRequest{Namespace: part.namespace, Start: part.prefix, End: prefixEnd(part.prefix), Limit: 1})
		if err != nil || len(page.Records) != 0 || page.HasMore {
			return 0, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
		}
	}
	return removed, nil
}

type offlineRange struct {
	namespace string
	prefix    []byte
}

func offlineScopeRanges(scope string) []offlineRange {
	return []offlineRange{
		{promptRecordsNamespace, storagecodec.Tuple(storagecodec.TupleString("record"), storagecodec.TupleString(scope))},
		{promptLookupNamespace, contextRequestPrefix(scope)}, {promptRequestLocatorNamespace, contextRequestPrefix(scope)},
	}
}

func inspectOfflineScope(ctx context.Context, source spi.ReadTx, target spi.WriteTx, scope, thread string, digest hash.Hash) (OfflineScopeProof, error) {
	var proof OfflineScopeProof
	if ctx == nil || source == nil || scope == "" || thread == "" {
		return proof, spi.ErrInvalidArgument
	}
	accept := func(namespace string, key, value []byte) error {
		if digest != nil {
			for _, part := range [][]byte{[]byte(namespace), key, value} {
				digest.Write(binary.BigEndian.AppendUint64(nil, uint64(len(part))))
				digest.Write(part)
			}
		}
		proof.Bytes += uint64(len(value))
		if target != nil {
			if _, err := target.Get(namespace, key); !errors.Is(err, spi.ErrNotFound) {
				return errors.Join(spi.ErrConflict, err)
			}
			return target.Put(namespace, key, value)
		}
		return nil
	}
	key := promptScopeKey(scope)
	value, err := source.Get(promptRecordsNamespace, key)
	if err != nil {
		return proof, err
	}
	counters, err := readPromptCounters(source, scope)
	if err != nil {
		return proof, err
	}
	if err := accept(promptRecordsNamespace, key, value); err != nil {
		return OfflineScopeProof{}, err
	}
	for _, part := range offlineScopeRanges(scope) {
		request := spi.ScanRequest{Namespace: part.namespace, Start: part.prefix, End: prefixEnd(part.prefix), Limit: 4}
		for {
			if err := ctx.Err(); err != nil {
				return OfflineScopeProof{}, err
			}
			page, err := source.Scan(request)
			if err != nil {
				return OfflineScopeProof{}, err
			}
			for _, row := range page.Records {
				if !bytes.HasPrefix(row.Key, part.prefix) || bytes.Compare(row.Key, request.After) <= 0 {
					return OfflineScopeProof{}, sessiontree.ErrAuthorityCorrupt
				}
				if part.namespace == promptRecordsNamespace {
					var record promptRecord
					if err := decodePromptValue(row.Value, "prompt_record", &record); err != nil || record.ScopeID != scope || record.Category < 0 || record.Category >= promptCategoryCount || !bytes.Equal(row.Key, promptRecordKey(record)) || record.Ordinal != proof.Records[record.Category]+1 {
						return OfflineScopeProof{}, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
					}
					author, err := promptRecordAuthor(record)
					if err != nil || author != thread {
						return OfflineScopeProof{}, errors.Join(spi.ErrConflict, err)
					}
					proof.Records[record.Category]++
				}
				if err := accept(part.namespace, row.Key, row.Value); err != nil {
					return OfflineScopeProof{}, err
				}
				request.After = row.Key
			}
			if !page.HasMore {
				break
			}
			if len(page.Records) == 0 || !bytes.Equal(request.After, page.Next) {
				return OfflineScopeProof{}, sessiontree.ErrAuthorityCorrupt
			}
		}
	}
	if proof.Records != counters.Counts {
		return OfflineScopeProof{}, sessiontree.ErrAuthorityCorrupt
	}
	if digest != nil {
		copy(proof.Digest[:], digest.Sum(nil))
	}
	return proof, nil
}
