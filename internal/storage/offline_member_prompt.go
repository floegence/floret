//go:build floret_layout_cutover

package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func visitMemberPromptSources(ctx context.Context, source spi.ReadTx, thread string, visit func(promptRecord) error) error {
	scopes := make(map[string][promptCategoryCount]uint64)
	err := WalkOfflinePromptOwners(ctx, source, func(scope, author string) error {
		if author != thread {
			return sessiontree.ErrAuthorityCorrupt
		}
		counts, err := readPromptCounters(source, scope)
		scopes[scope] = counts.Counts
		return err
	})
	if err != nil {
		return err
	}
	counts := make(map[string][promptCategoryCount]uint64)
	prefix := storagecodec.Tuple(storagecodec.TupleString("record"))
	request := spi.ScanRequest{Namespace: promptRecordsNamespace, Start: prefix, End: prefixEnd(prefix), Limit: 4}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := source.Scan(request)
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			var record promptRecord
			if err := decodePromptValue(row.Value, "prompt_record", &record); err != nil || record.Category < 0 || record.Category >= promptCategoryCount || bytes.Compare(row.Key, request.After) <= 0 || !bytes.Equal(row.Key, promptRecordKey(record)) {
				return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
			}
			current, limit := counts[record.ScopeID], scopes[record.ScopeID]
			if record.Ordinal == 0 || current[record.Category]+1 != record.Ordinal || record.Ordinal > limit[record.Category] {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err := visit(record); err != nil {
				return err
			}
			current[record.Category]++
			counts[record.ScopeID] = current
			request.After = row.Key
		}
		if !page.HasMore {
			break
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, request.After) {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
	for scope, limit := range scopes {
		if counts[scope] != limit {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
	return nil
}

func StageMemberPrompts(ctx context.Context, source spi.ReadTx, target *member.Journal) error {
	return visitMemberPromptSources(ctx, source, target.Thread(), func(record promptRecord) error {
		return appendMemberPrompt(target, record)
	})
}

func VerifyMemberPrompts(ctx context.Context, source spi.ReadTx, target *member.Journal) (OfflineScopeProof, error) {
	var proof OfflineScopeProof
	digest := sha256.New()
	digest.Write([]byte("floret.member.provider-replay.v1\x00"))
	err := visitMemberPromptSources(ctx, source, target.Thread(), func(record promptRecord) error {
		copied, err := readMemberPrompt(target, record.ScopeID, record.Category, record.Ordinal)
		if err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(record.Value, &fields); err != nil {
			return err
		}
		original, err := json.Marshal(fields)
		if err != nil || !bytes.Equal(copied.Value, original) {
			return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
		}
		key := promptRecordKey(record)
		for _, value := range [][]byte{key, original} {
			digest.Write(binary.BigEndian.AppendUint64(nil, uint64(len(value))))
			digest.Write(value)
		}
		proof.Records[record.Category]++
		proof.Bytes += uint64(len(original))
		return nil
	})
	if err != nil {
		return OfflineScopeProof{}, err
	}
	var total uint64
	for _, count := range proof.Records {
		total += count
	}
	stored, err := target.PromptCount()
	if err != nil || total != uint64(stored) {
		return OfflineScopeProof{}, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	copy(proof.Digest[:], digest.Sum(nil))
	return proof, nil
}
