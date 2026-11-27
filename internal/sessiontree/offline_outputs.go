//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"context"
	"errors"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

type offlineOutputSet map[string]*offlineOutputNeed

func PrepareOfflineCharges(ctx context.Context, tx spi.WriteTx, thread string, peak map[string]int64) error {
	catalog := storagebridge.CatalogWriter(tx)
	prefix := backendDomainKey("source")
	request := spi.ScanRequest{Namespace: outputSourceNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128}
	var local int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(request)
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			var header OutputSource
			if err := readOutputRecord(tx, row.Key, &header); err != nil || !bytes.Equal(row.Key, outputSourceKey(header.ID)) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			source, err := readOutputSource(tx, header.ID)
			if err != nil {
				return err
			}
			if err := verifyOutputSource(ctx, tx, source); err != nil {
				return err
			}
			if source.Bytes == 0 {
				continue
			}
			charge := outputCharge{thread, source.ID, source.Bytes}
			var prior outputCharge
			err = readOutputRecord(catalog, charge.ownerKey(), &prior)
			if errors.Is(err, spi.ErrNotFound) {
				if ok, err := changeOutputCharge(catalog, charge, 0); err != nil || !ok {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
			} else if err != nil || prior != charge {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := readOutputRecord(catalog, charge.rankKey(), &prior); err != nil || prior != charge {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			peak[source.ID] = max(peak[source.ID], source.Bytes)
			local += source.Bytes
		}
		if !page.HasMore {
			break
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, page.Records[len(page.Records)-1].Key) {
			return ErrAuthorityCorrupt
		}
		request.After = page.Next
	}
	total, err := outputStoreUsage(tx)
	if err != nil || total != local {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}

func VerifyOfflineCharges(catalog spi.ReadTx, peak map[string]int64) (int64, error) {
	var expected int64
	for id, bytes := range peak {
		highest, err := peakOutputCharge(catalog, id)
		if err != nil || highest != bytes {
			return 0, errors.Join(ErrAuthorityCorrupt, err)
		}
		expected += bytes
	}
	total, err := outputStoreUsage(catalog)
	if err != nil || total != expected || expected > outputStoreMaxBytes {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return total, nil
}

type offlineOutputNeed struct {
	full       bool
	references int64
	pins       map[OutputSnapshot]struct{}
}

func (outputs offlineOutputSet) retain(id string, pin *OutputSnapshot) {
	need := outputs[id]
	if need == nil {
		need = &offlineOutputNeed{}
		outputs[id] = need
	}
	need.references++
	if pin == nil {
		need.full = true
		return
	}
	if need.pins == nil {
		need.pins = make(map[OutputSnapshot]struct{})
	}
	need.pins[*pin] = struct{}{}
}

func (outputs offlineOutputSet) collect(source spi.ReadTx, target spi.WriteTx, entry Entry) error {
	if err := validateToolOutputReference(entry); err != nil {
		return err
	}
	if entry.Type == EntryHostWork {
		work, err := decodeHostWork(entry)
		if err != nil {
			return err
		}
		if work.State == "bound" && work.OutputStream != "" {
			if !work.Inherited && work.OutputStream != HostWorkOutputSourceID(entry.ThreadID, work.ID) || work.Inherited && work.OutputSnapshot == nil {
				return ErrAuthorityCorrupt
			}
			outputs.retain(work.OutputStream, work.OutputSnapshot)
		}
	}
	result := entry.Message.ToolResult
	if result == nil || result.FullOutput == nil {
		return nil
	}
	ref := *result.FullOutput
	if artifact.ValidateRef(ref) != nil || entry.Type != EntryToolResult && entry.Type != EntryHostWork && entry.Type != EntryReferenceHistory {
		return ErrAuthorityCorrupt
	}
	name := artifactRecordKey(entry.ThreadID, ref.ID)
	key := backendDomainKey(backendDomainRecordArtifact, name)
	stored, found, err := readBackendDomainRecord(source, backendDomainV32Format, key, backendDomainRecordArtifact, name, "", 0)
	if err != nil || !found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	var record artifact.Record
	if err := decodeBackendDomainValue(backendDomainV32Format, stored, &record); err != nil || record.CanonicalEntryID != entry.ID || record.ThreadID != entry.ThreadID || record.Ref != ref || record.CreatedAt.IsZero() {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if record.OutputSourceID == "" {
		if artifact.TextSHA256(record.Text) != ref.SHA256 || int64(len(record.Text)) != ref.SizeBytes {
			return ErrAuthorityCorrupt
		}
	} else {
		origin, err := readOutputSource(source, record.OutputSourceID)
		if err != nil || record.Text != "" || !origin.Sealed || origin.Bytes != ref.SizeBytes || origin.SHA256 != ref.SHA256 {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		outputs.retain(origin.ID, nil)
	}
	raw, err := source.Get(backendDomainV32Namespace, key)
	if err != nil {
		return err
	}
	return target.Put(backendDomainV32Namespace, key, raw)
}

func (outputs offlineOutputSet) stage(ctx context.Context, source spi.ReadTx, target spi.WriteTx) error {
	for id, need := range outputs {
		if err := ctx.Err(); err != nil {
			return err
		}
		origin, err := readOutputSource(source, id)
		if err != nil || origin.References < need.references {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		var longest OutputSnapshot
		for pin := range need.pins {
			if !validOutputSnapshot(pin) || pin.Chunks > origin.Chunks || pin.Bytes > origin.Bytes || pin.ObservedBytes > origin.ObservedBytes {
				return ErrAuthorityCorrupt
			}
			if longest.SHA256 == "" || pin.Chunks > longest.Chunks || pin.Chunks == longest.Chunks && (pin.ObservedBytes > longest.ObservedBytes || pin.ObservedBytes == longest.ObservedBytes && pin.Reason > longest.Reason) {
				longest = pin
			}
		}
		if need.full {
			longest = snapshotOutput(origin)
		}
		if err := retainDetachedOutput(ctx, source, target, origin, longest); err != nil {
			return err
		}
		local, err := readOutputSource(target, id)
		if err != nil {
			return err
		}
		for pin := range need.pins {
			if pin == longest {
				continue
			}
			prefix := local
			prefix.Bytes, prefix.Chunks, prefix.SHA256 = pin.Bytes, pin.Chunks, pin.SHA256
			if err := verifyOutputSource(ctx, target, prefix); err != nil {
				return err
			}
		}
		if need.full {
			if !bytes.Equal(origin.HashState, local.HashState) {
				return ErrAuthorityCorrupt
			}
			local = origin
		}
		local.References = need.references
		if err := putOutputRecord(target, outputSourceKey(id), local); err != nil {
			return err
		}
	}
	return nil
}
