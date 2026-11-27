package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
)

type scopedOutputWriter interface {
	ThreadOwner() identity.ThreadID
	CatalogWriter() spi.WriteTx
}

type outputCharge struct {
	Thread, Source string
	Bytes          int64
}

func (charge outputCharge) ownerKey() []byte {
	return backendDomainKey("output-owner", charge.Thread, charge.Source)
}
func (charge outputCharge) rankKey() []byte {
	return backendDomainKey("output-rank", charge.Source, fmt.Sprintf("%08x", OutputSourceMaxBytes-charge.Bytes), charge.Thread)
}

func peakOutputCharge(tx spi.ReadTx, source string) (int64, error) {
	prefix := backendDomainKey("output-rank", source)
	page, err := tx.Scan(spi.ScanRequest{Namespace: outputSourceNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 1})
	if err != nil || len(page.Records) == 0 {
		return 0, err
	}
	var charge outputCharge
	key := page.Records[0].Key
	if err := readOutputRecord(tx, key, &charge); err != nil || charge.Source != source || charge.Thread == "" || charge.Bytes <= 0 || charge.Bytes > OutputSourceMaxBytes || !bytes.Equal(key, charge.rankKey()) {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return charge.Bytes, nil
}

func changeOutputCharge(tx spi.WriteTx, charge outputCharge, previous int64) (bool, error) {
	if charge.Source == "" || charge.Thread == "" || previous < 0 || previous > OutputSourceMaxBytes || charge.Bytes < 0 || charge.Bytes > OutputSourceMaxBytes {
		return false, ErrAuthorityCorrupt
	}
	key := charge.ownerKey()
	var saved outputCharge
	err := readOutputRecord(tx, key, &saved)
	if err != nil && !errors.Is(err, spi.ErrNotFound) {
		return false, err
	}
	old := charge
	old.Bytes = previous
	if (previous == 0) != errors.Is(err, spi.ErrNotFound) || previous != 0 && saved != old {
		return false, ErrAuthorityCorrupt
	}
	before, err := peakOutputCharge(tx, charge.Source)
	if err != nil {
		return false, err
	}
	total, err := outputStoreUsage(tx)
	if err != nil || total < before || before < previous || total > outputStoreMaxBytes {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if charge.Bytes-before > outputStoreMaxBytes-total {
		return false, nil
	}
	if previous > 0 {
		if err := tx.Delete(outputSourceNamespace, old.rankKey()); err != nil {
			return false, err
		}
	}
	if charge.Bytes == 0 {
		if previous != 0 {
			if err := tx.Delete(outputSourceNamespace, key); err != nil {
				return false, err
			}
		}
	} else {
		if err := putOutputRecord(tx, key, charge); err != nil {
			return false, err
		}
		if err := putOutputRecord(tx, charge.rankKey(), charge); err != nil {
			return false, err
		}
	}
	after := max(before, charge.Bytes)
	if previous == before && charge.Bytes < before {
		after, err = peakOutputCharge(tx, charge.Source)
		if err != nil {
			return false, err
		}
	}
	if before != after {
		if err := putOutputRecord(tx, outputUsageKey(), total-before+after); err != nil {
			return false, err
		}
	}
	return true, nil
}

func releaseThreadOutputCharges(ctx context.Context, tx spi.WriteTx, thread string) error {
	prefix := backendDomainKey("output-owner", thread)
	request := spi.ScanRequest{Namespace: outputSourceNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(request)
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			var charge outputCharge
			if err := readOutputRecord(tx, row.Key, &charge); err != nil || charge.Thread != thread || charge.Bytes <= 0 || !bytes.Equal(row.Key, charge.ownerKey()) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			previous := charge.Bytes
			charge.Bytes = 0
			if ok, err := changeOutputCharge(tx, charge, previous); err != nil || !ok {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, page.Records[len(page.Records)-1].Key) {
			return ErrAuthorityCorrupt
		}
		request.After = page.Next
	}
}
