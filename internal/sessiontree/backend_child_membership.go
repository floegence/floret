package sessiontree

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

// A parent with children has a count at its exact prefix and one locator per
// child below it. Roots without children take no space. The schema owner must
// establish complete coverage before using absence as evidence of no children.
const childMembershipNamespace = "floret.sessiontree.child-membership.v1"

func storedChildCount(raw []byte) (uint64, error) {
	if len(raw) != 8 || binary.BigEndian.Uint64(raw) == 0 {
		return 0, ErrAuthorityCorrupt
	}
	return binary.BigEndian.Uint64(raw), nil
}

func changeChildMembership(tx spi.WriteTx, parent, child string, insert bool) error {
	tx = storagebridge.CatalogWriter(tx)
	if parent == "" || child == "" || child == parent {
		return ErrAuthorityCorrupt
	}
	prefix, key := backendDomainKey(parent), backendDomainKey(parent, child)
	value, err := tx.Get(childMembershipNamespace, key)
	switch {
	case err == nil:
		if insert || string(value) != child {
			return ErrAuthorityCorrupt
		}
	case errors.Is(err, spi.ErrNotFound):
		if !insert {
			return ErrAuthorityCorrupt
		}
	default:
		return err
	}
	var n uint64
	raw, err := tx.Get(childMembershipNamespace, prefix)
	if err == nil {
		n, err = storedChildCount(raw)
	} else if errors.Is(err, spi.ErrNotFound) && insert {
		// A missing count must not repair orphaned edges on the next insert.
		page, readErr := tx.Scan(spi.ScanRequest{Namespace: childMembershipNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 1})
		if readErr != nil {
			return readErr
		}
		if len(page.Records) != 0 || page.HasMore {
			return ErrAuthorityCorrupt
		}
		err = nil
	} else if errors.Is(err, spi.ErrNotFound) {
		return ErrAuthorityCorrupt
	}
	if err != nil {
		return err
	}
	if insert {
		if n == math.MaxUint64 {
			return ErrAuthorityCorrupt
		}
		if err := tx.Put(childMembershipNamespace, key, []byte(child)); err != nil {
			return err
		}
		n++
	} else {
		if err := tx.Delete(childMembershipNamespace, key); err != nil {
			return err
		}
		n--
		if n == 0 {
			return tx.Delete(childMembershipNamespace, prefix)
		}
	}
	return tx.Put(childMembershipNamespace, prefix, binary.BigEndian.AppendUint64(nil, n))
}

func visitIndexedChildren(ctx context.Context, tx spi.ReadTx, parent string, visit func(ThreadMeta) error) error {
	if parent == "" {
		return spi.ErrInvalidArgument
	}
	prefix := backendDomainKey(parent)
	req := spi.ScanRequest{Namespace: childMembershipNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: backendThreadEntryPageSize}
	var expected, seen uint64
	var previous []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(req)
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if bytes.Compare(row.Key, previous) <= 0 {
				return ErrAuthorityCorrupt
			}
			previous = row.Key
			if bytes.Equal(row.Key, prefix) {
				expected, err = storedChildCount(row.Value)
				if err != nil {
					return err
				}
				continue
			}
			child := string(row.Value)
			if seen >= expected || child == "" || child == parent || !bytes.Equal(row.Key, backendDomainKey(parent, child)) {
				return ErrAuthorityCorrupt
			}
			meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, child)
			if err != nil || meta.ParentThreadID != parent {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := visit(meta); err != nil {
				return err
			}
			seen++
		}
		if !page.HasMore {
			if seen != expected {
				return ErrAuthorityCorrupt
			}
			return ctx.Err()
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, previous) {
			return ErrAuthorityCorrupt
		}
		req.After = bytes.Clone(page.Next)
		previous = req.After
	}
}
