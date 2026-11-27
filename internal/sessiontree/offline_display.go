//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"reflect"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func copyOfflineOptional(source spi.ReadTx, target spi.WriteTx, namespace string, key []byte) error {
	raw, err := source.Get(namespace, key)
	if errors.Is(err, spi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return target.Put(namespace, key, raw)
}

func copyOfflinePrefix(ctx context.Context, source spi.ReadTx, target spi.WriteTx, namespace string, prefix []byte) error {
	request := spi.ScanRequest{Namespace: namespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: storagebridge.MaxRecordBatch}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := source.Scan(request)
		if err != nil {
			return err
		}
		previous := request.After
		for _, row := range page.Records {
			if !bytes.HasPrefix(row.Key, prefix) || bytes.Compare(row.Key, previous) <= 0 {
				return ErrAuthorityCorrupt
			}
			previous = row.Key
		}
		if len(page.Records) != 0 {
			if err := storagebridge.WriteRecords(target, namespace, page.Records); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, previous) {
			return ErrAuthorityCorrupt
		}
		request.After = bytes.Clone(previous)
	}
}

func stageOfflineDisplaySources(ctx context.Context, source spi.ReadTx, target spi.WriteTx, thread string) error {
	for _, namespace := range []string{itemMembershipNamespace, projectionSourcesNamespace, sparseSourcesNamespace, contextMetadataNamespace} {
		if err := copyOfflinePrefix(ctx, source, target, namespace, backendDomainKey(thread)); err != nil {
			return err
		}
	}
	for _, kind := range []string{"entry", "source", "turn_member", "queue", "active_turn", "request", "turn", "effect"} {
		if err := copyOfflinePrefix(ctx, source, target, backendEntryIndexNamespace, backendDomainKey(kind, thread)); err != nil {
			return err
		}
	}
	if err := copyOfflinePrefix(ctx, source, target, queryDocumentNamespace, []byte(thread+"\x00")); err != nil {
		return err
	}
	return copyOfflineOptional(source, target, queryManifestNamespace, []byte(thread))
}

func VerifyOfflineItemWindows(ctx context.Context, source, target spi.ReadTx, thread string, fields func(PathMetadata, bool) []string) (int, error) {
	options := ItemMembershipWindowOptions{ThreadID: thread, ItemsOnly: true, ActivityFields: true, Direction: PathWindowBefore, Limit: 20}
	read := func(tx spi.ReadTx) (WindowProjection, error) {
		meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread)
		if err != nil {
			return WindowProjection{}, err
		}
		return readWindowProjection(ctx, tx, meta, options, fields, true)
	}
	for page := 1; page <= 2; page++ {
		before, err := read(source)
		if err != nil {
			return 0, err
		}
		after, err := read(target)
		if err != nil {
			return 0, err
		}
		if !reflect.DeepEqual(before, after) {
			return 0, ErrAuthorityCorrupt
		}
		if page == 2 || !before.Membership.HasBefore {
			return page, nil
		}
		if len(before.Membership.Members) == 0 {
			return 0, ErrAuthorityCorrupt
		}
		options.AnchorID = before.Membership.Members[0].ID
		options.AncestorLeafID = before.Membership.LeafID
	}
	return 0, ErrAuthorityCorrupt
}
