//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

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

func VerifyOfflineItemWindows(ctx context.Context, source, target spi.ReadTx, thread string, fields func(PathMetadata, bool) []string, compare func(WindowProjection, WindowProjection) error) (int, error) {
	if compare != nil {
		derived, ok := source.(spi.WriteTx)
		if !ok {
			return 0, spi.ErrInvalidArgument
		}
		meta, err := readPublishedThreadMeta(source, backendDomainV32Format, thread)
		if err != nil {
			return 0, err
		}
		var entries []Entry
		var metadata []PathMetadata
		var projection ThreadContextProjection
		if err = visitCanonicalPathMetadata(ctx, source, thread, meta.LeafID, func(entry Entry) error {
			if ref, ok := entry.bodyFields["activity_json"]; ok {
				ready, err := activitySourceMetadataReady(source, entry)
				if err != nil {
					return err
				}
				if !ready {
					body, err := materializeCanonicalBody(ctx, source, ref)
					if err != nil {
						return err
					}
					if err = writeActivitySummaryIndex(ctx, derived, ref, body); err != nil {
						return err
					}
				}
			}
			facts, err := canonicalPathMetadata(entry)
			if err != nil {
				return err
			}
			metadata = append(metadata, facts)
			entries = append(entries, entry)
			return projection.Observe(entry)
		}); err != nil {
			return 0, err
		}
		entries, err = membershipEntries(ctx, source, entries)
		if err != nil {
			return 0, err
		}
		window, turns, membership, err := projectWindowSources(ctx, source, entries)
		if err != nil {
			return 0, err
		}
		if err = storeItemMembership(ctx, derived, meta, entries, membership); err != nil {
			return 0, err
		}
		if err = storeWindowSources(derived, thread, window, turns); err != nil {
			return 0, err
		}
		if err = storeContextProjection(ctx, derived, meta, &projection); err != nil {
			return 0, err
		}
		refs, leaf, err := modelContextSourcesFromMetadata(ctx, source, meta, metadata)
		if err != nil {
			return 0, err
		}
		if err = putItemJSON(derived, modelContextSourcesKey(thread), storedModelContextSources{Leaf: leaf, Sources: refs}); err != nil {
			return 0, err
		}
	}
	options := ItemMembershipWindowOptions{ThreadID: thread, ItemsOnly: true, ActivityFields: true, Direction: PathWindowBefore, Limit: 20}
	read := func(tx spi.ReadTx) (WindowProjection, error) {
		meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread)
		if err != nil {
			return WindowProjection{}, err
		}
		return readWindowProjection(ctx, tx, meta, options, fields, true)
	}
	for page := 1; page <= 2; page++ {
		options.ItemsOnly = compare == nil || page != 1
		options.Summary = !options.ItemsOnly
		before, err := read(source)
		var missing missingWindowTextFacts
		if compare != nil && errors.As(err, &missing) {
			reader := newCanonicalBodyPageReader(ctx, source)
			for _, absent := range missing {
				body, err := reader.read(absent.ref)
				if err != nil {
					return 0, fmt.Errorf("source window %d canonical %s: %w", page, absent.field, err)
				}
				if err = writeBodyTextFacts(source.(spi.WriteTx), absent.ref, body); err != nil {
					return 0, err
				}
			}
			before, err = read(source)
		}
		if err != nil {
			return 0, fmt.Errorf("source window %d: %w", page, err)
		}
		after, err := read(target)
		if err != nil {
			return 0, fmt.Errorf("destination window %d: %w", page, err)
		}
		if compare != nil {
			if !reflect.DeepEqual(before.Membership, after.Membership) {
				for position, original := range before.Membership.Members {
					if position >= len(after.Membership.Members) {
						break
					}
					left, right := reflect.ValueOf(original), reflect.ValueOf(after.Membership.Members[position])
					var changed []string
					for field := 0; field < left.NumField(); field++ {
						if !reflect.DeepEqual(left.Field(field).Interface(), right.Field(field).Interface()) {
							changed = append(changed, left.Type().Field(field).Name)
						}
					}
					if len(changed) > 0 {
						return 0, fmt.Errorf("window %d member %d (%s): %s: %w", page, position, original.Kind, strings.Join(changed, ", "), ErrAuthorityCorrupt)
					}
				}
				return 0, fmt.Errorf("window %d membership mismatch: %w", page, ErrAuthorityCorrupt)
			}
			if err := compare(before, after); err != nil {
				return 0, fmt.Errorf("window %d: %w", page, err)
			}
		} else if !reflect.DeepEqual(before, after) {
			var changed []string
			left, right := reflect.ValueOf(before), reflect.ValueOf(after)
			for column := 0; column < left.NumField(); column++ {
				if !reflect.DeepEqual(left.Field(column).Interface(), right.Field(column).Interface()) {
					changed = append(changed, left.Type().Field(column).Name)
				}
			}
			return 0, fmt.Errorf("window %d differs in %s: %w", page, strings.Join(changed, ", "), ErrAuthorityCorrupt)
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
