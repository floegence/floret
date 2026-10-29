package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const snapshotAuthorityNamespace = "floret.domain.sessiontree.snapshot-authority.v32"

var snapshotPreparationKey = storagecodec.Tuple(storagecodec.TupleString("preparation"))

type snapshotPreparation struct {
	Version         int
	MigrationTime   int64
	Phase           string
	DirectoryCursor []byte
}

// PrepareHistoricalSnapshot runs only in the Host-owned worker. Source bytes
// and parser checkpoints commit in bounded batches before any canonical owner
// is installed. The directory reserves historical identities; it never stores
// transcripts or serves runtime conversation reads.
func PrepareHistoricalSnapshot(ctx context.Context, backend spi.Backend, now func() time.Time) error {
	if ctx == nil || backend == nil || now == nil {
		return spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var sourceKind backendDomainSource
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		var err error
		sourceKind, err = detectBackendDomainSource(ctx, tx)
		return err
	}); err != nil {
		return err
	}
	if sourceKind != backendDomainSourceLegacy {
		return nil
	}
	for {
		var checkpoint snapshotImportCheckpoint
		if err := backend.Update(ctx, func(tx spi.WriteTx) error {
			var err error
			checkpoint, err = indexHistoricalSnapshotBatch(ctx, tx)
			return err
		}); err != nil {
			return err
		}
		reportStartupWork(ctx, StartupPhaseMigrating, checkpoint.Scan.Offset+checkpoint.VerifyOffset, checkpoint.SourceBytes*2)
		if checkpoint.Complete {
			break
		}
	}
	reader := migrationSourceReader{ctx: ctx, backend: backend}
	checkpoint, _, err := readSnapshotImportCheckpoint(reader)
	if err != nil {
		return err
	}
	sequences, err := snapshotJournalSequences(ctx, reader)
	if err != nil {
		return err
	}
	if len(sequences) > 0 && checkpoint.Version != 5 {
		return ErrAuthorityCorrupt
	}
	for _, sequence := range sequences {
		for {
			complete := false
			if err := backend.Update(ctx, func(tx spi.WriteTx) error {
				indexed, err := indexSnapshotJournalBatch(ctx, tx, sequence)
				complete = indexed.Complete
				return err
			}); err != nil {
				return err
			}
			if complete {
				break
			}
		}
	}
	source, err := newSnapshotEffectiveSource(ctx, reader, checkpoint.Version)
	if err != nil {
		return err
	}
	// Recheck all wrapper and member-key bytes, not just the indexed values.
	if err := verifyIndexedJSONRecord(ctx, reader, backendDomainNamespace, backendStateKey, checkpoint); err != nil {
		return err
	}
	if err := verifySnapshotSourceInventory(ctx, source, checkpoint.Version); err != nil {
		return err
	}
	var preparation snapshotPreparation
	err = backend.Update(ctx, func(tx spi.WriteTx) error {
		raw, err := tx.Get(snapshotImportNamespace, snapshotPreparationKey)
		if errors.Is(err, spi.ErrNotFound) {
			preparation = snapshotPreparation{Version: checkpoint.Version, MigrationTime: now().UTC().UnixNano(), Phase: "directory"}
			encoded, err := json.Marshal(preparation)
			if err != nil {
				return err
			}
			return tx.Put(snapshotImportNamespace, snapshotPreparationKey, encoded)
		}
		if err != nil {
			return err
		}
		if err := decodeCanonicalBodyJSON(raw, &preparation); err != nil || preparation.Version != checkpoint.Version || preparation.MigrationTime <= 0 || preparation.Phase != "directory" {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	identities, err := loadSnapshotIdentities(ctx, source, checkpoint.Version)
	if err != nil {
		return err
	}
	format, _ := migrationSourceFormat(checkpoint.Version)
	values, err := snapshotDirectoryRecords(format, identities)
	if err != nil {
		return err
	}
	keys := make([][]byte, 0, len(values))
	for key := range values {
		keys = append(keys, []byte(key))
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	// A restart revalidates completed directory rows against immutable source
	// identities, then resumes at the last atomically committed page.
	if err := verifySnapshotDirectory(ctx, reader, format, values, preparation.DirectoryCursor, false); err != nil {
		return err
	}
	start := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], preparation.DirectoryCursor) > 0 })
	for start < len(keys) {
		end, size := start, 0
		for end < len(keys) && end-start < backendThreadEntryPageSize {
			length := len(values[string(keys[end])])
			if end > start && size+length > backendThreadMigrationBatchBytes {
				break
			}
			size += length
			end++
		}
		if err := backend.Update(ctx, func(tx spi.WriteTx) error {
			for _, key := range keys[start:end] {
				if err := tx.Put(format.namespace, key, values[string(key)]); err != nil {
					return err
				}
			}
			preparation.DirectoryCursor = bytes.Clone(keys[end-1])
			raw, err := json.Marshal(preparation)
			if err != nil {
				return err
			}
			return tx.Put(snapshotImportNamespace, snapshotPreparationKey, raw)
		}); err != nil {
			return err
		}
		start = end
		reportStartupWork(ctx, StartupPhaseMigrating, int64(start), int64(len(keys)))
	}
	if err := verifySnapshotDirectory(ctx, reader, format, values, preparation.DirectoryCursor, true); err != nil {
		return err
	}
	var sequence int64
	value, found, err := source.field(ctx, "sequence")
	if err != nil || !found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := value.decode(ctx, reader, &sequence); err != nil || sequence < 0 {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return backend.Update(ctx, func(tx spi.WriteTx) error {
		manifest := backendDomainManifest{Version: checkpoint.Version, Sequence: sequence}
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
			return err
		}
		return putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: backendDomainV34Version, Sequence: sequence, SourceVersion: checkpoint.Version, MigrationTime: preparation.MigrationTime})
	})
}

// Identity metadata is bounded by thread count, never transcript size. Read
// root intents once so a 10,000-thread import does not scan them 10,000 times.
func loadSnapshotIdentities(ctx context.Context, source *snapshotEffectiveSource, version int) (memoryState, error) {
	state := memoryState{Version: version, Threads: map[string]ThreadMeta{}, Tombstones: map[string]ThreadTombstone{}, RootCreateIntents: map[string]legacyRootCreateRecord{}}
	for _, field := range []string{"threads", "tombstones", "root_create_intents"} {
		if field == "root_create_intents" && version >= 5 {
			continue
		}
		ids, err := source.members(ctx, field)
		if err != nil {
			return state, err
		}
		for _, id := range ids {
			switch field {
			case "threads":
				var meta ThreadMeta
				if err := source.decodeMember(ctx, &meta, field, id); err != nil {
					return state, err
				}
				if meta.ID != id {
					return state, ErrAuthorityCorrupt
				}
				state.Threads[id] = meta
			case "tombstones":
				var tombstone legacySnapshotTombstone
				if err := source.decodeMember(ctx, &tombstone, field, id); err != nil {
					return state, err
				}
				state.Tombstones[id] = snapshotTombstoneAtVersion(tombstone, version)
			case "root_create_intents":
				var intent legacyRootCreateRecord
				if err := source.decodeMember(ctx, &intent, field, id); err != nil {
					return state, err
				}
				state.RootCreateIntents[id] = intent
			}
		}
	}
	if version < 5 {
		if err := migrateSnapshotOrigins(&state); err != nil {
			return state, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	for id, tombstone := range state.Tombstones {
		if _, live := state.Threads[id]; live {
			return state, ErrAuthorityCorrupt
		}
		if err := validateMemoryArtifactTombstone(tombstone, id); err != nil {
			return state, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	return state, nil
}

func snapshotDirectoryRecords(format backendDomainFormat, identities memoryState) (map[string][]byte, error) {
	rows := make(map[string][]byte, len(identities.Threads)+len(identities.Tombstones))
	put := func(kind, id string, value any) error {
		raw, err := encodeBackendDomainRecord(format, kind, id, "", 0, value)
		if err == nil {
			rows[string(backendDomainKey(kind, id))] = raw
		}
		return err
	}
	for id, meta := range identities.Threads {
		if err := put(backendDomainRecordThread, id, meta); err != nil {
			return nil, err
		}
	}
	for id, tombstone := range identities.Tombstones {
		if err := put(backendDomainRecordTombstone, id, tombstone); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func verifySnapshotDirectory(ctx context.Context, tx spi.ReadTx, format backendDomainFormat, expected map[string][]byte, cursor []byte, complete bool) error {
	if len(cursor) > 0 {
		if _, ok := expected[string(cursor)]; !ok {
			return ErrAuthorityCorrupt
		}
	}
	seen := 0
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: format.namespace, After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			want, found := expected[string(row.Key)]
			if !found || !bytes.Equal(want, row.Value) || !complete && bytes.Compare(row.Key, cursor) > 0 {
				return ErrAuthorityCorrupt
			}
			seen++
		}
		if !page.HasMore {
			break
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
	count := 0
	for key := range expected {
		if complete || bytes.Compare([]byte(key), cursor) <= 0 {
			count++
		}
	}
	if count != seen {
		return ErrAuthorityCorrupt
	}
	return nil
}

// Check global ownership before selecting a closure. Selecting a subset must
// never hide orphan records or an invalid source recovery frame.
func verifySnapshotSourceInventory(ctx context.Context, source *snapshotEffectiveSource, version int) error {
	for name, definition := range snapshotStateFields() {
		value, found, err := source.field(ctx, name)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if len(source.sequences) == 0 && !snapshotFieldKeyedByThread(name) && name != "artifacts" {
			if err := validateSnapshotTypedValue(ctx, source.tx, definition.Type, value, snapshotSourceKey("payload", name)); err != nil {
				return err
			}
		}
	}
	for _, field := range []string{"entries", "provider_states", "todos"} {
		ids, err := source.members(ctx, field)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if _, found, err := source.member(ctx, "threads", id); err != nil || !found {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	}
	if _, err := loadSnapshotIdentities(ctx, source, version); err != nil {
		return err
	}
	keys, err := source.members(ctx, "artifacts")
	if err != nil {
		return err
	}
	for _, key := range keys {
		owner, _, ok := strings.Cut(key, "\x00")
		if !ok {
			return ErrAuthorityCorrupt
		}
		if _, found, err := source.member(ctx, "threads", owner); err != nil || !found {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	if version < 5 {
		orphans := map[string]bool{}
		// The released decoder types these raw collections before discarding
		// obsolete lifecycle mirrors. Scope filtering must not hide malformed
		// members, including records whose thread no longer exists.
		for name, shape := range map[string]reflect.Type{
			"leases":           reflect.TypeFor[map[string]legacyTurnLease](),
			"lease_generation": reflect.TypeFor[map[string]int64](),
			"subagent_inputs":  reflect.TypeFor[map[string][]SubAgentInputRecord](),
			"turn_admissions":  reflect.TypeFor[map[string]legacyTurnAdmission](),
			"turn_finishes":    reflect.TypeFor[map[string]legacyTurnFinish](),
		} {
			value, found, err := source.field(ctx, name)
			if err != nil || !found {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := validateSnapshotTypedValue(ctx, source.tx, shape, value, snapshotSourceKey("payload", name)); err != nil {
				return err
			}
			keys, err := source.members(ctx, name)
			if err != nil {
				return err
			}
			for _, key := range keys {
				owner := key
				if name == "turn_admissions" || name == "turn_finishes" {
					owner, _, _ = strings.Cut(key, "\x00")
				}
				if _, found, err := source.member(ctx, "threads", owner); err != nil {
					return err
				} else if !found {
					orphans[owner] = true
				}
			}
		}
		// Even an empty store must satisfy the released legacy shape checks.
		// Include orphan mirrors so their exact admission proof checks cannot
		// disappear when the live threads are prepared independently.
		state, err := loadEffectiveSnapshotScopedState(ctx, source, version, orphans)
		if err != nil {
			return err
		}
		if _, _, err := migrateDecodedMemoryState(state, time.Now); err != nil {
			return err
		}
	}
	return nil
}

// Publication does not retire the source until every durable collection and
// the released root inventory have passed their original authority checks.
func (repo *BackendRepo) verifySnapshotBeforeCleanup(ctx context.Context, manifest backendDomainManifest) error {
	reader := migrationSourceReader{ctx: ctx, backend: repo.backend}
	checkpoint, found, err := readSnapshotImportCheckpoint(reader)
	if err != nil || !found || checkpoint.Version != manifest.SourceVersion {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := verifyIndexedJSONRecord(ctx, reader, backendDomainNamespace, backendStateKey, checkpoint); err != nil {
		return err
	}
	source, err := newSnapshotEffectiveSource(ctx, reader, checkpoint.Version)
	if err != nil {
		return err
	}
	if err := verifySnapshotSourceInventory(ctx, source, checkpoint.Version); err != nil {
		return err
	}
	identities, err := loadSnapshotIdentities(ctx, source, checkpoint.Version)
	if err != nil {
		return err
	}
	format := *repo.migrationSource
	expected, err := snapshotDirectoryRecords(format, identities)
	if err != nil {
		return err
	}
	value, found, err := source.field(ctx, "sequence")
	var sequence int64
	if err != nil || !found {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := value.decode(ctx, reader, &sequence); err != nil {
		return err
	}
	encoded, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: checkpoint.Version, Sequence: sequence})
	if err != nil {
		return err
	}
	expected[string(backendDomainKey(backendDomainRecordManifest))] = encoded
	if err := verifySnapshotDirectory(ctx, reader, format, expected, nil, true); err != nil {
		return err
	}
	repaired, err := snapshotHasFallbackTitleRepair(ctx, source, identities)
	if err != nil {
		return err
	}
	if checkpoint.Version == 5 && !repaired {
		for {
			done := false
			if err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				indexed, err := indexSnapshotRootsBatch(ctx, tx)
				done = indexed.Complete
				return err
			}); err != nil {
				return err
			}
			if done {
				break
			}
		}
		if err := verifySnapshotRootInventory(ctx, source, func() time.Time { return time.Unix(0, manifest.MigrationTime).UTC() }); err != nil {
			return err
		}
	}
	// Decode every durable artifact, including values not referenced by the
	// active path. A filtered closure must not hide a malformed ownership key.
	keys, err := source.members(ctx, "artifacts")
	if err != nil {
		return err
	}
	for _, key := range keys {
		var value artifact.Record
		if err := source.decodeMember(ctx, &value, "artifacts", key); err != nil {
			return err
		}
		if artifactRecordKey(value.ThreadID, value.Ref.ID) != key {
			return ErrAuthorityCorrupt
		}
		if _, found, err := readBackendDomainRecord(reader, repo.domainFormat(), backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0); err != nil || !found {
			if _, deleted, deleteErr := readBackendDomainRecord(reader, repo.domainFormat(), backendDomainKey(backendDomainRecordTombstone, value.ThreadID), backendDomainRecordTombstone, value.ThreadID, "", 0); deleteErr != nil || !deleted {
				return errors.Join(ErrAuthorityCorrupt, err, deleteErr)
			}
		}
	}
	for _, kind := range []string{backendDomainRecordThread, backendDomainRecordTombstone} {
		prefix := backendDomainKey(kind)
		var after []byte
		for {
			page, err := reader.Scan(spi.ScanRequest{Namespace: format.namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
			if err != nil {
				return err
			}
			for _, row := range page.Records {
				if err := verifyMigrationSourceRecord(ctx, reader, format, repo.domainFormat(), row); err != nil {
					return err
				}
			}
			if !page.HasMore {
				break
			}
			if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
				return ErrAuthorityCorrupt
			}
			after = bytes.Clone(page.Next)
		}
	}
	return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		current, err := readV32Manifest(tx)
		if err != nil {
			return err
		}
		if current.SourceVersion != manifest.SourceVersion || current.MigrationPhase != "verify" {
			return spi.ErrConflict
		}
		current.MigrationPhase, current.MigrationCursor = "cleanup", nil
		return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, current)
	})
}

// The released v5 reader repairs missing fallback titles before root inventory
// verification. A repaired checkpoint regenerates that inventory; recovery frames
// cannot coexist with this repair and are already rejected by journal validation.
func snapshotHasFallbackTitleRepair(ctx context.Context, source *snapshotEffectiveSource, identities memoryState) (bool, error) {
	for id, meta := range identities.Threads {
		if meta.Title != "" {
			continue
		}
		value, found, err := source.member(ctx, "entries", id)
		if err != nil {
			return false, err
		}
		if !found {
			continue
		}
		first, repaired := true, false
		if err := visitSnapshotEntries(ctx, source.tx, value, func(entry Entry) error {
			if !first || entry.Type != EntryUserMessage || entry.Message.Role != "user" {
				return nil
			}
			first = false
			_, repaired, err = installFallbackThreadTitle(meta, entry.Message, entry.CreatedAt)
			return err
		}); err != nil {
			return false, err
		}
		if repaired {
			return true, nil
		}
	}
	return false, nil
}

func cleanupSnapshotSourceBatch(ctx context.Context, tx spi.WriteTx) (bool, error) {
	// Keep every original source record until locator cleanup is complete. The
	// final source deletion and source_version reset share one transaction.
	page, err := tx.Scan(spi.ScanRequest{Namespace: snapshotImportNamespace, Limit: backendThreadEntryPageSize})
	if err != nil {
		return false, err
	}
	if len(page.Records) > 0 {
		for _, row := range page.Records {
			if err := tx.Delete(snapshotImportNamespace, row.Key); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	// Journal values may contain entire histories. Locate the first surviving
	// key without materializing frame values, then delete a bounded key batch.
	manifest, err := readV32Manifest(tx)
	if err != nil {
		return false, err
	}
	var cursor uint64
	if len(manifest.MigrationCursor) > 0 {
		// Directory cursors are tuple keys; journal cursors use a separate phase.
		if manifest.MigrationPhase == "snapshot_cleanup" {
			if err := json.Unmarshal(manifest.MigrationCursor, &cursor); err != nil {
				return false, ErrAuthorityCorrupt
			}
		}
	}
	for count := 0; count < backendThreadEntryPageSize; count++ {
		key := backendJournalKey(cursor + 1)
		_, err := historicalRecordSize(tx, backendDomainJournalNamespace, key)
		if errors.Is(err, spi.ErrNotFound) {
			break
		}
		if err != nil {
			return false, err
		}
		if err := tx.Delete(backendDomainJournalNamespace, key); err != nil {
			return false, err
		}
		cursor++
		if count == backendThreadEntryPageSize-1 {
			manifest.MigrationPhase = "snapshot_cleanup"
			manifest.MigrationCursor, _ = json.Marshal(cursor)
			if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	if remaining, err := backendNamespaceHasRecords(tx, backendDomainJournalNamespace); err != nil || remaining {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	for _, key := range [][]byte{backendStateKey, backendRootThreadInventoryKey} {
		if err := tx.Delete(backendDomainNamespace, key); err != nil {
			return false, err
		}
	}
	return false, nil
}
