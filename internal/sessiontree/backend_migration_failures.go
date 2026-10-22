package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/floegence/floret/v7/storage/spi"
)

const backendMigrationFailureNamespace = "floret.domain.sessiontree.migration-failures.v32"

type threadMigrationFailure struct {
	ThreadID string `json:"thread_id"`
	Error    string `json:"error"`
}

func readThreadMigrationFailure(tx spi.ReadTx, threadID string) error {
	raw, err := tx.Get(backendMigrationFailureNamespace, []byte(threadID))
	if errors.Is(err, spi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var failure threadMigrationFailure
	if err := decodeCanonicalBodyJSON(raw, &failure); err != nil || failure.ThreadID != threadID || failure.Error == "" {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return fmt.Errorf("%w: thread %q migration failed: %s", ErrAuthorityCorrupt, threadID, failure.Error)
}

func (repo *BackendRepo) isolateMigrationFailure(ctx context.Context, threadID string, failure error) error {
	if threadID == "" || !errors.Is(failure, ErrAuthorityCorrupt) {
		return failure
	}
	return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := writeThreadMigrationFailure(tx, threadID, failure); err != nil {
			return err
		}
		manifest, err := readV32Manifest(tx)
		if err != nil {
			return err
		}
		manifest.MigrationPhase = "publish"
		manifest.MigrationCursor = backendDomainKey(backendDomainRecordThread, threadID)
		return putBackendDomainRecordContext(ctx, tx, repo.domainFormat(), backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest)
	})
}

func migrationFailureRemaining(tx spi.ReadTx) error {
	page, err := tx.Scan(spi.ScanRequest{Namespace: backendMigrationFailureNamespace, Limit: 1})
	if err != nil {
		return err
	}
	if len(page.Records) == 0 {
		return nil
	}
	return readThreadMigrationFailure(tx, string(page.Records[0].Key))
}

func clearThreadMigrationFailure(tx spi.WriteTx, threadID string) error {
	if _, err := tx.Get(backendMigrationFailureNamespace, []byte(threadID)); errors.Is(err, spi.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	return tx.Delete(backendMigrationFailureNamespace, []byte(threadID))
}

func writeThreadMigrationFailure(tx spi.WriteTx, threadID string, failure error) error {
	message := failure.Error()
	if len(message) > 4096 {
		end := 4096
		for end > 0 && !utf8.RuneStart(message[end]) {
			end--
		}
		message = message[:end] + "..."
	}
	raw, err := json.Marshal(threadMigrationFailure{ThreadID: threadID, Error: message})
	if err != nil {
		return err
	}
	return tx.Put(backendMigrationFailureNamespace, []byte(threadID), raw)
}
