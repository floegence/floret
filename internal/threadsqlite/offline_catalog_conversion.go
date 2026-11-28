//go:build floret_layout_cutover

package threadsqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const directoryRecordKinds = `'floret.domain.prompt.v2','floret.domain.sessiontree.locators.v32','floret.domain.sessiontree.outputs.v39','floret.domain.sessiontree.v17','floret.layout.prompt-owner.v1','floret.sessiontree.child-membership.v1','floret.system','floret.query.documents.v2'`
const memberRecordKinds = `'floret.domain.prompt.v2','floret.domain.sessiontree.bodies.v30','floret.domain.sessiontree.locators.v32','floret.domain.sessiontree.outputs.v39','floret.domain.sessiontree.v17','floret.prompt.lookups.v1','floret.prompt.request-locators.v1','floret.query.documents.v2','floret.query.manifests.v1','floret.sessiontree.activity-fields.v6','floret.sessiontree.body-text-facts.v1','floret.sessiontree.context-source-metadata.v1','floret.sessiontree.item-membership.v8','floret.sessiontree.projection-sources.v7','floret.sessiontree.sparse-sources.v3'`

func verifyOfflineNamespace(ctx context.Context, tx *sql.Tx, catalog bool) error {
	kinds, table := memberRecordKinds, "main.floret_backend_records"
	if catalog {
		kinds, table = directoryRecordKinds, "source_catalog.floret_backend_records"
	}
	var unknown string
	err := tx.QueryRowContext(ctx, `SELECT namespace FROM `+table+` WHERE namespace NOT IN (`+kinds+`) LIMIT 1`).Scan(&unknown)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("unclassified namespace %q: %w", unknown, ErrFileFormat)
}

func PrepareMemberCatalog(ctx context.Context, source *Backend, destination string) (result *Backend, err error) {
	if source == nil || source.Catalog == nil || source.Files == nil || source.UsesMemberJournals() || !filepath.IsAbs(destination) {
		return nil, spi.ErrInvalidArgument
	}
	owner, err := createCatalogForStore(ctx, destination, "7", source.Catalog.Store)
	if err != nil {
		return nil, err
	}
	files, err := NewMemberFiles(destination+".d", owner.Store, 4, 1, time.Minute)
	if err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	candidate := &Backend{ThreadViews: ThreadViews{Catalog: owner, Files: files}}
	defer func() {
		if err != nil {
			err = errors.Join(err, candidate.Close())
		}
	}()
	release, err := source.Catalog.acquireOwner(ctx, false)
	if err != nil {
		return nil, err
	}
	defer release()
	if _, err := owner.owner.ExecContext(ctx, `ATTACH DATABASE ? AS source_catalog`, databaseURI(source.Catalog.path, url.Values{"mode": {"ro"}})); err != nil {
		return nil, err
	}
	defer func() {
		_, detach := owner.owner.ExecContext(context.WithoutCancel(ctx), `DETACH DATABASE source_catalog`)
		if detach != nil {
			result = nil
			err = errors.Join(err, detach)
		}
	}()
	err = candidate.catalogTransaction(ctx, false, func(tx *sql.Tx) error {
		var ok bool
		if err := tx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM source_catalog.floret_thread_staging) AND (SELECT count(*) FROM source_catalog.floret_backend_metadata)=2 AND (SELECT value FROM source_catalog.floret_backend_metadata WHERE name='store')=(SELECT value FROM main.floret_backend_metadata WHERE name='store') AND (SELECT value FROM source_catalog.floret_backend_metadata WHERE name='physical_schema')=CAST('6' AS BLOB)`).Scan(&ok); err != nil || !ok {
			return errors.Join(ErrFileIdentity, err)
		}
		if err := verifyOfflineNamespace(ctx, tx, true); err != nil {
			return err
		}
		for _, table := range []string{"floret_backend_records", "floret_thread_decisions", "floret_thread_routes"} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO main.`+table+` SELECT * FROM source_catalog.`+table); err != nil {
				return err
			}
			if err := tx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT * FROM source_catalog.`+table+` EXCEPT SELECT * FROM main.`+table+`) AND (SELECT count(*) FROM source_catalog.`+table+`)=(SELECT count(*) FROM main.`+table+`)`).Scan(&ok); err != nil || !ok {
				return errors.Join(ErrFileIdentity, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return candidate, nil
}
