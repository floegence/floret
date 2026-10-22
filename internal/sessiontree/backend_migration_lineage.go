package sessiontree

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

// One ordered lineage owns every released transform. Source loaders retain
// their exact version validation; this sequence never substitutes the latest
// validator for the source contract.
type historicalDomainLineage struct {
	version int
	load    func(context.Context, spi.ReadTx, func() time.Time) (*MemoryRepo, bool, error)
	remove  func(context.Context, spi.WriteTx) error
}

func historicalSegmentedLineage(source backendDomainSource) (historicalDomainLineage, bool) {
	switch source {
	case backendDomainSourceV6:
		return historicalDomainLineage{6, loadBackendDomainV6, deleteAllBackendDomainV6}, true
	case backendDomainSourceV7:
		return historicalDomainLineage{7, loadBackendDomainV7, deleteAllBackendDomainV7}, true
	case backendDomainSourceV8:
		return historicalDomainLineage{8, loadBackendDomainV8, deleteAllBackendDomainV8}, true
	case backendDomainSourceV9:
		return historicalDomainLineage{9, loadBackendDomainV9, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV9Format)
		}}, true
	case backendDomainSourceV10:
		return historicalDomainLineage{10, loadBackendDomainV10, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV10Format)
		}}, true
	case backendDomainSourceV11:
		return historicalDomainLineage{11, loadBackendDomainV11, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV11Format)
		}}, true
	case backendDomainSourceV12:
		return historicalDomainLineage{12, loadBackendDomainV12, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV12Format)
		}}, true
	case backendDomainSourceV13:
		return historicalDomainLineage{13, loadBackendDomainV13, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV13Format)
		}}, true
	case backendDomainSourceV14:
		return historicalDomainLineage{14, loadBackendDomainV14, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV14Format)
		}}, true
	case backendDomainSourceV15:
		return historicalDomainLineage{15, loadBackendDomainV15, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV15Format)
		}}, true
	case backendDomainSourceV16:
		return historicalDomainLineage{16, loadBackendDomainV16, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV16Format)
		}}, true
	case backendDomainSourceV17:
		return historicalDomainLineage{17, loadBackendDomainV17, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV17Format)
		}}, true
	case backendDomainSourceV18:
		return historicalDomainLineage{18, loadBackendDomainV18, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV18Format)
		}}, true
	case backendDomainSourceV19:
		return historicalDomainLineage{19, loadBackendDomainV19, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV19Format)
		}}, true
	case backendDomainSourceV20:
		return historicalDomainLineage{20, loadBackendDomainV20, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV20Format)
		}}, true
	case backendDomainSourceV21:
		return historicalDomainLineage{21, loadBackendDomainV21, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV21Format)
		}}, true
	case backendDomainSourceV22:
		return historicalDomainLineage{22, loadBackendDomainV22, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV22Format)
		}}, true
	case backendDomainSourceV23:
		return historicalDomainLineage{23, loadBackendDomainV23, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV23Format)
		}}, true
	case backendDomainSourceV24:
		return historicalDomainLineage{24, loadBackendDomainV24, func(ctx context.Context, tx spi.WriteTx) error {
			return deleteAllBackendDomain(ctx, tx, backendDomainV24Format)
		}}, true
	case backendDomainSourceV25:
		return historicalDomainLineage{25, loadBackendDomainV25, nil}, true
	case backendDomainSourceV26:
		return historicalDomainLineage{26, loadBackendDomainV26, nil}, true
	case backendDomainSourceV27:
		return historicalDomainLineage{27, loadBackendDomainV27, nil}, true
	case backendDomainSourceV28:
		return historicalDomainLineage{28, loadBackendDomainV28, nil}, true
	default:
		return historicalDomainLineage{}, false
	}
}

func migrateHistoricalMemoryThroughV29(ctx context.Context, memory *MemoryRepo, version int) error {
	if ctx == nil || memory == nil || version < 6 || version > 29 {
		return errors.New("historical migration requires exact source version and memory")
	}
	edges := [...]func(context.Context, *MemoryRepo) error{
		6:  migrateBackendDomainV6ToV7,
		7:  migrateBackendDomainV7ToV8,
		8:  migrateBackendDomainV8ToV9,
		9:  migrateBackendDomainV9ToV10,
		10: migrateBackendDomainV10ToV11,
		11: migrateBackendDomainV11ToV12,
		12: migrateBackendDomainV12ToV13,
		13: migrateBackendDomainV13ToV14,
		14: migrateBackendDomainV14ToV15,
		15: migrateBackendDomainV15ToV16,
		16: migrateBackendDomainV16ToV17,
		17: migrateBackendDomainV17ToV18,
		18: migrateBackendDomainV18ToV19,
		19: migrateBackendDomainV19ToV20,
		20: migrateBackendDomainV20ToV21,
		21: migrateBackendDomainV21ToV22,
		22: migrateBackendDomainV22ToV23,
		23: migrateBackendDomainV23ToV24,
		24: migrateBackendDomainV24ToV25,
		25: migrateBackendDomainV25ToV26,
		26: migrateBackendDomainV26ToV27,
		27: migrateBackendDomainV27ToV28,
		28: migrateBackendDomainV28ToV29,
	}
	// Preserve the released unknown-effect boundary. v6 owns its convergence
	// inside the 6 -> 7 edge; v7 needs no additional convergence. Later sources
	// settle uncertainty only after the same validated edge as their writers.
	convergeAt := 0
	switch {
	case version == 8:
		convergeAt = 9
	case version >= 9:
		convergeAt = max(25, version+1)
	}
	for next := version; next < 29; next++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := edges[next](ctx, memory); err != nil {
			return err
		}
		if next+1 == convergeAt {
			memory.parentEvents = parentEventsEnabled(ctx)
			if err := convergeUnknownEffectTurns(ctx, memory); err != nil {
				return err
			}
		}
	}
	return nil
}
