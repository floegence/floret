package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32ArtifactReadAndClosureUseCanonicalFacts(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	request := canonicalEffectRequest(attempt)
	request.FullOutput = &artifact.FullOutput{Text: "Complete canonical output"}
	finished, err := repo.FinishEffectDispatch(t.Context(), request)
	if err != nil || finished.Artifact == nil {
		t.Fatalf("finish=%+v, %v", finished, err)
	}
	repo.backend = artifactDirectReadBackend{backend}
	content, err := repo.ReadArtifact(t.Context(), ArtifactReadRequest{ThreadID: "thread", ArtifactID: finished.Artifact.ID})
	if err != nil || content.Text != request.FullOutput.Text || !reflect.DeepEqual(content.Ref, *finished.Artifact) {
		t.Fatalf("artifact=%+v, %v", content, err)
	}
	closure, err := repo.ArtifactClosure(t.Context(), ArtifactClosureRequest{SourceThreadID: "thread", DestinationThreadID: "fork", EntryIDs: []string{finished.Result.ID}})
	if err != nil || len(closure.Items) != 1 || closure.Items[0].SourceEntryID != finished.Result.ID {
		t.Fatalf("closure=%+v, %v", closure, err)
	}
	if repo.threadMemory != nil || repo.domainMemory != nil {
		t.Fatal("artifact read installed a MemoryRepo")
	}
	if _, err := repo.ReadArtifact(t.Context(), ArtifactReadRequest{ThreadID: "thread", ParentThreadID: "thread", ArtifactID: finished.Artifact.ID}); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("self parent=%v", err)
	}
	repo.backend = backend
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "child", ParentThreadID: "thread", TaskName: "child", HostProfileRef: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadArtifact(t.Context(), ArtifactReadRequest{ThreadID: "child", ArtifactID: "missing"}); !errors.Is(err, ErrSubAgentParentRequired) {
		t.Fatalf("child without parent=%v", err)
	}
	if _, err := repo.ReadArtifact(t.Context(), ArtifactReadRequest{ParentThreadID: "thread", ThreadID: "child", ArtifactID: "missing"}); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("authorized child=%v", err)
	}
}

type artifactDirectReadBackend struct{ spi.Backend }

func (b artifactDirectReadBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return read(artifactDirectReadTx{tx}) })
}

type artifactDirectReadTx struct{ spi.ReadTx }

func (tx artifactDirectReadTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == canonicalBodyNamespace {
		return nil, errors.New("artifact read hydrated conversation body")
	}
	return tx.ReadTx.Get(namespace, key)
}

func (tx artifactDirectReadTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	return spi.ScanPage{}, errors.New("artifact read scanned canonical history")
}
