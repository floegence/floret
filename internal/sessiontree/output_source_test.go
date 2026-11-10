package sessiontree

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOutputSourcePagesPreserveUnicodeAndChecksums(t *testing.T) {
	backend := newMigrationTestBackend()
	text := "FIRST\n" + strings.Repeat("a中文🌸é\r\n", 40000) + "LAST"
	var source OutputSource
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		source, err = writeOutputSource(t.Context(), tx, "test", text)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		for _, size := range []int{8000, 65536} {
			hash := sha256.New()
			for offset := int64(0); offset < source.Bytes; {
				page, err := readOutputSourcePage(t.Context(), tx, source, offset, size)
				if err != nil {
					return err
				}
				if len(page.Text) == 0 || len(page.Text) > size || !utf8.ValidString(page.Text) || page.NextOffset <= offset {
					return fmt.Errorf("invalid page at %d", offset)
				}
				hash.Write([]byte(page.Text))
				offset = page.NextOffset
			}
			if fmt.Sprintf("%x", hash.Sum(nil)) != source.SHA256 {
				t.Fatal("source bytes changed")
			}
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := readOutputSourcePage(ctx, tx, source, 0, 8000); !errors.Is(err, context.Canceled) {
			t.Fatalf("ignored cancellation: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOutputSourceQuotaRetainsPrefixWithoutEviction(t *testing.T) {
	backend := newMigrationTestBackend()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		source, err := newOutputSource(tx, "quota")
		if err != nil {
			return err
		}
		source, err = appendOutputSource(t.Context(), tx, source, "retained")
		if err != nil {
			return err
		}
		if err := putOutputRecord(tx, outputUsageKey(), outputStoreMaxBytes); err != nil {
			return err
		}
		source, err = appendOutputSource(t.Context(), tx, source, "not retained")
		if err != nil {
			return err
		}
		source, err = sealOutputSource(tx, source, "")
		if err != nil {
			return err
		}
		if source.Reason != "storage_limit" || source.Bytes != 8 || source.ObservedBytes != 20 || source.Chunks != 1 {
			t.Fatalf("incorrect completeness: %+v", source)
		}
		text, err := materializeOutputSource(t.Context(), tx, source)
		if err != nil {
			return err
		}
		if text != "retained" {
			t.Fatal("prefix was removed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLargeArtifactRangesForkAndDelete(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	req := canonicalEffectRequest(attempt)
	text := "BEGIN\n" + strings.Repeat("middle 世界🙂\n", 12000) + "END"
	req.FullOutput = &artifact.FullOutput{Text: text}
	result, err := repo.FinishEffectDispatch(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	var body strings.Builder
	for offset := int64(0); offset < int64(len(text)); {
		page, err := repo.ReadArtifactPage(t.Context(), ArtifactPageRequest{ArtifactReadRequest: ArtifactReadRequest{ThreadID: "thread", ArtifactID: result.Artifact.ID}, Offset: offset, MaxBytes: 8000})
		if err != nil {
			t.Fatal(err)
		}
		if !page.Complete || len(page.Text) > 8000 || page.NextOffset <= offset {
			t.Fatal("invalid page")
		}
		body.WriteString(page.Text)
		offset = page.NextOffset
	}
	if body.String() != text {
		t.Fatal("range reads lost source")
	}
	if _, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ReadArtifact(t.Context(), ArtifactReadRequest{ThreadID: "fork", ArtifactID: result.Artifact.ID})
	if err != nil || got.Text != text {
		t.Fatalf("fork output: %v", err)
	}
	if _, err := repo.FinishEffectDispatch(t.Context(), req); err != nil {
		t.Fatalf("committed result replay: %v", err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ReadArtifactPage(t.Context(), ArtifactPageRequest{ArtifactReadRequest: ArtifactReadRequest{ThreadID: "fork", ArtifactID: result.Artifact.ID}, MaxBytes: 8000})
	if err != nil || !strings.HasPrefix(page.Text, "BEGIN") {
		t.Fatal("source deletion lost inherited artifact", err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "fork"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		used, err := outputStoreUsage(tx)
		if used != 0 {
			t.Fatal("deleted output quota leaked")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
