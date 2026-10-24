package provider_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"github.com/floegence/floret/v7/provider"
	"image"
	"image/color"
	"image/png"
	"math/rand/v2"
	"testing"
)

func TestPrepareImagePreservesSourceAndDimensions(t *testing.T) {
	img := image.NewNRGBA(image.Rect(0, 0, 512, 256))
	rng := rand.New(rand.NewPCG(1, 2))
	for y := 0; y < 256; y++ {
		for x := 0; x < 512; x++ {
			img.SetNRGBA(x, y, color.NRGBA{uint8(rng.Uint32()), uint8(rng.Uint32()), uint8(rng.Uint32()), 255})
		}
	}
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	source := b.Bytes()
	before := sha256.Sum256(source)
	policy := provider.ImagePreparationOptions{JPEGQuality: 90, MaxSourceBytes: 16 << 20, MaxPixels: 32 << 20}
	got, err := provider.PrepareImage(t.Context(), source, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got.MIMEType != "image/jpeg" || len(got.Bytes) >= len(source) || got.Width != 512 || got.Height != 256 || before != sha256.Sum256(source) {
		t.Fatalf("unexpected image: mime=%s size=%d source=%d dimensions=%dx%d", got.MIMEType, len(got.Bytes), len(source), got.Width, got.Height)
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(got.Bytes))
	if err != nil || format != "jpeg" || config.Width != 512 || config.Height != 256 {
		t.Fatal(config, format, err)
	}
	again, err := provider.PrepareImage(t.Context(), source, policy)
	if err != nil || !bytes.Equal(got.Bytes, again.Bytes) {
		t.Fatal("non-deterministic preparation", err)
	}
	policy.JPEGQuality = 0
	original, err := provider.PrepareImage(t.Context(), source, policy)
	if err != nil || !bytes.Equal(original.Bytes, source) || original.SHA256 != original.SourceSHA256 {
		t.Fatal("original mode changed content", err)
	}
	original.Bytes[0] ^= 1
	if before != sha256.Sum256(source) {
		t.Fatal("output aliases input")
	}
	policy.JPEGQuality = 90
	jpeg, err := provider.PrepareImage(t.Context(), got.Bytes, policy)
	if err != nil || !bytes.Equal(jpeg.Bytes, got.Bytes) {
		t.Fatal("JPEG was recompressed", err)
	}
	img.SetNRGBA(0, 0, color.NRGBA{})
	b.Reset()
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	transparent, err := provider.PrepareImage(t.Context(), b.Bytes(), policy)
	if err != nil || !bytes.Equal(transparent.Bytes, b.Bytes()) {
		t.Fatal("transparency changed", err)
	}
	policy.MaxPixels = 1
	if _, err = provider.PrepareImage(t.Context(), source, policy); err == nil {
		t.Fatal("unbounded pixels")
	}
	policy.MaxPixels = 32 << 20
	policy.MaxSourceBytes = 1
	if _, err = provider.PrepareImage(t.Context(), source, policy); err == nil {
		t.Fatal("unbounded bytes")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = provider.PrepareImage(ctx, source, policy); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExactRequestByteLimit(t *testing.T) {
	payload := []byte("中文 base64:AAAA")
	if err := provider.CheckRequestBytes(payload, int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	err := provider.CheckRequestBytes(payload, int64(len(payload)-1))
	var size *provider.RequestByteLimitError
	if !errors.As(err, &size) || !errors.Is(err, provider.ErrContextOverflow) || size.ActualBytes != int64(len(payload)) {
		t.Fatal(err)
	}
	if err := provider.CheckRequestBytes(payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := provider.CheckRequestBytes(payload, -1); err == nil {
		t.Fatal("negative policy accepted")
	}
}
