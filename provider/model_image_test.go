package provider_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func modelPolicy() provider.ModelImageOptions {
	return provider.ModelImageOptions{JPEGQuality: 85, MaxDimension: 2560, MaxSourceBytes: 64 << 20, MaxPixels: 32 << 20}
}
func TestPrepareModelImageIncidentAndDeterminism(t *testing.T) {
	img := image.NewNRGBA(image.Rect(0, 0, 5120, 2880))
	for y := 0; y < 2880; y++ {
		for x := 0; x < 5120; x++ {
			img.SetNRGBA(x, y, color.NRGBA{uint8(x), uint8(y), uint8(x * y), 255})
		}
	}
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			var b bytes.Buffer
			var err error
			if format == "png" {
				err = png.Encode(&b, img)
			} else {
				err = jpeg.Encode(&b, img, &jpeg.Options{Quality: 100})
			}
			if err != nil {
				t.Fatal(err)
			}
			source := b.Bytes()
			hash := sha256.Sum256(source)
			got, err := provider.PrepareModelImage(t.Context(), source, modelPolicy())
			if err != nil {
				t.Fatal(err)
			}
			if got.Width != 2560 || got.Height != 1440 || got.SourceWidth != 5120 || got.SourceHeight != 2880 || got.SHA256 == got.SourceSHA256 {
				t.Fatalf("bad transform: %+v", got.PreparedImage.Width)
			}
			again, err := provider.PrepareModelImage(t.Context(), source, modelPolicy())
			if err != nil || !bytes.Equal(got.Bytes, again.Bytes) || hash != sha256.Sum256(source) {
				t.Fatal("non deterministic or mutated source", err)
			}
		})
	}
}
func TestPrepareModelImageAlphaSmallAndCancellation(t *testing.T) {
	img := image.NewNRGBA(image.Rect(0, 0, 80, 40))
	img.SetNRGBA(0, 0, color.NRGBA{255, 0, 0, 64})
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	p := modelPolicy()
	p.MaxDimension = 40
	got, err := provider.PrepareModelImage(t.Context(), b.Bytes(), p)
	if err != nil || got.MIMEType != "image/png" || got.Width != 40 || got.Height != 20 {
		t.Fatal(got.Width, err)
	}
	decoded, _, err := image.Decode(bytes.NewReader(got.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, alpha := decoded.At(39, 19).RGBA()
	if alpha != 0 {
		t.Fatal("lost transparency")
	}
	p = modelPolicy()
	got, err = provider.PrepareModelImage(t.Context(), b.Bytes(), p)
	if err != nil || got.Width != 80 || got.Height != 40 {
		t.Fatal("upscaled", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = provider.PrepareModelImage(ctx, b.Bytes(), p); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.MaxPixels = 1
	if _, err = provider.PrepareModelImage(t.Context(), b.Bytes(), p); err == nil {
		t.Fatal("unbounded pixels")
	}
	p = modelPolicy()
	p.MaxSourceBytes = 1
	if _, err = provider.PrepareModelImage(t.Context(), b.Bytes(), p); err == nil {
		t.Fatal("unbounded bytes")
	}
	if _, err = provider.PrepareModelImage(t.Context(), []byte("invalid"), modelPolicy()); err == nil {
		t.Fatal("accepted corrupt image")
	}
}
func TestPrepareModelImageAnimationPreserved(t *testing.T) {
	palette := color.Palette{color.Transparent, color.Black, color.White}
	a := image.NewPaletted(image.Rect(0, 0, 20, 10), palette)
	b := image.NewPaletted(a.Bounds(), palette)
	b.SetColorIndex(0, 0, 2)
	var raw bytes.Buffer
	if err := gif.EncodeAll(&raw, &gif.GIF{Image: []*image.Paletted{a, b}, Delay: []int{10, 20}, LoopCount: 3}); err != nil {
		t.Fatal(err)
	}
	got, err := provider.PrepareModelImage(t.Context(), raw.Bytes(), modelPolicy())
	if err != nil || !got.Animated || !bytes.Equal(got.Bytes, raw.Bytes()) {
		t.Fatal("animation changed", err)
	}
	p := modelPolicy()
	p.MaxDimension = 10
	_, err = provider.PrepareModelImage(t.Context(), raw.Bytes(), p)
	var limit *provider.MediaLimitError
	if !errors.As(err, &limit) || limit.Kind != "animated_dimension" {
		t.Fatal(err)
	}
}
func TestPrepareModelImageEXIFOrientation(t *testing.T) {
	img := image.NewNRGBA(image.Rect(0, 0, 60, 40))
	for y := 0; y < 40; y++ {
		for x := 0; x < 60; x++ {
			c := color.NRGBA{255, 0, 0, 255}
			if x >= 30 {
				c = color.NRGBA{0, 0, 255, 255}
			}
			img.SetNRGBA(x, y, c)
		}
	}
	var raw bytes.Buffer
	if err := jpeg.Encode(&raw, img, &jpeg.Options{Quality: 100}); err != nil {
		t.Fatal(err)
	}
	for orientation := 1; orientation <= 8; orientation++ {
		exif := []byte{'E', 'x', 'i', 'f', 0, 0, 'I', 'I', 42, 0, 8, 0, 0, 0, 1, 0, 0x12, 1, 3, 0, 1, 0, 0, 0, byte(orientation), 0, 0, 0, 0, 0, 0, 0}
		segment := []byte{0xff, 0xe1, 0, 0}
		binary.BigEndian.PutUint16(segment[2:], uint16(len(exif)+2))
		source := append(bytes.Clone(raw.Bytes()[:2]), segment...)
		source = append(source, exif...)
		source = append(source, raw.Bytes()[2:]...)
		got, err := provider.PrepareModelImage(t.Context(), source, modelPolicy())
		if err != nil {
			t.Fatal(err)
		}
		w, h := 60, 40
		if orientation >= 5 {
			w, h = h, w
		}
		if got.Width != w || got.Height != h || got.Orientation != orientation {
			t.Fatal("wrong orientation", orientation, got.Width, got.Height)
		}
		decoded, _, err := image.Decode(bytes.NewReader(got.Bytes))
		if err != nil {
			t.Fatal(err)
		}
		r, _, blue, _ := decoded.At(5, 5).RGBA()
		wantRed := orientation == 1 || orientation == 4 || orientation == 5 || orientation == 6
		if (r > blue) != wantRed {
			t.Fatal("wrong oriented pixels", orientation)
		}
	}
}
