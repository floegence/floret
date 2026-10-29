package provider

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"image"
	"image/png"
	"testing"
)

func TestModelImageProfileRoundTrip(t *testing.T) {
	var source bytes.Buffer
	img := image.NewNRGBA(image.Rect(0, 0, 30, 20))
	if err := png.Encode(&source, img); err != nil {
		t.Fatal(err)
	}
	profile := bytes.Repeat([]byte("synthetic-profile"), 5000)
	for _, mime := range []string{"image/png", "image/jpeg"} {
		data := source.Bytes()
		format := "png"
		if mime == "image/jpeg" {
			data = []byte{0xff, 0xd8, 0xff, 0xd9}
			format = "jpeg"
		}
		encoded, err := modelImageWithProfile(data, mime, profile)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := modelImageColorProfile(encoded, format)
		if err != nil || !bytes.Equal(actual, profile) {
			t.Fatal("ICC profile lost", mime, err)
		}
	}
	encoded, _ := modelImageWithProfile(source.Bytes(), "image/png", profile)
	got, err := PrepareModelImage(t.Context(), encoded, ModelImageOptions{JPEGQuality: 85, MaxDimension: 10, MaxSourceBytes: 1 << 20, MaxPixels: 10000})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := modelImageColorProfile(got.Bytes, "png")
	if err != nil || !bytes.Equal(actual, profile) {
		t.Fatal("transformed profile lost", err)
	}
}
func TestModelImagePreservesAPNGAndAnimatedWebP(t *testing.T) {
	var source bytes.Buffer
	if err := png.Encode(&source, image.NewNRGBA(image.Rect(0, 0, 30, 20))); err != nil {
		t.Fatal(err)
	}
	// Container markers establish that the transformer must not flatten frames.
	chunk := []byte{'a', 'c', 'T', 'L', 0, 0, 0, 2, 0, 0, 0, 0}
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, 8)
	suffix := make([]byte, 4)
	binary.BigEndian.PutUint32(suffix, crc32.ChecksumIEEE(chunk))
	apng := append(bytes.Clone(source.Bytes()[:33]), prefix...)
	apng = append(apng, chunk...)
	apng = append(apng, suffix...)
	apng = append(apng, source.Bytes()[33:]...)
	webp := []byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P', 'V', 'P', '8', 'X', 10, 0, 0, 0, 2, 0, 0, 0, 29, 0, 0, 19, 0, 0, 'A', 'N', 'I', 'M', 6, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(webp[4:8], uint32(len(webp)-8))
	for _, data := range [][]byte{apng, webp} {
		got, err := PrepareModelImage(t.Context(), data, ModelImageOptions{JPEGQuality: 85, MaxDimension: 40, MaxSourceBytes: 1 << 20, MaxPixels: 10000})
		if err != nil || !got.Animated || !bytes.Equal(got.Bytes, data) {
			t.Fatal("animation flattened", err)
		}
	}
}
