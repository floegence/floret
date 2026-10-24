package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
)

// ImagePreparationOptions explicitly opts a host into model-only PNG encoding.
// Zero JPEGQuality preserves the original. No mode changes pixel dimensions.
// MaxSourceBytes and MaxPixels must be positive; MinBytes may be zero.
type ImagePreparationOptions struct {
	JPEGQuality    int
	MinBytes       int64
	MaxSourceBytes int64
	MaxPixels      int64
}

// PreparedImage describes a transient model representation. Bytes are owned by
// the caller. Original identity and pixels remain unchanged in canonical storage.
// JPEG encoding uses Go's standard 4:2:0 encoder; transparent PNGs remain lossless.
type PreparedImage struct {
	Bytes         []byte
	MIMEType      string
	Width, Height int
	SourceBytes   int64
	SourceSHA256  string
	SHA256        string
}

// PrepareImage validates bounded PNG/JPEG input and optionally encodes opaque
// PNGs as JPEG. It retains original bytes unless the candidate is smaller.
// It never resizes, upscales, removes transparency, or recompresses JPEG input.
func PrepareImage(ctx context.Context, source []byte, options ImagePreparationOptions) (PreparedImage, error) {
	if err := ctx.Err(); err != nil {
		return PreparedImage{}, err
	}
	if options.JPEGQuality < 0 || options.JPEGQuality > 100 || options.MinBytes < 0 || options.MaxSourceBytes <= 0 || options.MaxPixels <= 0 {
		return PreparedImage{}, errors.New("invalid image preparation policy")
	}
	if len(source) == 0 || int64(len(source)) > options.MaxSourceBytes {
		return PreparedImage{}, errors.New("image source exceeds preparation byte limit")
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(source))
	if err != nil {
		return PreparedImage{}, fmt.Errorf("image preparation header: %w", err)
	}
	if format != "png" && format != "jpeg" {
		return PreparedImage{}, errors.New("image preparation supports PNG and JPEG")
	}
	if config.Width <= 0 || config.Height <= 0 || int64(config.Width) > options.MaxPixels/int64(config.Height) {
		return PreparedImage{}, errors.New("image source exceeds preparation pixel limit")
	}
	decoded, _, err := image.Decode(bytes.NewReader(source))
	if err != nil {
		return PreparedImage{}, fmt.Errorf("image preparation pixels: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return PreparedImage{}, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(source))
	out := PreparedImage{Bytes: bytes.Clone(source), MIMEType: "image/" + format, Width: config.Width, Height: config.Height, SourceBytes: int64(len(source)), SourceSHA256: hash, SHA256: hash}
	if options.JPEGQuality == 0 || int64(len(source)) < options.MinBytes || format != "png" {
		return out, nil
	}
	opaque, ok := decoded.(interface{ Opaque() bool })
	if !ok || !opaque.Opaque() {
		return out, nil
	}
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, decoded, &jpeg.Options{Quality: options.JPEGQuality}); err != nil {
		return PreparedImage{}, err
	}
	if err := ctx.Err(); err != nil {
		return PreparedImage{}, err
	}
	if encoded.Len() < len(source) {
		out.Bytes = encoded.Bytes()
		out.MIMEType = "image/jpeg"
		out.SHA256 = fmt.Sprintf("%x", sha256.Sum256(out.Bytes))
	}
	return out, nil
}
