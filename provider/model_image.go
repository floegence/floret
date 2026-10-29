package provider

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
)

// ModelImageOptions explicitly selects a transient representation of authorized
// source bytes. All limits must be positive and JPEGQuality must be 1..100.
// MaxDimension bounds the longer side without upscaling. Animation is retained
// unchanged when it fits; an oversized animation fails instead of losing frames.
type ModelImageOptions struct {
	JPEGQuality    int
	MaxDimension   int
	MaxSourceBytes int64
	MaxPixels      int64
}

// ModelImage retains both source identity and the actual model representation.
// Source dimensions precede EXIF orientation. Orientation is the source EXIF
// orientation (1..8); output pixels are upright. Animated bytes remain unchanged.
type ModelImage struct {
	PreparedImage
	SourceWidth, SourceHeight int
	Orientation               int
	Animated                  bool
}

// PrepareModelImage validates PNG, JPEG, GIF and WebP and prepares a bounded,
// deterministic model representation. It never mutates source, removes alpha,
// upscales, or silently selects an animation frame. The host owns authorization,
// product defaults, coordinate mapping and provider-specific final validation.
// Always pass original bytes, never an earlier lossy representation.
func PrepareModelImage(ctx context.Context, source []byte, options ModelImageOptions) (ModelImage, error) {
	if err := ctx.Err(); err != nil {
		return ModelImage{}, err
	}
	if options.JPEGQuality < 1 || options.JPEGQuality > 100 || options.MaxDimension < 1 || options.MaxSourceBytes < 1 || options.MaxPixels < 1 {
		return ModelImage{}, errors.New("invalid model image policy")
	}
	if len(source) == 0 || int64(len(source)) > options.MaxSourceBytes {
		return ModelImage{}, errors.New("model image source exceeds byte limit")
	}
	cfg, format, err := image.DecodeConfig(bytes.NewReader(source))
	if err != nil {
		return ModelImage{}, fmt.Errorf("model image header: %w", err)
	}
	if format != "png" && format != "jpeg" && format != "gif" && format != "webp" {
		return ModelImage{}, errors.New("unsupported model image format")
	}
	if cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width) > options.MaxPixels/int64(cfg.Height) {
		return ModelImage{}, errors.New("model image source exceeds pixel limit")
	}
	orientation, animated, err := modelImageMetadata(source, format)
	if err != nil {
		return ModelImage{}, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(source))
	out := ModelImage{PreparedImage: PreparedImage{Bytes: bytes.Clone(source), MIMEType: "image/" + format, Width: cfg.Width, Height: cfg.Height, SourceBytes: int64(len(source)), SourceSHA256: hash, SHA256: hash}, SourceWidth: cfg.Width, SourceHeight: cfg.Height, Orientation: orientation, Animated: animated}
	profile, err := modelImageColorProfile(source, format)
	if err != nil {
		return ModelImage{}, err
	}
	if animated {
		if max(cfg.Width, cfg.Height) > options.MaxDimension {
			return ModelImage{}, &MediaLimitError{Kind: "animated_dimension", Actual: int64(max(cfg.Width, cfg.Height)), Limit: int64(options.MaxDimension)}
		}
		if err := ctx.Err(); err != nil {
			return ModelImage{}, err
		}
		return out, nil
	}
	decoded, _, err := image.Decode(bytes.NewReader(source))
	if err != nil {
		return ModelImage{}, fmt.Errorf("model image pixels: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return ModelImage{}, err
	}
	if orientation != 1 {
		decoded = orientModelImage(decoded, orientation)
	}
	width, height := decoded.Bounds().Dx(), decoded.Bounds().Dy()
	longest := max(width, height)
	if longest > options.MaxDimension {
		width = max(1, int(int64(width)*int64(options.MaxDimension)/int64(longest)))
		height = max(1, int(int64(height)*int64(options.MaxDimension)/int64(longest)))
		scaled := image.NewNRGBA(image.Rect(0, 0, width, height))
		draw.CatmullRom.Scale(scaled, scaled.Bounds(), decoded, decoded.Bounds(), draw.Src, nil)
		decoded = scaled
	}
	if err := ctx.Err(); err != nil {
		return ModelImage{}, err
	}
	var candidate bytes.Buffer
	opaque, ok := decoded.(interface{ Opaque() bool })
	mime := "image/png"
	if ok && opaque.Opaque() {
		mime = "image/jpeg"
		err = jpeg.Encode(&candidate, decoded, &jpeg.Options{Quality: options.JPEGQuality})
	} else {
		err = png.Encode(&candidate, decoded)
	}
	if err != nil {
		return ModelImage{}, err
	}
	if err := ctx.Err(); err != nil {
		return ModelImage{}, err
	}
	encoded, err := modelImageWithProfile(candidate.Bytes(), mime, profile)
	if err != nil {
		return ModelImage{}, err
	}
	if width != cfg.Width || height != cfg.Height || orientation != 1 || len(encoded) < len(source) {
		out.Bytes = encoded
		out.MIMEType = mime
		out.Width = width
		out.Height = height
		out.SHA256 = fmt.Sprintf("%x", sha256.Sum256(out.Bytes))
	}
	return out, nil
}

func orientModelImage(src image.Image, orientation int) image.Image {
	w, h := src.Bounds().Dx(), src.Bounds().Dy()
	dw, dh := w, h
	if orientation >= 5 {
		dw, dh = h, w
	}
	out := image.NewNRGBA(image.Rect(0, 0, dw, dh))
	for y := 0; y < dh; y++ {
		for x := 0; x < dw; x++ {
			sx, sy := x, y
			switch orientation {
			case 2:
				sx = w - 1 - x
			case 3:
				sx, sy = w-1-x, h-1-y
			case 4:
				sy = h - 1 - y
			case 5:
				sx, sy = y, x
			case 6:
				sx, sy = y, h-1-x
			case 7:
				sx, sy = w-1-y, h-1-x
			case 8:
				sx, sy = w-1-y, x
			}
			out.Set(x, y, src.At(src.Bounds().Min.X+sx, src.Bounds().Min.Y+sy))
		}
	}
	return out
}

// Metadata parsing never allocates based on untrusted chunk or frame counts.
func modelImageMetadata(data []byte, format string) (int, bool, error) {
	orientation := 1
	animated := false
	readEXIF := func(b []byte) error {
		if bytes.HasPrefix(b, []byte("Exif\x00\x00")) {
			b = b[6:]
		}
		o, err := modelImageOrientation(b)
		if err == nil {
			orientation = o
		}
		return err
	}
	switch format {
	case "jpeg":
		for pos := 2; pos < len(data); {
			if data[pos] != 0xff {
				return 0, false, errors.New("invalid JPEG marker")
			}
			for pos < len(data) && data[pos] == 0xff {
				pos++
			}
			if pos >= len(data) {
				break
			}
			marker := data[pos]
			pos++
			if marker == 0xda || marker == 0xd9 {
				break
			}
			if marker == 0x01 || (marker >= 0xd0 && marker <= 0xd7) {
				continue
			}
			if pos+2 > len(data) {
				return 0, false, errors.New("invalid JPEG segment")
			}
			n := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			if n < 2 || n > len(data)-pos {
				return 0, false, errors.New("invalid JPEG segment")
			}
			if marker == 0xe1 && bytes.HasPrefix(data[pos+2:pos+n], []byte("Exif\x00\x00")) {
				if err := readEXIF(data[pos+2 : pos+n]); err != nil {
					return 0, false, err
				}
			}
			pos += n
		}
	case "png":
		for pos := 8; pos+12 <= len(data); {
			n := uint64(binary.BigEndian.Uint32(data[pos : pos+4]))
			if n > uint64(len(data)-pos-12) {
				return 0, false, errors.New("invalid PNG chunk")
			}
			end := pos + 8 + int(n)
			switch string(data[pos+4 : pos+8]) {
			case "acTL":
				animated = true
			case "eXIf":
				if err := readEXIF(data[pos+8 : end]); err != nil {
					return 0, false, err
				}
			}
			pos = end + 4
		}
	case "webp":
		for pos := 12; pos+8 <= len(data); {
			n := uint64(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
			if n > uint64(len(data)-pos-8) {
				return 0, false, errors.New("invalid WebP chunk")
			}
			end := pos + 8 + int(n)
			switch string(data[pos : pos+4]) {
			case "ANIM", "ANMF":
				animated = true
			case "EXIF":
				if err := readEXIF(data[pos+8 : end]); err != nil {
					return 0, false, err
				}
			}
			pos = end + int(n&1)
		}
	case "gif":
		// Decode only the first frame; the block scanner detects additional frames
		// without allocating every frame of a potentially large animation.
		if _, err := gif.Decode(bytes.NewReader(data)); err != nil {
			return 0, false, err
		}
		var err error
		animated, err = modelGIFAnimated(data)
		if err != nil {
			return 0, false, err
		}
	}
	return orientation, animated, nil
}

func modelGIFAnimated(b []byte) (bool, error) {
	bad := errors.New("invalid GIF blocks")
	if len(b) < 13 {
		return false, bad
	}
	pos := 13
	if b[10]&128 != 0 {
		pos += 3 * (1 << ((b[10] & 7) + 1))
	}
	frames := 0
	skip := func() bool {
		for pos < len(b) {
			n := int(b[pos])
			pos++
			if n == 0 {
				return true
			}
			if n > len(b)-pos {
				return false
			}
			pos += n
		}
		return false
	}
	for pos < len(b) {
		kind := b[pos]
		pos++
		switch kind {
		case 0x3b:
			return frames > 1, nil
		case 0x21:
			if pos >= len(b) {
				return false, bad
			}
			pos++
			if !skip() {
				return false, bad
			}
		case 0x2c:
			if len(b)-pos < 9 {
				return false, bad
			}
			packed := b[pos+8]
			pos += 9
			if packed&128 != 0 {
				pos += 3 * (1 << ((packed & 7) + 1))
			}
			if pos >= len(b) {
				return false, bad
			}
			pos++
			if !skip() {
				return false, bad
			}
			frames++
		default:
			return false, bad
		}
	}
	return false, bad
}

func modelImageOrientation(b []byte) (int, error) {
	bad := errors.New("invalid image EXIF orientation metadata")
	if len(b) < 8 {
		return 0, bad
	}
	var order binary.ByteOrder
	switch string(b[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 0, bad
	}
	if order.Uint16(b[2:4]) != 42 {
		return 0, bad
	}
	offset := uint64(order.Uint32(b[4:8]))
	if offset > uint64(len(b)-2) {
		return 0, bad
	}
	pos := int(offset)
	n := int(order.Uint16(b[pos : pos+2]))
	pos += 2
	if n > (len(b)-pos)/12 {
		return 0, bad
	}
	orientation := 1
	found := false
	for i := 0; i < n; i++ {
		e := b[pos+i*12 : pos+(i+1)*12]
		if order.Uint16(e[:2]) != 0x112 {
			continue
		}
		if found || order.Uint16(e[2:4]) != 3 || order.Uint32(e[4:8]) != 1 {
			return 0, bad
		}
		orientation = int(order.Uint16(e[8:10]))
		if orientation < 1 || orientation > 8 {
			return 0, bad
		}
		found = true
	}
	return orientation, nil
}

// Preserve ICC interpretation when changing containers. Expansion is bounded.
func modelImageColorProfile(data []byte, format string) ([]byte, error) {
	const maxProfile = 1 << 20
	var profile []byte
	switch format {
	case "jpeg":
		segments := map[int][]byte{}
		total := 0
		for pos := 2; pos+4 <= len(data); {
			if data[pos] != 0xff {
				break
			}
			marker := data[pos+1]
			if marker == 0xda || marker == 0xd9 {
				break
			}
			n := int(binary.BigEndian.Uint16(data[pos+2 : pos+4]))
			if n < 2 || n > len(data)-pos-2 {
				break
			}
			payload := data[pos+4 : pos+2+n]
			if marker == 0xe2 && bytes.HasPrefix(payload, []byte("ICC_PROFILE\x00")) {
				if len(payload) < 14 {
					return nil, errors.New("invalid ICC segment")
				}
				index, count := int(payload[12]), int(payload[13])
				if index < 1 || index > count || (total != 0 && total != count) || segments[index] != nil {
					return nil, errors.New("invalid ICC segment order")
				}
				total = count
				segments[index] = payload[14:]
			}
			pos += 2 + n
		}
		for i := 1; i <= total; i++ {
			segment, ok := segments[i]
			if !ok {
				return nil, errors.New("incomplete ICC profile")
			}
			if len(profile)+len(segment) > maxProfile {
				return nil, errors.New("ICC profile exceeds limit")
			}
			profile = append(profile, segment...)
		}
	case "png":
		for pos := 8; pos+12 <= len(data); {
			n := uint64(binary.BigEndian.Uint32(data[pos : pos+4]))
			if n > uint64(len(data)-pos-12) {
				return nil, errors.New("invalid PNG chunk")
			}
			end := pos + 8 + int(n)
			if crc32.ChecksumIEEE(data[pos+4:end]) != binary.BigEndian.Uint32(data[end:end+4]) {
				return nil, errors.New("invalid PNG checksum")
			}
			if string(data[pos+4:pos+8]) == "iCCP" {
				payload := data[pos+8 : end]
				index := bytes.IndexByte(payload, 0)
				if profile != nil || index < 1 || index > 79 || index+2 > len(payload) || payload[index+1] != 0 {
					return nil, errors.New("invalid PNG color profile")
				}
				r, err := zlib.NewReader(bytes.NewReader(payload[index+2:]))
				if err != nil {
					return nil, err
				}
				profile, err = io.ReadAll(io.LimitReader(r, maxProfile+1))
				r.Close()
				if err != nil {
					return nil, err
				}
				if len(profile) > maxProfile {
					return nil, errors.New("ICC profile exceeds limit")
				}
			}
			pos = end + 4
		}
	case "webp":
		if len(data) < 12 || uint64(binary.LittleEndian.Uint32(data[4:8]))+8 != uint64(len(data)) {
			return nil, errors.New("invalid WebP size")
		}
		for pos := 12; pos+8 <= len(data); {
			n := uint64(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
			if n > uint64(len(data)-pos-8) {
				return nil, errors.New("invalid WebP chunk")
			}
			end := pos + 8 + int(n)
			if string(data[pos:pos+4]) == "ICCP" {
				if profile != nil || n > maxProfile {
					return nil, errors.New("invalid WebP color profile")
				}
				profile = bytes.Clone(data[pos+8 : end])
			}
			pos = end + int(n&1)
		}
	}
	return profile, nil
}
func modelImageWithProfile(data []byte, mime string, profile []byte) ([]byte, error) {
	if len(profile) == 0 {
		return data, nil
	}
	var out bytes.Buffer
	if mime == "image/jpeg" {
		out.Write(data[:2])
		const chunk = 65519
		total := (len(profile) + chunk - 1) / chunk
		for i := 0; i < total; i++ {
			payload := append([]byte("ICC_PROFILE\x00"), byte(i+1), byte(total))
			payload = append(payload, profile[i*chunk:min(len(profile), (i+1)*chunk)]...)
			header := []byte{0xff, 0xe2, 0, 0}
			binary.BigEndian.PutUint16(header[2:], uint16(len(payload)+2))
			out.Write(header)
			out.Write(payload)
		}
		out.Write(data[2:])
		return out.Bytes(), nil
	}
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	if _, err := w.Write(profile); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	payload := append([]byte{'I', 'C', 'C', 0, 0}, compressed.Bytes()...)
	chunk := append([]byte("iCCP"), payload...)
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(payload)))
	checksum := make([]byte, 4)
	binary.BigEndian.PutUint32(checksum, crc32.ChecksumIEEE(chunk))
	out.Write(data[:33])
	out.Write(length)
	out.Write(chunk)
	out.Write(checksum)
	out.Write(data[33:])
	return out.Bytes(), nil
}
