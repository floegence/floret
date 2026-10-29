package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	_ "golang.org/x/image/webp"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
)

// MediaLimitError describes an independently enforced media restriction.
// Aggregate limits may be relieved by compacting history; individual invalid
// attachments retain their independent terminal error identity.
type MediaLimitError struct {
	Kind          string
	Actual, Limit int64
}

func (e *MediaLimitError) Error() string {
	return fmt.Sprintf("provider image %s limit exceeded: actual=%d limit=%d", e.Kind, e.Actual, e.Limit)
}

func validateDeepSeekFileImages(input []json.RawMessage, images map[string]deepSeekResolvedImage) error {
	count := 0
	total := int64(0)
	maxSide := 0
	for _, raw := range input {
		var item struct {
			Type, Role      string
			Content, Output json.RawMessage
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return err
		}
		content := item.Content
		if item.Type == "function_call_output" {
			content = item.Output
		} else if item.Type != "message" || item.Role != "user" {
			continue
		}
		if len(content) == 0 || content[0] != '[' {
			continue
		}
		var parts []deepSeekImageContent
		if err := json.Unmarshal(content, &parts); err != nil {
			return err
		}
		for _, part := range parts {
			if part.Type != "image_reference" {
				continue
			}
			if part.Image == nil {
				return errors.New("DeepSeek image reference is incomplete")
			}
			resolved, ok := images[deepSeekHash(part.Image)]
			if !ok {
				return errors.New("DeepSeek image has no authorized content")
			}
			count++
			total += int64(len(resolved.data))
			if len(resolved.data) > 64<<20 {
				return &MediaLimitError{Kind: "bytes", Actual: int64(len(resolved.data)), Limit: 64 << 20}
			}
			dimensions, format, err := image.DecodeConfig(bytes.NewReader(resolved.data))
			if err != nil || "image/"+format != resolved.mime {
				return errors.New("DeepSeek image content does not match its declared format")
			}
			maxSide = max(maxSide, dimensions.Width, dimensions.Height)
		}
	}
	if count > 600 {
		return &MediaLimitError{Kind: "count", Actual: int64(count), Limit: 600}
	}
	if total > 200<<20 {
		return &MediaLimitError{Kind: "total_bytes", Actual: total, Limit: 200 << 20}
	}
	limit := 8192
	if count >= 15 {
		limit = 4096
	}
	if maxSide > limit {
		return &MediaLimitError{Kind: "dimension", Actual: int64(maxSide), Limit: int64(limit)}
	}
	return nil
}

// RequestCapacityLimit exposes aggregate image capacity without aliasing
// ErrContextOverflow. Empty kind means an individual-image failure.
func (e *MediaLimitError) RequestCapacityLimit() (kind string, actual, limit int64) {
	if e == nil || e.Limit <= 0 || e.Actual <= e.Limit {
		return "", 0, 0
	}
	switch e.Kind {
	case "count":
		return "image_count", e.Actual, e.Limit
	case "total_bytes":
		return "image_bytes", e.Actual, e.Limit
	default:
		return "", 0, 0
	}
}
