package requestestimate

import (
	"encoding/base64"
	"errors"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"math"
	"strings"
)

func (c counter) image(url, detail string) (int64, error) {
	if c.provider == "deepseek" && c.encoding == "deepseek_v4" {
		return 1024, nil
	}
	// URLs and opaque image IDs are never fetched just to estimate a request.
	width, height := float64(0), float64(0)
	if strings.HasPrefix(url, "data:") {
		header, encoded, ok := strings.Cut(url, ",")
		if !ok || !strings.HasSuffix(header, ";base64") {
			return 0, errors.New("invalid inline image encoding")
		}
		reader := base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded))
		dimensions, _, err := image.DecodeConfig(reader)
		if err == nil && dimensions.Width > 0 && dimensions.Height > 0 {
			width = float64(dimensions.Width)
			height = float64(dimensions.Height)
		}
	}
	model := c.model
	provider := c.provider
	if provider == "openrouter" && strings.HasPrefix(model, "openai/") {
		provider = "openai"
		model = strings.TrimPrefix(model, "openai/")
	}
	if provider == "openai" {
		base, tile := float64(0), float64(0)
		switch {
		case family(model, "gpt-4o-mini"):
			base, tile = 2833, 5667
		case family(model, "gpt-4o"), family(model, "gpt-4.1"):
			base, tile = 85, 170
		case family(model, "gpt-5.1"), model == "gpt-5":
			base, tile = 70, 140
		case family(model, "o1"), family(model, "o3"):
			base, tile = 75, 150
		}
		// Mini/nano use patches, unlike their full-size 4.1 counterpart.
		if strings.HasPrefix(model, "gpt-4.1-mini") || strings.HasPrefix(model, "gpt-4.1-nano") {
			base, tile = 0, 0
		}
		if tile > 0 {
			if detail == "low" {
				return int64(base), nil
			}
			if width == 0 {
				return int64(base + 8*tile), nil
			}
			scale := math.Min(1, 2048/math.Max(width, height))
			width *= scale
			height *= scale
			scale = math.Min(1, 768/math.Min(width, height))
			width = math.Floor(width * scale)
			height = math.Floor(height * scale)
			return int64(base + math.Ceil(width/512)*math.Ceil(height/512)*tile), nil
		}
		// Explicit model families follow published patch budgets. Unknown families
		// use the proxy pixel estimate below, not an invented official mapping.
		budget, multiplier := float64(0), float64(1.2)
		switch {
		case family(model, "gpt-6-astra"):
			budget = 30000
			if detail == "high" {
				budget = 2500
			}
			if detail == "low" {
				budget = 256
			}
		case strings.HasPrefix(model, "gpt-5.6-"):
			budget = 30000
			if detail == "high" {
				budget = 2500
			}
			if detail == "low" {
				budget = 256
			}
		case family(model, "gpt-5.5"):
			budget = 10000
			if detail == "high" {
				budget = 2500
			}
			if detail == "low" {
				budget = 256
			}
		case family(model, "gpt-5.4"):
			budget = 2500
			if detail == "original" {
				budget = 10000
			}
			if detail == "low" {
				budget = 6144
			}
		case family(model, "gpt-5.2"):
			budget = 6144
		case family(model, "gpt-4.1-mini"):
			budget = 6144
			multiplier = 1.62
		case family(model, "gpt-4.1-nano"):
			budget = 1536
			multiplier = 2.46
		case family(model, "gpt-5-mini"):
			budget = 1536
		case family(model, "gpt-5-nano"):
			budget = 1536
			multiplier = 1.5
		case family(model, "o4-mini"):
			budget = 1536
			multiplier = 1.72
		}
		if budget > 0 {
			if width > 0 {
				budget = math.Min(budget, math.Ceil(width/32)*math.Ceil(height/32))
			}
			return int64(math.Ceil(budget * multiplier)), nil
		}
	}
	if width == 0 {
		return 32768, nil
	}
	// No documented matching rule: a conservative 16-pixel patch proxy, with
	// explicit overhead. Native usage subsequently calibrates the complete request.
	return int64(math.Ceil(width/16)*math.Ceil(height/16)) + 1024, nil
}

func family(model, prefix string) bool {
	return model == prefix || strings.HasPrefix(model, prefix+"-")
}
