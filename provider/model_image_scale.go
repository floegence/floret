package provider

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"math"

	"golang.org/x/image/draw"
)

type modelFilter struct {
	first   int
	weights []float64
	norm    float64
}

func modelFilters(source, destination int) ([]modelFilter, int) {
	ratio := float64(source) / float64(destination)
	radius := draw.CatmullRom.Support * ratio
	inverse := 1 / ratio
	filters := make([]modelFilter, destination)
	weights := make([]float64, 0, 5*source+destination)
	widest := 0
	for pos := range filters {
		center := float64((float64(pos)+0.5)*ratio) - 0.5
		first := max(0, int(math.Floor(center-radius)))
		last := min(source, int(math.Ceil(center+radius)))
		offset := len(weights)
		var total float64
		for sample := first; sample < last; sample++ {
			distance := math.Abs((center - float64(sample)) * inverse)
			weight := 0.0
			if distance < draw.CatmullRom.Support {
				weight = draw.CatmullRom.At(distance)
			}
			total += weight
			weights = append(weights, weight)
		}
		filters[pos] = modelFilter{first, weights[offset:], 1 / total}
		widest = max(widest, last-first)
	}
	return filters, widest
}

type modelChannels [4]float64

func filterModelRow(target []modelChannels, source []byte, filters []modelFilter) {
	// Match x/image's premultiplication and separate multiply/add rounding.
	for col, filter := range filters {
		var sum modelChannels
		pixels := source[filter.first*4:][:4*len(filter.weights)]
		for index, weight := range filter.weights {
			pixel := pixels[index*4:][:4]
			alpha := uint32(pixel[3]) * 257
			if alpha == 0 {
				continue
			}
			sum[0] += float64(float64(uint32(pixel[0])*alpha/255) * weight)
			sum[1] += float64(float64(uint32(pixel[1])*alpha/255) * weight)
			sum[2] += float64(float64(uint32(pixel[2])*alpha/255) * weight)
			sum[3] += float64(float64(alpha) * weight)
		}
		norm := filter.norm / 65535
		target[col] = modelChannels{sum[0] * norm, sum[1] * norm, sum[2] * norm, sum[3] * norm}
	}
}

func modelChannel(value, norm float64) uint16 {
	return uint16(max(0, min(65535, int(float64(65535*float64(value*norm))+0.5))))
}

func scaleModelNRGBA(ctx context.Context, source, dest *image.NRGBA) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	width, height := dest.Rect.Dx(), dest.Rect.Dy()
	horizontal, _ := modelFilters(source.Rect.Dx(), width)
	vertical, capacity := modelFilters(source.Rect.Dy(), height)
	rows := make([]modelChannels, capacity*width)
	// Consecutive output rows share input rows within the vertical kernel's support.
	sums := make([]modelChannels, width)
	nextRow := 0
	var previous []byte
	for row, filter := range vertical {
		if err := ctx.Err(); err != nil {
			return err
		}
		for nextRow < filter.first+len(filter.weights) {
			start := (nextRow % capacity) * width
			target := rows[start : start+width]
			pixels := source.Pix[nextRow*source.Stride:][:4*source.Rect.Dx()]
			if previous != nil && bytes.Equal(previous, pixels) {
				prior := ((nextRow - 1) % capacity) * width
				copy(target, rows[prior:prior+width])
			} else {
				filterModelRow(target, pixels, horizontal)
			}
			previous = pixels
			nextRow++
		}
		clear(sums)
		for offset, weight := range filter.weights {
			if weight == 0 {
				continue
			}
			start := ((filter.first + offset) % capacity) * width
			for col, pixel := range rows[start : start+width] {
				sums[col][0] += float64(pixel[0] * weight)
				sums[col][1] += float64(pixel[1] * weight)
				sums[col][2] += float64(pixel[2] * weight)
				sums[col][3] += float64(pixel[3] * weight)
			}
		}
		for col, pixel := range sums {
			alpha := pixel[3]
			dest.SetRGBA64(dest.Rect.Min.X+col, dest.Rect.Min.Y+row, color.RGBA64{
				R: modelChannel(min(pixel[0], alpha), filter.norm),
				G: modelChannel(min(pixel[1], alpha), filter.norm),
				B: modelChannel(min(pixel[2], alpha), filter.norm),
				A: modelChannel(alpha, filter.norm),
			})
		}
	}
	return nil
}
