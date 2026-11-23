package provider

import (
	"bytes"
	"context"
	"errors"
	"image"
	"math/rand/v2"
	"testing"

	"golang.org/x/image/draw"
)

func TestModelRowScaleMatchesCatmullRom(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 9))
	for _, size := range [][4]int{{3614, 1920, 2560, 1360}, {3, 1, 1, 1}, {7, 17, 2, 5}, {97, 61, 63, 40}, {512, 65, 128, 16}, {1, 193, 1, 5}, {233, 1, 16, 1}} {
		parent := image.NewNRGBA(image.Rect(-8, -7, size[0]+3, size[1]+4))
		source := parent.SubImage(image.Rect(-2, 1, size[0]-2, size[1]+1)).(*image.NRGBA)
		for index := range parent.Pix {
			parent.Pix[index] = byte(random.Uint32())
		}
		for row := 2; row < source.Rect.Dy()/2; row++ {
			copy(source.Pix[row*source.Stride:][:4*size[0]], source.Pix[:4*size[0]])
		}
		actual := image.NewNRGBA(image.Rect(3, -2, size[2]+3, size[3]-2))
		reference := image.NewNRGBA(actual.Bounds())
		draw.CatmullRom.Scale(reference, reference.Bounds(), source, source.Bounds(), draw.Src, nil)
		if err := scaleModelNRGBA(t.Context(), source, actual); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual.Pix, reference.Pix) {
			t.Fatalf("different output for %v", size)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := scaleModelNRGBA(ctx, source, actual); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}

func TestModelRowScaleTransparentMargins(t *testing.T) {
	parent := image.NewNRGBA(image.Rect(-4, -2, 79, 71))
	source := parent.SubImage(image.Rect(0, 1, 73, 67)).(*image.NRGBA)
	for row := 0; row < source.Rect.Dy(); row++ {
		if row%11 < 5 {
			continue
		}
		first := []int{0, 19, 72}[row%3]
		for col := first; col < min(first+7, source.Rect.Dx()); col++ {
			index := row*source.Stride + col*4
			source.Pix[index+row%4] = 255
			source.Pix[index+3] = byte(130 + row)
		}
	}
	for _, size := range []image.Point{{39, 37}, {1, 3}, {73, 20}} {
		bounds := image.Rect(4, -1, 4+size.X, -1+size.Y)
		actual := image.NewNRGBA(bounds)
		reference := image.NewNRGBA(bounds)
		for index := range actual.Pix {
			actual.Pix[index] = 255
		}
		draw.CatmullRom.Scale(reference, bounds, source, source.Bounds(), draw.Src, nil)
		if err := scaleModelNRGBA(t.Context(), source, actual); err != nil || !bytes.Equal(actual.Pix, reference.Pix) {
			t.Fatalf("transparent margins differ at %v: %v", size, err)
		}
	}
}

func BenchmarkModelDenseRowScale(b *testing.B) {
	source := image.NewNRGBA(image.Rect(0, 0, 3614, 1920))
	random := rand.New(rand.NewPCG(3, 11))
	for offset := range source.Pix {
		source.Pix[offset] = byte(random.Uint32())
	}
	for _, name := range []string{"reference", "row"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				dest := image.NewNRGBA(image.Rect(0, 0, 2560, 1360))
				if name == "reference" {
					draw.CatmullRom.Scale(dest, dest.Bounds(), source, source.Bounds(), draw.Src, nil)
				} else if err := scaleModelNRGBA(b.Context(), source, dest); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
