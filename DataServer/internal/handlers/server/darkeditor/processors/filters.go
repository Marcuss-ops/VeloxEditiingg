// Package processors provides image processing functionality for Dark Editor
package processors

import (
	"image"
	"image/color"
	"math"

	"github.com/disintegration/imaging"
)

// FilterType defines the type of filter to apply
type FilterType string

const (
	FilterBrightness FilterType = "brightness"
	FilterContrast   FilterType = "contrast"
	FilterSaturation FilterType = "saturation"
	FilterBlur       FilterType = "blur"
	FilterSharpen    FilterType = "sharpen"
	FilterPixelation FilterType = "pixelation"
	FilterGrayscale  FilterType = "grayscale"
	FilterSepia      FilterType = "sepia"
	FilterInvert     FilterType = "invert"
	FilterHue        FilterType = "hue"
	FilterGamma      FilterType = "gamma"
)

// FilterOptions contains options for filter processing
type FilterOptions struct {
	Type   FilterType
	Value  float64 // Generic value for adjustments
	Radius float64 // For blur/sharpen
}

// ApplyFilter applies a filter to an image
func ApplyFilter(img image.Image, opts FilterOptions) image.Image {
	switch opts.Type {
	case FilterBrightness:
		return applyBrightness(img, opts.Value)
	case FilterContrast:
		return applyContrast(img, opts.Value)
	case FilterSaturation:
		return applySaturation(img, opts.Value)
	case FilterBlur:
		return applyBlur(img, opts.Radius)
	case FilterSharpen:
		return applySharpen(img, opts.Value)
	case FilterPixelation:
		return applyPixelation(img, opts.Value)
	case FilterGrayscale:
		return imaging.Grayscale(img)
	case FilterSepia:
		return applySepia(img)
	case FilterInvert:
		return applyInvert(img)
	case FilterHue:
		return applyHue(img, opts.Value)
	case FilterGamma:
		return imaging.AdjustGamma(img, opts.Value)
	default:
		return img
	}
}

// applyBrightness adjusts image brightness
// value range: -100 to 100 (0 = no change)
func applyBrightness(img image.Image, value float64) image.Image {
	// Normalize from -100..100 to -1..1
	adjustment := value / 100.0
	return imaging.AdjustBrightness(img, adjustment)
}

// applyContrast adjusts image contrast
// value range: -100 to 100 (0 = no change)
func applyContrast(img image.Image, value float64) image.Image {
	// Normalize from -100..100 to -1..1
	adjustment := value / 100.0
	return imaging.AdjustContrast(img, adjustment)
}

// applySaturation adjusts image saturation
// value range: -100 to 100 (0 = no change)
func applySaturation(img image.Image, value float64) image.Image {
	// Normalize from -100..100 to -1..1
	adjustment := value / 100.0
	return imaging.AdjustSaturation(img, adjustment)
}

// applyBlur applies Gaussian blur
// radius: blur radius in pixels
func applyBlur(img image.Image, radius float64) image.Image {
	if radius <= 0 {
		return img
	}
	return imaging.Blur(img, radius)
}

// applySharpen sharpens the image
// value: sharpening strength (typically 0-5)
func applySharpen(img image.Image, value float64) image.Image {
	if value <= 0 {
		return img
	}
	return imaging.Sharpen(img, value)
}

func applyPixelation(img image.Image, pixelSize float64) image.Image {
	if pixelSize <= 0 {
		return img
	}

	b := img.Bounds()
	w := b.Dx()
	h := b.Dy()
	if w <= 0 || h <= 0 {
		return img
	}

	size := int(math.Round(pixelSize))
	if size < 1 {
		size = 1
	}

	downW := w / size
	downH := h / size
	if downW < 1 {
		downW = 1
	}
	if downH < 1 {
		downH = 1
	}

	small := imaging.Resize(img, downW, downH, imaging.NearestNeighbor)
	return imaging.Resize(small, w, h, imaging.NearestNeighbor)
}

// applySepia applies a sepia tone effect
func applySepia(img image.Image) image.Image {
	bounds := img.Bounds()
	result := image.NewRGBA(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			original := color.RGBAModel.Convert(img.At(x, y)).(color.RGBA)

			// Sepia transformation matrix
			r := float64(original.R)*0.393 + float64(original.G)*0.769 + float64(original.B)*0.189
			g := float64(original.R)*0.349 + float64(original.G)*0.686 + float64(original.B)*0.168
			b := float64(original.R)*0.272 + float64(original.G)*0.534 + float64(original.B)*0.131

			// Clamp values
			result.SetRGBA(x, y, color.RGBA{
				R: uint8(math.Min(255, r)),
				G: uint8(math.Min(255, g)),
				B: uint8(math.Min(255, b)),
				A: original.A,
			})
		}
	}

	return result
}

// applyInvert inverts image colors
func applyInvert(img image.Image) image.Image {
	bounds := img.Bounds()
	result := image.NewRGBA(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			original := color.RGBAModel.Convert(img.At(x, y)).(color.RGBA)
			result.SetRGBA(x, y, color.RGBA{
				R: 255 - original.R,
				G: 255 - original.G,
				B: 255 - original.B,
				A: original.A,
			})
		}
	}

	return result
}

// applyHue rotates the hue of the image
// value: hue rotation in degrees (0-360)
func applyHue(img image.Image, value float64) image.Image {
	return imaging.AdjustFunc(img, func(c color.NRGBA) color.NRGBA {
		// Convert RGB to HSL
		h, s, l := rgbToHsl(float64(c.R)/255, float64(c.G)/255, float64(c.B)/255)

		// Rotate hue
		h = math.Mod(h+value/360.0, 1.0)
		if h < 0 {
			h += 1
		}

		// Convert back to RGB
		r, g, b := hslToRgb(h, s, l)

		return color.NRGBA{
			R: uint8(r * 255),
			G: uint8(g * 255),
			B: uint8(b * 255),
			A: c.A,
		}
	})
}

// rgbToHsl converts RGB to HSL
func rgbToHsl(r, g, b float64) (h, s, l float64) {
	max := math.Max(math.Max(r, g), b)
	min := math.Min(math.Min(r, g), b)
	l = (max + min) / 2

	if max == min {
		h = 0
		s = 0
	} else {
		d := max - min
		s = d / (1 - math.Abs(2*l-1))

		switch max {
		case r:
			h = math.Mod((g-b)/d, 6)
		case g:
			h = (b-r)/d + 2
		case b:
			h = (r-g)/d + 4
		}
		h /= 6
		if h < 0 {
			h += 1
		}
	}

	return h, s, l
}

// hslToRgb converts HSL to RGB
func hslToRgb(h, s, l float64) (r, g, b float64) {
	if s == 0 {
		return l, l, l
	}

	var hueToRgb func(p, q, t float64) float64
	hueToRgb = func(p, q, t float64) float64 {
		if t < 0 {
			t += 1
		}
		if t > 1 {
			t -= 1
		}
		if t < 1/6.0 {
			return p + (q-p)*6*t
		}
		if t < 1/2.0 {
			return q
		}
		if t < 2/3.0 {
			return p + (q-p)*(2/3.0-t)*6
		}
		return p
	}

	q := l + s - s*l
	if l < 0.5 {
		q = l * (1 + s)
	} else {
		q = l + s - s*l
	}
	p := 2*l - q

	r = hueToRgb(p, q, h+1/3.0)
	g = hueToRgb(p, q, h)
	b = hueToRgb(p, q, h-1/3.0)

	return r, g, b
}
