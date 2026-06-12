// Package processors provides image processing functionality for Dark Editor
package processors

import (
	"image"

	"github.com/disintegration/imaging"
)

// TransformOptions contains options for image transformation
type TransformOptions struct {
	// Crop box: [left, top, right, bottom]
	CropBox []int
	// Resize dimensions: [width, height]
	ResizeDims []int
	// Rotation angle in degrees
	Rotation float64
	// Flip horizontal
	FlipH bool
	// Flip vertical
	FlipV bool
	// Maintain aspect ratio when resizing
	MaintainAspectRatio bool
}

// Transform applies transformations to an image
func Transform(img image.Image, opts TransformOptions) image.Image {
	result := img

	// Apply crop if specified
	if len(opts.CropBox) == 4 {
		result = applyCrop(result, opts.CropBox)
	}

	// Apply resize if specified
	if len(opts.ResizeDims) == 2 {
		result = applyResize(result, opts.ResizeDims, opts.MaintainAspectRatio)
	}

	// Apply rotation if specified
	if opts.Rotation != 0 {
		result = applyRotation(result, opts.Rotation)
	}

	// Apply flips
	if opts.FlipH {
		result = imaging.FlipH(result)
	}
	if opts.FlipV {
		result = imaging.FlipV(result)
	}

	return result
}

// applyCrop crops the image to the specified box
// cropBox: [left, top, right, bottom]
func applyCrop(img image.Image, cropBox []int) image.Image {
	bounds := img.Bounds()
	imgWidth := bounds.Dx()
	imgHeight := bounds.Dy()

	left := cropBox[0]
	top := cropBox[1]
	right := cropBox[2]
	bottom := cropBox[3]

	// Validate and clamp crop box
	if left < 0 {
		left = 0
	}
	if top < 0 {
		top = 0
	}
	if right > imgWidth {
		right = imgWidth
	}
	if bottom > imgHeight {
		bottom = imgHeight
	}

	// Ensure valid crop dimensions
	if left >= right || top >= bottom {
		return img
	}

	// imaging.Crop uses image.Rectangle with coordinates relative to origin
	cropRect := image.Rect(left, top, right, bottom)
	return imaging.Crop(img, cropRect)
}

// applyResize resizes the image to the specified dimensions
func applyResize(img image.Image, dims []int, maintainAspectRatio bool) image.Image {
	width := dims[0]
	height := dims[1]

	if width <= 0 && height <= 0 {
		return img
	}

	// Get original dimensions
	bounds := img.Bounds()
	origWidth := bounds.Dx()
	origHeight := bounds.Dy()

	if maintainAspectRatio {
		// Calculate dimensions maintaining aspect ratio
		if width <= 0 {
			// Calculate width from height
			aspectRatio := float64(origWidth) / float64(origHeight)
			width = int(float64(height) * aspectRatio)
		} else if height <= 0 {
			// Calculate height from width
			aspectRatio := float64(origHeight) / float64(origWidth)
			height = int(float64(width) * aspectRatio)
		} else {
			// Both specified - fit within bounds maintaining aspect ratio
			widthRatio := float64(width) / float64(origWidth)
			heightRatio := float64(height) / float64(origHeight)

			if widthRatio < heightRatio {
				height = int(float64(origHeight) * widthRatio)
			} else {
				width = int(float64(origWidth) * heightRatio)
			}
		}
	}

	// Ensure minimum dimensions
	if width <= 0 {
		width = 1
	}
	if height <= 0 {
		height = 1
	}

	return imaging.Resize(img, width, height, imaging.Lanczos)
}

// applyRotation rotates the image by the specified angle
func applyRotation(img image.Image, angle float64) image.Image {
	// Normalize angle to 0-360
	for angle < 0 {
		angle += 360
	}
	for angle >= 360 {
		angle -= 360
	}

	// Handle common rotations efficiently
	switch angle {
	case 90:
		return imaging.Rotate90(img)
	case 180:
		return imaging.Rotate180(img)
	case 270:
		return imaging.Rotate270(img)
	case 0, 360:
		return img
	default:
		// For arbitrary angles, use imaging.Rotate
		// Note: This creates a larger canvas to fit the rotated image
		return imaging.Rotate(img, angle, image.Transparent)
	}
}

// CropFromCenter crops from the center of the image
// width and height are the desired crop dimensions
func CropFromCenter(img image.Image, width, height int) image.Image {
	bounds := img.Bounds()
	imgWidth := bounds.Dx()
	imgHeight := bounds.Dy()

	// Clamp to image size
	if width > imgWidth {
		width = imgWidth
	}
	if height > imgHeight {
		height = imgHeight
	}

	// Calculate center crop coordinates
	left := (imgWidth - width) / 2
	top := (imgHeight - height) / 2
	right := left + width
	bottom := top + height

	return imaging.Crop(img, image.Rect(left, top, right, bottom))
}

// FitToSize resizes and crops image to exactly fit the specified dimensions
func FitToSize(img image.Image, width, height int) image.Image {
	bounds := img.Bounds()
	origWidth := bounds.Dx()
	origHeight := bounds.Dy()

	// Calculate aspect ratios
	targetRatio := float64(width) / float64(height)
	origRatio := float64(origWidth) / float64(origHeight)

	var resized image.Image

	if origRatio > targetRatio {
		// Original is wider - resize by height, then crop width
		resized = imaging.Resize(img, 0, height, imaging.Lanczos)
	} else {
		// Original is taller - resize by width, then crop height
		resized = imaging.Resize(img, width, 0, imaging.Lanczos)
	}

	// Center crop to exact dimensions
	return CropFromCenter(resized, width, height)
}
