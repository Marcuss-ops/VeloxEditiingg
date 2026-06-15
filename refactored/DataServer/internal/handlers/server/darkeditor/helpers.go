package darkeditor

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// ============================================================================
// HANDLER HELPERS
// ============================================================================

// getUniqueFilename generates a unique filename with the given extension
func (h *Handler) getUniqueFilename(ext string) string {
	id := uuid.New().String()
	timestamp := time.Now().Unix()
	return fmt.Sprintf("%d_%s.%s", timestamp, id[:8], ext)
}

// ensureDir ensures a directory exists
func (h *Handler) ensureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// getTempPath returns the full path for a temp file
func (h *Handler) getTempPath(filename string) string {
	return filepath.Join(h.cfg.TempDir, filename)
}

// ============================================================================
// MULTIPART BUILDER
// ============================================================================

// MultipartBuilder helps build multipart form requests
type MultipartBuilder struct {
	body   *bytes.Buffer
	writer *multipart.Writer
}

// NewMultipartBuilder creates a new multipart builder
func NewMultipartBuilder() *MultipartBuilder {
	body := &bytes.Buffer{}
	return &MultipartBuilder{
		body:   body,
		writer: multipart.NewWriter(body),
	}
}

// AddFile adds a file to the multipart form
func (b *MultipartBuilder) AddFile(fieldName, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	part, err := b.writer.CreateFormFile(fieldName, filepath.Base(filePath))
	if err != nil {
		return err
	}

	_, err = io.Copy(part, file)
	return err
}

// Close finalizes the multipart form
func (b *MultipartBuilder) Close() error {
	return b.writer.Close()
}

// Body returns the body buffer
func (b *MultipartBuilder) Body() *bytes.Buffer {
	return b.body
}

// ContentType returns the content type with boundary
func (b *MultipartBuilder) ContentType() string {
	return b.writer.FormDataContentType()
}

// BuildRequest creates an HTTP request with the multipart body
func (b *MultipartBuilder) BuildRequest(method, url string) (*http.Request, error) {
	if err := b.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, url, b.body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", b.ContentType())
	return req, nil
}

// CreateMultipartRequest creates a multipart request from a file path
func CreateMultipartRequest(method, url, fieldName, filePath string) (*http.Request, error) {
	builder := NewMultipartBuilder()
	if err := builder.AddFile(fieldName, filePath); err != nil {
		return nil, err
	}
	return builder.BuildRequest(method, url)
}
