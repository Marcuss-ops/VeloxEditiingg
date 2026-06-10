package darkeditor

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

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

// AddFileFromBytes adds a file from byte data to the multipart form
func (b *MultipartBuilder) AddFileFromBytes(fieldName, filename string, data []byte) error {
	part, err := b.writer.CreateFormFile(fieldName, filename)
	if err != nil {
		return err
	}

	_, err = part.Write(data)
	return err
}

// AddField adds a text field to the multipart form
func (b *MultipartBuilder) AddField(fieldName, value string) error {
	return b.writer.WriteField(fieldName, value)
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

// ============== BACKGROUND REMOVAL API CLIENT ==============

// BackgroundRemovalAPIClient handles communication with external background removal APIs
type BackgroundRemovalAPIClient struct {
	endpoint string
	apiKey   string
}

// RemoveBackground sends an image to the API for background removal
func (c *BackgroundRemovalAPIClient) RemoveBackground(inputPath, outputPath, model string) error {
	// Read input file
	inputData, err := os.ReadFile(inputPath)
	if err != nil {
		return err
	}

	// Build multipart request
	builder := NewMultipartBuilder()

	// Add file
	if err := builder.AddFileFromBytes("file", filepath.Base(inputPath), inputData); err != nil {
		return err
	}

	// Add model parameter
	if model != "" {
		if err := builder.AddField("model", model); err != nil {
			return err
		}
	}

	// Build request
	req, err := builder.BuildRequest("POST", c.endpoint)
	if err != nil {
		return err
	}

	// Add API key if provided
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	// Execute request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(body),
		}
	}

	// Read response and save to output
	outputData, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return os.WriteFile(outputPath, outputData, 0644)
}

// APIError represents an API error
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return e.Message
}

// ============== UTILITY FUNCTIONS ==============

// CreateMultipartRequest creates a multipart request from a file path
func CreateMultipartRequest(method, url, fieldName, filePath string) (*http.Request, error) {
	builder := NewMultipartBuilder()
	if err := builder.AddFile(fieldName, filePath); err != nil {
		return nil, err
	}
	return builder.BuildRequest(method, url)
}

// CreateMultipartRequestFromBytes creates a multipart request from byte data
func CreateMultipartRequestFromBytes(method, url, fieldName, filename string, data []byte) (*http.Request, error) {
	builder := NewMultipartBuilder()
	if err := builder.AddFileFromBytes(fieldName, filename, data); err != nil {
		return nil, err
	}
	return builder.BuildRequest(method, url)
}

// UploadFile uploads a file to a URL using multipart form
func UploadFile(url, fieldName, filePath string, headers map[string]string) ([]byte, error) {
	req, err := CreateMultipartRequest("POST", url, fieldName, filePath)
	if err != nil {
		return nil, err
	}

	// Add custom headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}
