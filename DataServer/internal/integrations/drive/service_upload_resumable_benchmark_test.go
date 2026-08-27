package drive

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
)

func BenchmarkResumableChunking(b *testing.B) {
	const fileSize int64 = 64 << 20
	filePath := benchmarkUploadFile(b, fileSize)

	for _, chunkSize := range []int64{256 << 10, 1 << 20, 2 << 20, 4 << 20} {
		b.Run(strconv.FormatInt(chunkSize>>10, 10)+"KiB", func(b *testing.B) {
			file, err := os.Open(filePath)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = file.Close() })

			payload := make([]byte, chunkSize)
			b.SetBytes(chunkSize)
			b.ReportMetric(float64((fileSize+chunkSize-1)/chunkSize), "chunks/op")
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := file.Seek(0, io.SeekStart); err != nil {
					b.Fatal(err)
				}
				for offset := int64(0); offset < fileSize; offset += chunkSize {
					length := chunkSize
					if remaining := fileSize - offset; remaining < length {
						length = remaining
					}
					if int64(len(payload)) != length {
						payload = make([]byte, length)
					}
					if _, err := file.ReadAt(payload[:length], offset); err != nil && err != io.EOF {
						b.Fatal(err)
					}
					request, err := http.NewRequest(http.MethodPut, "http://chunk.test/upload", bytes.NewReader(payload[:length]))
					if err != nil {
						b.Fatal(err)
					}
					request.Header.Set("Content-Length", strconv.FormatInt(length, 10))
					request.Header.Set("Content-Range", "bytes "+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+length-1, 10)+"/"+strconv.FormatInt(fileSize, 10))
				}
			}
		})
	}
}

func BenchmarkResumableChunkHTTPRoundTrip(b *testing.B) {
	const fileSize int64 = 16 << 20
	filePath := benchmarkUploadFile(b, fileSize)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusPermanentRedirect)
	}))
	b.Cleanup(server.Close)

	for _, chunkSize := range []int64{256 << 10, 1 << 20, 2 << 20, 4 << 20} {
		b.Run(strconv.FormatInt(chunkSize>>10, 10)+"KiB", func(b *testing.B) {
			file, err := os.Open(filePath)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = file.Close() })
			payload := make([]byte, chunkSize)
			b.SetBytes(chunkSize)
			b.ReportMetric(float64((fileSize+chunkSize-1)/chunkSize), "chunks/op")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for offset := int64(0); offset < fileSize; offset += chunkSize {
					length := chunkSize
					if remaining := fileSize - offset; remaining < length {
						length = remaining
					}
					if _, err := file.ReadAt(payload[:length], offset); err != nil && err != io.EOF {
						b.Fatal(err)
					}
					request, err := http.NewRequest(http.MethodPut, server.URL, bytes.NewReader(payload[:length]))
					if err != nil {
						b.Fatal(err)
					}
					request.Header.Set("Content-Length", strconv.FormatInt(length, 10))
					request.Header.Set("Content-Range", "bytes "+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+length-1, 10)+"/"+strconv.FormatInt(fileSize, 10))
					response, err := http.DefaultClient.Do(request)
					if err != nil {
						b.Fatal(err)
					}
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
				}
			}
		})
	}
}

func benchmarkUploadFile(b *testing.B, size int64) string {
	b.Helper()
	file, err := os.CreateTemp(b.TempDir(), "benchmark-*.mp4")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.Remove(file.Name()) })
	if err := file.Truncate(size); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	return file.Name()
}
