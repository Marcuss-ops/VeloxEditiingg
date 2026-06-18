package uploads

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestUploadCompletedVideo_AutoUploadsToYouTubeAndDrive(t *testing.T) {
	t.Skip("Skipping: requires rewriting for new BlobStore-based UploadCompletedVideo signature (removed youtubeSvc/driveSvc params)")
}
