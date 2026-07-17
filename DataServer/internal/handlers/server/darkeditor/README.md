# Dark Editor API

Web-based image editor with AI-powered features. This package provides the Go backend implementation for the Dark Editor API.

## Structure

```
darkeditor/
├── config.go              # Config and Handler structs
├── types.go               # All request/response types
├── helpers.go             # Helper functions (filename generation, paths)
├── handlers.go            # Core image operation handlers
├── routes.go              # Route registration
├── background_removal.go  # Background removal integration
├── social.go              # Social platform integration (generic)
├── drive.go               # Google Drive integration
└── processors/            # Image processing utilities
    ├── filters.go         # Filter implementations
    ├── transform.go       # Crop and resize
    ├── export.go          # Export to various formats
    └── upscale.go         # Image upscaling
```

## API Endpoints

### Core Image Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/dark_editor_v2/upload` | Upload an image |
| POST | `/dark_editor_v2/process/filter` | Apply a filter |
| POST | `/dark_editor_v2/process/transform` | Crop or resize |
| POST | `/dark_editor_v2/export` | Export in specific format |
| POST | `/dark_editor_v2/generate` | AI image generation (NVIDIA FLUX) |
| POST | `/dark_editor_v2/api/upscale` | Upscale image |
| POST | `/dark_editor_v2/api/tools/thumbnail_grab` | Grab thumbnail from URL |

### Available Filters

| Filter | Value Range | Description |
|--------|-------------|-------------|
| `brightness` | -100 to 100 | Adjust brightness |
| `contrast` | -100 to 100 | Adjust contrast |
| `saturation` | -100 to 100 | Adjust saturation |
| `blur` | 0.1 to 10.0 | Gaussian blur radius |
| `sharpen` | 0.1 to 5.0 | Sharpening strength |
| `grayscale` | - | Convert to grayscale |
| `sepia` | - | Apply sepia tone |
| `invert` | - | Invert colors |
| `hue` | 0 to 360 | Rotate hue |
| `gamma` | 0.1 to 3.0 | Gamma correction |

### Background Removal

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/dark_editor_v2/api/remove-bg` | Remove background |
| POST | `/dark_editor_v2/api/remove-bg/upload` | Direct upload + remove |
| GET | `/dark_editor_v2/api/remove-bg/status/:task_id` | Check async status |
| GET | `/dark_editor_v2/api/remove-bg/models` | List available models |
| GET | `/dark_editor_v2/api/remove-bg/health` | Health check |

### Social Platform Integration

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/dark_editor_v2/api/social/thumbnail` | Set video thumbnail |
| POST | `/dark_editor_v2/api/social/thumbnail/upload` | Direct upload thumbnail |
| GET | `/dark_editor_v2/api/social/destinations` | List destinations |
| GET | `/dark_editor_v2/api/social/destinations/:id/validate` | Validate destination |
| GET | `/dark_editor_v2/api/social/videos/:id` | Get video info |
| GET | `/dark_editor_v2/api/social/oauth/start` | Start OAuth flow |
| GET | `/dark_editor_v2/api/social/health` | Health check |

### Google Drive Integration

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/dark_editor_v2/api/drive/upload` | Upload to Drive |
| POST | `/dark_editor_v2/api/drive/upload/direct` | Direct upload |
| POST | `/dark_editor_v2/api/drive/folders` | Create folder |
| GET | `/dark_editor_v2/api/drive/files` | List files |
| GET | `/dark_editor_v2/api/drive/files/:id` | Get file info |
| GET | `/dark_editor_v2/api/drive/files/:id/download` | Download file |
| POST | `/dark_editor_v2/api/drive/files/:id/share` | Share file |
| DELETE | `/dark_editor_v2/api/drive/files/:id` | Delete file |
| GET | `/dark_editor_v2/api/drive/storage` | Storage info |
| POST | `/dark_editor_v2/api/drive/sync` | Sync project |
| GET | `/dark_editor_v2/api/drive/health` | Health check |

### Projects API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/dark_editor_v2/api/projects` | List projects |
| POST | `/dark_editor_v2/api/projects` | Save project |
| GET | `/dark_editor_v2/api/projects/:id` | Load project |
| DELETE | `/dark_editor_v2/api/projects/:id` | Delete project |

## Configuration

Environment variables:

| Variable | Description |
|----------|-------------|
| `NVIDIA_API_KEY` | NVIDIA FLUX API key for AI generation |
| `REMBG_PYTHON_PATH` | Path to Python for rembg |
| `REMBG_USE_API` | Set to "true" for external API |
| `REMBG_API_ENDPOINT` | External rembg API endpoint |
| `REMBG_API_KEY` | External rembg API key |
| `DARK_EDITOR_DRIVE_FOLDER` | Default Drive folder ID |

## Usage

```go
import (
    "velox-server/internal/handlers/server/darkeditor"
)

// Create handler
cfg := &darkeditor.Config{
    TempDir:      "/path/to/temp",
    ProjectsDir:  "/path/to/projects",
    NVIDIAAPIKey: "your-api-key",
}
handler := darkeditor.NewHandler(cfg)

// Register routes
darkeditor.RegisterAPIRoutes(r, handler)

// Register routes
darkeditor.RegisterAPIRoutes(r, handler)
```

## Dependencies

- `github.com/gin-gonic/gin` - HTTP framework
- `github.com/disintegration/imaging` - Image processing
- `github.com/google/uuid` - UUID generation
- `velox-server/internal/store` - Project storage
- `velox-server/internal/integrations/social` - Social platform API (generic)
- `velox-server/internal/integrations/drive` - Google Drive API

## See Also

- [API Documentation](../../../../../../docs/DARK_EDITOR_API.md)
- [Migration Guide](../../../../../../docs/DARK_EDITOR_V2_MIGRATION.md)