package store

import "context"

// ProjectStore defines the interface for project storage operations
type ProjectStore interface {
	// Project operations
	CreateProject(ctx context.Context, project *Project) error
	GetProject(ctx context.Context, id string) (*Project, error)
	UpdateProject(ctx context.Context, project *Project) error
	DeleteProject(ctx context.Context, id string) error
	ListProjects(ctx context.Context, opts ProjectListOptions) ([]*Project, error)
	AssignProjectFolder(ctx context.Context, projectID string, folderID *string) error

	// Asset operations
	CreateAsset(ctx context.Context, asset *Asset) error
	GetAsset(ctx context.Context, id string) (*Asset, error)
	GetAssetByFilename(ctx context.Context, filename string) (*Asset, error)
	DeleteAsset(ctx context.Context, id string) error
	ListProjectAssets(ctx context.Context, projectID string) ([]*Asset, error)

	// Template operations
	CreateTemplate(ctx context.Context, template *Template) error
	GetTemplate(ctx context.Context, id string) (*Template, error)
	UpdateTemplate(ctx context.Context, template *Template) error
	DeleteTemplate(ctx context.Context, id string) error
	ListTemplates(ctx context.Context, opts TemplateListOptions) ([]*Template, error)
	IncrementTemplateUsage(ctx context.Context, id string) error

	// Temp file operations
	CreateTempFile(ctx context.Context, file *TempFile) error
	GetTempFile(ctx context.Context, filename string) (*TempFile, error)
	DeleteTempFile(ctx context.Context, filename string) error
	CleanupExpiredTempFiles(ctx context.Context) (int64, error)

	// Folder operations
	ListFolders(ctx context.Context) ([]*Folder, error)
	GetFolder(ctx context.Context, id string) (*Folder, error)
	CreateFolder(ctx context.Context, folder *Folder) error
	UpdateFolder(ctx context.Context, folder *Folder) error
	DeleteFolder(ctx context.Context, id string) error

	// Generation history
	CreateGenerationRecord(ctx context.Context, record *GenerationRecord) error
	ListGenerationHistory(ctx context.Context, opts GenerationListOptions) ([]*GenerationRecord, error)

	// Database operations
	Close() error
	Ping() error
}
