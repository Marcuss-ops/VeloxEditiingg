package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// Config holds PostgreSQL configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// PostgresStore implements the store interface with PostgreSQL
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates a new PostgreSQL store
func NewPostgresStore(cfg *Config) (*PostgresStore, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	log.Printf("Connected to PostgreSQL: %s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, cfg.Database)

	return &PostgresStore{db: db}, nil
}

// Close closes the database connection
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// User operations

// CreateUser creates a new user
func (s *PostgresStore) CreateUser(ctx context.Context, email, name, passwordHash string) (*User, error) {
	id := uuid.New()
	query := `
		INSERT INTO users (id, email, name, password_hash)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at, updated_at
	`

	user := &User{
		ID:           id,
		Email:        email,
		Name:         name,
		PasswordHash: passwordHash,
	}

	err := s.db.QueryRowContext(ctx, query, id, email, name, passwordHash).Scan(&user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return user, nil
}

// GetUserByID retrieves a user by ID
func (s *PostgresStore) GetUserByID(ctx context.Context, id uuid.UUID) (*User, error) {
	query := `
		SELECT id, email, name, password_hash, created_at, updated_at
		FROM users WHERE id = $1
	`

	user := &User{}
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&user.ID, &user.Email, &user.Name, &user.PasswordHash, &user.CreatedAt, &user.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return user, nil
}

// GetUserByEmail retrieves a user by email
func (s *PostgresStore) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	query := `
		SELECT id, email, name, password_hash, created_at, updated_at
		FROM users WHERE email = $1
	`

	user := &User{}
	err := s.db.QueryRowContext(ctx, query, email).Scan(
		&user.ID, &user.Email, &user.Name, &user.PasswordHash, &user.CreatedAt, &user.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return user, nil
}

// Project operations

// CreateProject creates a new project
func (s *PostgresStore) CreateProject(ctx context.Context, userID uuid.UUID, name, projectType string, canvasJSON []byte) (*Project, error) {
	id := uuid.New()
	query := `
		INSERT INTO projects (id, user_id, name, type, canvas_json)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING created_at, updated_at
	`

	project := &Project{
		ID:         id,
		UserID:     userID,
		Name:       name,
		Type:       projectType,
		CanvasJSON: canvasJSON,
	}

	err := s.db.QueryRowContext(ctx, query, id, userID, name, projectType, canvasJSON).Scan(&project.CreatedAt, &project.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create project: %w", err)
	}

	return project, nil
}

// GetProjectByID retrieves a project by ID
func (s *PostgresStore) GetProjectByID(ctx context.Context, id uuid.UUID) (*Project, error) {
	query := `
		SELECT id, user_id, name, type, canvas_json, preview_url, is_public, created_at, updated_at
		FROM projects WHERE id = $1
	`

	project := &Project{}
	var previewURL sql.NullString
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&project.ID, &project.UserID, &project.Name, &project.Type,
		&project.CanvasJSON, &previewURL, &project.IsPublic,
		&project.CreatedAt, &project.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get project: %w", err)
	}

	project.PreviewURL = previewURL.String
	return project, nil
}

// ListProjectsByUserID lists all projects for a user
func (s *PostgresStore) ListProjectsByUserID(ctx context.Context, userID uuid.UUID, limit, offset int) ([]*Project, error) {
	query := `
		SELECT id, user_id, name, type, canvas_json, preview_url, is_public, created_at, updated_at
		FROM projects WHERE user_id = $1
		ORDER BY updated_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := s.db.QueryContext(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list projects: %w", err)
	}
	defer rows.Close()

	var projects []*Project
	for rows.Next() {
		project := &Project{}
		var previewURL sql.NullString
		err := rows.Scan(
			&project.ID, &project.UserID, &project.Name, &project.Type,
			&project.CanvasJSON, &previewURL, &project.IsPublic,
			&project.CreatedAt, &project.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan project: %w", err)
		}
		project.PreviewURL = previewURL.String
		projects = append(projects, project)
	}

	return projects, nil
}

// UpdateProject updates a project
func (s *PostgresStore) UpdateProject(ctx context.Context, id uuid.UUID, name string, canvasJSON []byte, previewURL string) error {
	query := `
		UPDATE projects
		SET name = COALESCE(NULLIF($2, ''), name),
		    canvas_json = COALESCE($3, canvas_json),
		    preview_url = COALESCE(NULLIF($4, ''), preview_url),
		    updated_at = NOW()
		WHERE id = $1
	`

	result, err := s.db.ExecContext(ctx, query, id, name, canvasJSON, previewURL)
	if err != nil {
		return fmt.Errorf("failed to update project: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if affected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// DeleteProject deletes a project
func (s *PostgresStore) DeleteProject(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM projects WHERE id = $1`

	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete project: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if affected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// Session operations

// CreateSession creates a new session
func (s *PostgresStore) CreateSession(ctx context.Context, userID uuid.UUID, tokenHash string, expiresAt time.Time, userAgent, ipAddress string) (*Session, error) {
	id := uuid.New()
	query := `
		INSERT INTO sessions (id, user_id, token_hash, expires_at, user_agent, ip_address)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at
	`

	session := &Session{
		ID:        id,
		UserID:    userID,
		TokenHash: tokenHash,
		ExpiresAt: expiresAt,
		UserAgent: userAgent,
		IPAddress: ipAddress,
	}

	err := s.db.QueryRowContext(ctx, query, id, userID, tokenHash, expiresAt, userAgent, ipAddress).Scan(&session.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	return session, nil
}

// GetSessionByTokenHash retrieves a session by token hash
func (s *PostgresStore) GetSessionByTokenHash(ctx context.Context, tokenHash string) (*Session, error) {
	query := `
		SELECT id, user_id, token_hash, expires_at, created_at, user_agent, ip_address
		FROM sessions WHERE token_hash = $1 AND expires_at > NOW()
	`

	session := &Session{}
	var userAgent, ipAddress sql.NullString
	err := s.db.QueryRowContext(ctx, query, tokenHash).Scan(
		&session.ID, &session.UserID, &session.TokenHash, &session.ExpiresAt,
		&session.CreatedAt, &userAgent, &ipAddress,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	session.UserAgent = userAgent.String
	session.IPAddress = ipAddress.String
	return session, nil
}

// DeleteSession deletes a session
func (s *PostgresStore) DeleteSession(ctx context.Context, tokenHash string) error {
	query := `DELETE FROM sessions WHERE token_hash = $1`
	_, err := s.db.ExecContext(ctx, query, tokenHash)
	return err
}

// CleanExpiredSessions removes expired sessions
func (s *PostgresStore) CleanExpiredSessions(ctx context.Context) (int64, error) {
	query := `DELETE FROM sessions WHERE expires_at < NOW()`
	result, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Types

type User struct {
	ID           uuid.UUID
	Email        string
	Name         string
	PasswordHash string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Project struct {
	ID         uuid.UUID
	UserID     uuid.UUID
	Name       string
	Type       string
	CanvasJSON []byte
	PreviewURL string
	IsPublic   bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Session struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	TokenHash string
	ExpiresAt time.Time
	CreatedAt time.Time
	UserAgent string
	IPAddress string
}
