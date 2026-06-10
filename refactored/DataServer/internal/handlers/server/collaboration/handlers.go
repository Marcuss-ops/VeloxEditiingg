package collaboration

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/store/postgres"
)

// Handler holds collaboration handlers
type Handler struct {
	store *postgres.PostgresStore
	db    *sql.DB
}

// NewHandler creates a new collaboration handler
func NewHandler(store *postgres.PostgresStore) *Handler {
	return &Handler{
		store: store,
		db:    nil, // Will be set via SetDB
	}
}

// SetDB sets the database connection
func (h *Handler) SetDB(db *sql.DB) {
	h.db = db
}

// Request/Response types

type InviteCollaboratorRequest struct {
	Email string `json:"email" binding:"required,email"`
	Role  string `json:"role"` // owner, editor, viewer
}

type CollaboratorResponse struct {
	ID         string `json:"id"`
	UserID     string `json:"user_id"`
	Email      string `json:"email"`
	Name       string `json:"name"`
	Role       string `json:"role"`
	InvitedAt  string `json:"invited_at"`
	AcceptedAt string `json:"accepted_at,omitempty"`
}

type ShareProjectRequest struct {
	Public bool `json:"is_public"`
}

// InviteCollaborator invites a user to collaborate on a project
func (h *Handler) InviteCollaborator(c *gin.Context) {
	projectIDStr := c.Param("id")
	projectID, err := uuid.Parse(projectIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid project ID"})
		return
	}

	var req InviteCollaboratorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// Default role is viewer
	if req.Role == "" {
		req.Role = "viewer"
	}

	// Validate role
	if req.Role != "owner" && req.Role != "editor" && req.Role != "viewer" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid role. Must be owner, editor, or viewer"})
		return
	}

	ctx := c.Request.Context()

	// Get current user ID
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Not authenticated"})
		return
	}

	// Check if project exists and user is owner
	project, err := h.store.GetProjectByID(ctx, projectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get project"})
		return
	}
	if project == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
		return
	}

	// Only owner can invite collaborators
	if project.UserID != userID.(uuid.UUID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Only the project owner can invite collaborators"})
		return
	}

	// Find user by email
	invitee, err := h.store.GetUserByEmail(ctx, req.Email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to find user"})
		return
	}
	if invitee == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found with this email"})
		return
	}

	// Check if already a collaborator
	existingCollab, err := h.getCollaborator(ctx, projectID, invitee.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check collaborator"})
		return
	}
	if existingCollab != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "User is already a collaborator"})
		return
	}

	// Create collaborator record
	collabID := uuid.New()
	query := `
		INSERT INTO project_collaborators (id, project_id, user_id, role, invited_by, invited_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`

	_, err = h.db.ExecContext(ctx, query,
		collabID, projectID, invitee.ID, req.Role, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to invite collaborator"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Collaborator invited successfully",
		"collaborator": CollaboratorResponse{
			ID:        collabID.String(),
			UserID:    invitee.ID.String(),
			Email:     invitee.Email,
			Name:      invitee.Name,
			Role:      req.Role,
			InvitedAt: time.Now().Format(time.RFC3339),
		},
	})
}

// ListCollaborators lists all collaborators for a project
func (h *Handler) ListCollaborators(c *gin.Context) {
	projectIDStr := c.Param("id")
	projectID, err := uuid.Parse(projectIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid project ID"})
		return
	}

	ctx := c.Request.Context()

	// Check if project exists
	project, err := h.store.GetProjectByID(ctx, projectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get project"})
		return
	}
	if project == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
		return
	}

	// Get collaborators
	query := `
		SELECT pc.id, pc.user_id, u.email, u.name, pc.role, pc.invited_at, pc.accepted_at
		FROM project_collaborators pc
		JOIN users u ON pc.user_id = u.id
		WHERE pc.project_id = $1
		ORDER BY pc.invited_at DESC
	`

	rows, err := h.db.QueryContext(ctx, query, projectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get collaborators"})
		return
	}
	defer rows.Close()

	var collaborators []CollaboratorResponse
	for rows.Next() {
		var collab CollaboratorResponse
		var acceptedAt sql.NullString
		err := rows.Scan(&collab.ID, &collab.UserID, &collab.Email, &collab.Name, &collab.Role, &collab.InvitedAt, &acceptedAt)
		if err != nil {
			log.Printf("Failed to scan collaborator: %v", err)
			continue
		}
		if acceptedAt.Valid {
			collab.AcceptedAt = acceptedAt.String
		}
		collaborators = append(collaborators, collab)
	}

	// Add owner to the list
	owner := CollaboratorResponse{
		UserID: project.UserID.String(),
		Role:   "owner",
	}

	// Get owner details
	ownerUser, _ := h.store.GetUserByID(ctx, project.UserID)
	if ownerUser != nil {
		owner.Email = ownerUser.Email
		owner.Name = ownerUser.Name
	}

	collaborators = append([]CollaboratorResponse{owner}, collaborators...)

	c.JSON(http.StatusOK, collaborators)
}

// RemoveCollaborator removes a collaborator from a project
func (h *Handler) RemoveCollaborator(c *gin.Context) {
	projectIDStr := c.Param("id")
	projectID, err := uuid.Parse(projectIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid project ID"})
		return
	}

	collaboratorIDStr := c.Param("collaborator_id")
	collaboratorID, err := uuid.Parse(collaboratorIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid collaborator ID"})
		return
	}

	ctx := c.Request.Context()

	// Get current user ID
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Not authenticated"})
		return
	}

	// Check if project exists and user is owner
	project, err := h.store.GetProjectByID(ctx, projectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get project"})
		return
	}
	if project == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
		return
	}

	// Only owner can remove collaborators
	if project.UserID != userID.(uuid.UUID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Only the project owner can remove collaborators"})
		return
	}

	// Remove collaborator
	query := `DELETE FROM project_collaborators WHERE id = $1 AND project_id = $2`
	result, err := h.db.ExecContext(ctx, query, collaboratorID, projectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to remove collaborator"})
		return
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Collaborator not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Collaborator removed successfully"})
}

// ShareProject makes a project public or private
func (h *Handler) ShareProject(c *gin.Context) {
	projectIDStr := c.Param("id")
	projectID, err := uuid.Parse(projectIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid project ID"})
		return
	}

	var req ShareProjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	ctx := c.Request.Context()

	// Get current user ID
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Not authenticated"})
		return
	}

	// Check if project exists and user is owner
	project, err := h.store.GetProjectByID(ctx, projectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get project"})
		return
	}
	if project == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
		return
	}

	// Only owner can change sharing settings
	if project.UserID != userID.(uuid.UUID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Only the project owner can change sharing settings"})
		return
	}

	// Update project
	query := `UPDATE projects SET is_public = $1 WHERE id = $2`
	_, err = h.db.ExecContext(ctx, query, req.Public, projectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update project"})
		return
	}

	message := "Project is now private"
	if req.Public {
		message = "Project is now public"
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   message,
		"is_public": req.Public,
	})
}

// Helper functions

func (h *Handler) getCollaborator(ctx context.Context, projectID, userID uuid.UUID) (*CollaboratorResponse, error) {
	query := `
		SELECT id, user_id, role, invited_at, accepted_at
		FROM project_collaborators
		WHERE project_id = $1 AND user_id = $2
	`

	var collab CollaboratorResponse
	var acceptedAt sql.NullString
	err := h.db.QueryRowContext(ctx, query, projectID, userID).Scan(
		&collab.ID, &collab.UserID, &collab.Role, &collab.InvitedAt, &acceptedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if acceptedAt.Valid {
		collab.AcceptedAt = acceptedAt.String
	}

	return &collab, nil
}

// RegisterRoutes registers collaboration routes
func RegisterRoutes(r *gin.Engine, h *Handler, authMiddleware gin.HandlerFunc, db *sql.DB) {
	h.SetDB(db)

	collab := r.Group("/api/projects/:id/collaborators")
	collab.Use(authMiddleware)
	{
		collab.GET("", h.ListCollaborators)
		collab.POST("", h.InviteCollaborator)
		collab.DELETE("/:collaborator_id", h.RemoveCollaborator)
	}

	share := r.Group("/api/projects/:id/share")
	share.Use(authMiddleware)
	{
		share.POST("", h.ShareProject)
	}

	log.Printf("✅ Collaboration routes registered at /api/projects/:id/collaborators/*")
}
