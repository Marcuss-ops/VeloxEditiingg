package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"velox-server/internal/store/postgres"
)

// Config holds auth configuration
type Config struct {
	JWTSecret      string
	SessionExpiry  time.Duration
	CookieSecure   bool
	CookieSameSite http.SameSite
}

// Handler holds auth handlers
type Handler struct {
	store  *postgres.PostgresStore
	config *Config
}

// NewHandler creates a new auth handler
func NewHandler(store *postgres.PostgresStore, config *Config) *Handler {
	if config.SessionExpiry == 0 {
		config.SessionExpiry = 7 * 24 * time.Hour // 7 days
	}
	return &Handler{
		store:  store,
		config: config,
	}
}

// Request/Response types

type RegisterRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=8"`
	Name     string `json:"name"`
}

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type AuthResponse struct {
	User    *UserResponse `json:"user"`
	Message string        `json:"message"`
}

type UserResponse struct {
	ID        string `json:"id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
}

// Register handles user registration
func (h *Handler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Check if user already exists
	ctx := c.Request.Context()
	existingUser, err := h.store.GetUserByEmail(ctx, req.Email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check user"})
		return
	}
	if existingUser != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "Email already registered"})
		return
	}

	// Hash password
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to hash password"})
		return
	}

	// Create user
	user, err := h.store.CreateUser(ctx, req.Email, req.Name, string(passwordHash))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create user"})
		return
	}

	// Create session
	session, err := h.createSession(ctx, user.ID, c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create session"})
		return
	}

	// Set cookie
	h.setSessionCookie(c, session.TokenHash)

	c.JSON(http.StatusCreated, AuthResponse{
		User: &UserResponse{
			ID:        user.ID.String(),
			Email:     user.Email,
			Name:      user.Name,
			CreatedAt: user.CreatedAt.Format(time.RFC3339),
		},
		Message: "Registration successful",
	})
}

// Login handles user login
func (h *Handler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	ctx := c.Request.Context()

	// Get user by email
	user, err := h.store.GetUserByEmail(ctx, req.Email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user"})
		return
	}
	if user == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}

	// Create session
	session, err := h.createSession(ctx, user.ID, c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create session"})
		return
	}

	// Set cookie
	h.setSessionCookie(c, session.TokenHash)

	c.JSON(http.StatusOK, AuthResponse{
		User: &UserResponse{
			ID:        user.ID.String(),
			Email:     user.Email,
			Name:      user.Name,
			CreatedAt: user.CreatedAt.Format(time.RFC3339),
		},
		Message: "Login successful",
	})
}

// Logout handles user logout
func (h *Handler) Logout(c *gin.Context) {
	// Get session token from cookie
	token, err := c.Cookie("session_token")
	if err == nil && token != "" {
		// Delete session from database
		tokenHash := hashToken(token)
		ctx := c.Request.Context()
		if err := h.store.DeleteSession(ctx, tokenHash); err != nil {
			log.Printf("Failed to delete session: %v", err)
		}
	}

	// Clear cookie
	h.clearSessionCookie(c)

	c.JSON(http.StatusOK, gin.H{"message": "Logged out successfully"})
}

// GetMe returns the current authenticated user
func (h *Handler) GetMe(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Not authenticated"})
		return
	}

	ctx := c.Request.Context()
	user, err := h.store.GetUserByID(ctx, userID.(uuid.UUID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user"})
		return
	}
	if user == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user": &UserResponse{
			ID:        user.ID.String(),
			Email:     user.Email,
			Name:      user.Name,
			CreatedAt: user.CreatedAt.Format(time.RFC3339),
		},
	})
}

// Middleware

// AuthMiddleware checks if the user is authenticated
func (h *Handler) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get session token from cookie
		token, err := c.Cookie("session_token")
		if err != nil || token == "" {
			// Try Authorization header as fallback
			authHeader := c.GetHeader("Authorization")
			if authHeader != "" && strings.HasPrefix(authHeader, "Bearer ") {
				token = strings.TrimPrefix(authHeader, "Bearer ")
			}
		}

		if token == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Not authenticated"})
			c.Abort()
			return
		}

		// Validate session
		tokenHash := hashToken(token)
		ctx := c.Request.Context()
		session, err := h.store.GetSessionByTokenHash(ctx, tokenHash)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to validate session"})
			c.Abort()
			return
		}
		if session == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired session"})
			c.Abort()
			return
		}

		// Get user
		user, err := h.store.GetUserByID(ctx, session.UserID)
		if err != nil || user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "User not found"})
			c.Abort()
			return
		}

		// Set user info in context
		c.Set("userID", user.ID)
		c.Set("user", user)
		c.Set("session", session)

		c.Next()
	}
}

// Helper functions

func (h *Handler) createSession(ctx context.Context, userID uuid.UUID, c *gin.Context) (*postgres.Session, error) {
	// Generate random token
	token := generateToken()
	tokenHash := hashToken(token)

	// Get request metadata
	userAgent := c.GetHeader("User-Agent")
	ipAddress := c.ClientIP()

	// Create session in database
	expiresAt := time.Now().Add(h.config.SessionExpiry)
	session, err := h.store.CreateSession(ctx, userID, tokenHash, expiresAt, userAgent, ipAddress)
	if err != nil {
		return nil, err
	}

	// Store the raw token temporarily (we'll need it for the cookie)
	session.TokenHash = token // Return the raw token for the cookie

	return session, nil
}

func (h *Handler) setSessionCookie(c *gin.Context, token string) {
	c.SetCookie(
		"session_token",
		token,
		int(h.config.SessionExpiry.Seconds()),
		"/",
		"",
		h.config.CookieSecure,
		true, // HttpOnly
	)
}

func (h *Handler) clearSessionCookie(c *gin.Context) {
	c.SetCookie(
		"session_token",
		"",
		-1,
		"/",
		"",
		h.config.CookieSecure,
		true,
	)
}

func generateToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		log.Printf("Failed to generate random token: %v", err)
		// Fallback to UUID
		return uuid.New().String()
	}
	return hex.EncodeToString(b)
}

func hashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

// RegisterRoutes registers auth routes
func RegisterRoutes(r *gin.Engine, h *Handler) {
	auth := r.Group("/api/auth")
	{
		auth.POST("/register", h.Register)
		auth.POST("/login", h.Login)
		auth.POST("/logout", h.Logout)
		auth.GET("/me", h.AuthMiddleware(), h.GetMe)
	}

	log.Printf("✅ Auth routes registered at /api/auth/*")
}
