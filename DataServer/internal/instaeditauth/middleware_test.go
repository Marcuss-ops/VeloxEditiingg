package instaeditauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func setupGin() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return gin.New()
}

func TestMiddleware_ValidToken_Passes(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	var captured *Claims
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		captured = FromContext(c)
		c.JSON(200, gin.H{"ok": true})
	})

	token := mintToken(t, testSecret, validClaims())
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if captured == nil {
		t.Fatal("expected claims in context")
	}
	if captured.WorkspaceID != 45 {
		t.Fatalf("workspace_id mismatch: %d", captured.WorkspaceID)
	}
}

func TestMiddleware_WrongIssuer_401(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	claims := validClaims()
	claims.Issuer = "evil-issuer"
	token := mintToken(t, testSecret, claims)
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong issuer, got %d", w.Code)
	}
}

func TestMiddleware_MissingToken_401(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestMiddleware_BadToken_401(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer garbage.token.here")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestMiddleware_FreeXUserID_Rejected(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	// Even with a valid token, the free header must be rejected first.
	token := mintToken(t, testSecret, validClaims())
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-User-ID", "999")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for free X-User-ID header, got %d", w.Code)
	}
}

func TestMiddleware_FreeXWorkspaceID_Rejected(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	token := mintToken(t, testSecret, validClaims())
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Workspace-ID", "999")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for free X-Workspace-ID header, got %d", w.Code)
	}
}

func TestMiddleware_ScopeEnforcement_Pass(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, []string{"velox:jobs:read"}), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	token := mintToken(t, testSecret, validClaims())
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200 with sufficient scope, got %d", w.Code)
	}
}

func TestMiddleware_ScopeEnforcement_Fail_403(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, []string{"velox:assets:write"}), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	token := mintToken(t, testSecret, validClaims())
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for missing scope, got %d", w.Code)
	}
}

func TestMiddleware_NilVerifier_503(t *testing.T) {
	r := setupGin()
	r.GET("/test", Middleware(nil, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for nil verifier, got %d", w.Code)
	}
}

func TestMiddleware_ExpiredToken_401(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	c := validClaims()
	c.ExpiresAt = time.Now().Add(-1 * time.Minute).Unix()
	token := mintToken(t, testSecret, c)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for expired token, got %d", w.Code)
	}
}

func TestMiddleware_MissingWorkspaceID_401(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	c := validClaims()
	c.WorkspaceID = 0
	token := mintToken(t, testSecret, c)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing workspace_id, got %d", w.Code)
	}
}

func TestMiddleware_NoBearerPrefix_401(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	token := mintToken(t, testSecret, validClaims())
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", token) // no "Bearer " prefix
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing Bearer prefix, got %d", w.Code)
	}
}

func TestMiddleware_FromContext_NilWhenNoMiddleware(t *testing.T) {
	r := setupGin()
	var captured *Claims
	r.GET("/test", func(c *gin.Context) {
		captured = FromContext(c)
		c.JSON(200, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if captured != nil {
		t.Fatal("expected nil claims when middleware did not run")
	}
}

func TestMiddleware_CaseInsensitiveBearer(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/test", Middleware(v, nil), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	token := mintToken(t, testSecret, validClaims())
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "bearer "+token) // lowercase
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200 with lowercase bearer, got %d: %s", w.Code, w.Body.String())
	}
}

func TestExtractBearerToken_Empty(t *testing.T) {
	if extractBearerToken("") != "" {
		t.Fatal("expected empty string for empty header")
	}
}

func TestExtractBearerToken_NoPrefix(t *testing.T) {
	if extractBearerToken("just-a-token") != "" {
		t.Fatal("expected empty string for non-bearer header")
	}
}

func TestHasFreeIdentityHeaders_Neither(t *testing.T) {
	r := setupGin()
	r.GET("/test", func(c *gin.Context) {
		if hasFreeIdentityHeaders(c) {
			t.Fatal("expected false when neither header present")
		}
		c.JSON(200, gin.H{"ok": true})
	})
	req := httptest.NewRequest("GET", "/test", nil)
	r.ServeHTTP(httptest.NewRecorder(), req)
}

func TestHasFreeIdentityHeaders_Both(t *testing.T) {
	r := setupGin()
	r.GET("/test", func(c *gin.Context) {
		if !hasFreeIdentityHeaders(c) {
			t.Fatal("expected true when both headers present")
		}
		c.JSON(200, gin.H{"ok": true})
	})
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-User-ID", "1")
	req.Header.Set("X-Workspace-ID", "2")
	r.ServeHTTP(httptest.NewRecorder(), req)
}
