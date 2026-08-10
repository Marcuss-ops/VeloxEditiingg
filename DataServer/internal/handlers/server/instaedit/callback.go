package instaedit

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

const callbackClockSkew = 5 * time.Minute

// deliveryEventRequest is the canonical velox.delivery.event.v1 body.
type deliveryEventRequest struct {
	ContractVersion    string    `json:"contract_version"`
	EventID            string    `json:"event_id"`
	DeliveryID         string    `json:"delivery_id"`
	PublicationID      string    `json:"publication_id,omitempty"`
	SocialDeliveryID   string    `json:"social_delivery_id,omitempty"`
	ExternalDeliveryID string    `json:"external_delivery_id,omitempty"`
	Sequence           int64     `json:"sequence"`
	Status             string    `json:"status"`
	Phase              string    `json:"phase,omitempty"`
	RemoteID           string    `json:"remote_id,omitempty"`
	RemoteURL          string    `json:"remote_url,omitempty"`
	ErrorCode          string    `json:"error_code,omitempty"`
	ErrorMessage       string    `json:"error_message,omitempty"`
	OccurredAt         time.Time `json:"occurred_at"`
}

// RegisterInternalCallbackRoute mounts the HMAC-authenticated callback
// outside the browser JWT route group. It persists and acknowledges only;
// slow publication work remains in the delivery runner.
func (h *Handler) RegisterInternalCallbackRoute(r *gin.Engine) {
	if h == nil || h.deps.WebhookSecret == "" || h.deps.Service == nil {
		return
	}
	r.POST("/internal/v1/instaedit/delivery-events", h.deliveryEvents())
}

func (h *Handler) deliveryEvents() gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
		if err != nil {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error_code": "BODY_TOO_LARGE"})
			return
		}
		tsRaw := firstHeader(c, "X-Timestamp", "X-Velox-Timestamp")
		sigRaw := firstHeader(c, "X-Signature", "X-Velox-Signature")
		eventHeader := firstHeader(c, "X-Event-ID", "X-Velox-Event-ID")
		if !verifyCallbackSignature(h.deps.WebhookSecret, tsRaw, sigRaw, body) {
			c.JSON(http.StatusUnauthorized, gin.H{"error_code": "INVALID_CALLBACK_SIGNATURE"})
			return
		}
		var event deliveryEventRequest
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error_code": "INVALID_CALLBACK", "message": err.Error()})
			return
		}
		if event.ContractVersion != "velox.delivery.event.v1" || event.EventID == "" || event.EventID != eventHeader || event.DeliveryID == "" || event.Sequence <= 0 || event.Status == "" {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error_code": "INVALID_CALLBACK"})
			return
		}
		if event.OccurredAt.IsZero() {
			event.OccurredAt = time.Now().UTC()
		}
		payload, _ := json.Marshal(event)
		_, err = h.deps.Service.ApplyDeliveryEvent(c.Request.Context(), store.InstaEditDeliveryEvent{
			EventID: event.EventID, DeliveryID: event.DeliveryID,
			SocialDeliveryID: event.SocialDeliveryID, Sequence: event.Sequence,
			Status: event.Status, Phase: event.Phase, RemoteID: event.RemoteID,
			RemoteURL: event.RemoteURL, ErrorCode: event.ErrorCode,
			ErrorMessage: event.ErrorMessage, OccurredAt: event.OccurredAt,
			Payload: payload,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error_code": "CALLBACK_PERSISTENCE_FAILED"})
			return
		}
		c.Status(http.StatusNoContent)
	}
}

func firstHeader(c *gin.Context, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(c.GetHeader(name)); value != "" {
			return value
		}
	}
	return ""
}

func verifyCallbackSignature(secret, timestamp, signature string, body []byte) bool {
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || time.Since(time.Unix(seconds, 0)) > callbackClockSkew || time.Unix(seconds, 0).Sub(time.Now()) > callbackClockSkew {
		return false
	}
	signature = strings.TrimPrefix(signature, "sha256=")
	want, err := hex.DecodeString(signature)
	if err != nil || len(want) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return hmac.Equal(want, mac.Sum(nil))
}
