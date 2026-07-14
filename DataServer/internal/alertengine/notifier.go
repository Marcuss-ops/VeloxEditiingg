// Package alertengine / notifier.go
//
// Webhook notifiers for Slack and Telegram. Configured via
// environment variables:
//
//	VELOX_ALERT_WEBHOOK_URL  — the webhook URL to POST alerts to
//	VELOX_ALERT_WEBHOOK_TYPE — "slack" or "telegram" (default: "slack")
package alertengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// NewNotifierFromEnv returns a Notifier based on env vars, or nil
// when no webhook URL is configured.
func NewNotifierFromEnv() Notifier {
	url := os.Getenv("VELOX_ALERT_WEBHOOK_URL")
	if url == "" {
		return nil
	}
	typ := os.Getenv("VELOX_ALERT_WEBHOOK_TYPE")
	switch typ {
	case "telegram":
		return &TelegramNotifier{url: url, client: &http.Client{Timeout: 10 * time.Second}}
	default:
		return &SlackNotifier{url: url, client: &http.Client{Timeout: 10 * time.Second}}
	}
}

// SlackNotifier sends alerts to a Slack incoming webhook.
type SlackNotifier struct {
	url    string
	client *http.Client
}

func (n *SlackNotifier) Send(ctx context.Context, alert Alert) error {
	color := "#ffa500" // orange for warning
	if alert.Severity == "critical" {
		color = "#ff0000"
	}

	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{
			{
				"color":  color,
				"title":  fmt.Sprintf("[%s] %s", alert.Severity, alert.Name),
				"text":   alert.Description,
				"fields": slackFields(alert),
				"footer": "Velox AlertEngine",
				"ts":     alert.Timestamp.Unix(),
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("slack post: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("slack post: status %d", resp.StatusCode)
	}
	return nil
}

func slackFields(alert Alert) []map[string]interface{} {
	var fields []map[string]interface{}
	for k, v := range alert.Labels {
		fields = append(fields, map[string]interface{}{
			"title": k,
			"value": v,
			"short": true,
		})
	}
	return fields
}

// TelegramNotifier sends alerts to a Telegram bot via HTTP API.
type TelegramNotifier struct {
	url    string // full URL including bot token and method
	client *http.Client
}

func (n *TelegramNotifier) Send(ctx context.Context, alert Alert) error {
	emoji := "⚠️"
	if alert.Severity == "critical" {
		emoji = "🚨"
	}

	text := fmt.Sprintf("%s *[%s] %s*\n_%s_\n\n%s",
		emoji, alert.Severity, alert.Name, alert.Summary, alert.Description)

	if len(alert.Labels) > 0 {
		text += "\n"
		for k, v := range alert.Labels {
			text += fmt.Sprintf("\n• %s: `%s`", k, v)
		}
	}

	payload := map[string]interface{}{
		"chat_id":    extractTelegramChatID(n.url),
		"text":       text,
		"parse_mode": "Markdown",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal telegram payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("telegram request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram post: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("telegram post: status %d", resp.StatusCode)
	}
	return nil
}

// extractTelegramChatID pulls the numeric chat ID from a Telegram
// bot URL like https://api.telegram.org/bot<TOKEN>/sendMessage?chat_id=<ID>
func extractTelegramChatID(url string) string {
	// Simple extraction: find chat_id= in the URL.
	idx := len("chat_id=")
	for i := 0; i < len(url)-idx; i++ {
		if url[i:i+idx] == "chat_id=" {
			end := i + idx
			for end < len(url) && url[end] != '&' {
				end++
			}
			return url[i+idx : end]
		}
	}
	return ""
}
