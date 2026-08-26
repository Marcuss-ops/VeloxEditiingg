package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// NewWebhookNotifier builds the configured external webhook notifier. An
// empty URL disables external delivery and returns nil; the composition root
// still supplies the always-on LogNotifier.
func NewWebhookNotifier(rawURL, rawType string) Notifier {
	url := strings.TrimSpace(rawURL)
	if url == "" {
		return nil
	}
	client := &http.Client{Timeout: 10 * time.Second}
	switch strings.ToLower(strings.TrimSpace(rawType)) {
	case "telegram":
		return &TelegramNotifier{url: url, client: client}
	default:
		return &SlackNotifier{url: url, client: client}
	}
}

// SlackNotifier sends alerts to a Slack incoming webhook.
type SlackNotifier struct {
	url    string
	client *http.Client
}

// Notify implements Notifier for Slack incoming webhooks.
func (n *SlackNotifier) Notify(ctx context.Context, alert Alert) error {
	color := "#ffa500"
	if alert.Severity == SeverityError || alert.Severity == SeverityFatal {
		color = "#ff0000"
	}
	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{
			{
				"color":  color,
				"title":  fmt.Sprintf("[%s] %s", alert.Severity, alert.Source),
				"text":   alert.Body,
				"fields": slackFields(alert),
				"footer": "Velox Alerts",
				"ts":     alert.Timestamp.Unix(),
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal Slack payload: %w", err)
	}
	return postWebhook(ctx, n.client, n.url, body, "Slack")
}

func slackFields(alert Alert) []map[string]interface{} {
	fields := make([]map[string]interface{}, 0, len(alert.Tags))
	for key, value := range alert.Tags {
		fields = append(fields, map[string]interface{}{
			"title": key,
			"value": value,
			"short": true,
		})
	}
	return fields
}

// TelegramNotifier sends alerts to a Telegram bot via its HTTP API URL.
type TelegramNotifier struct {
	url    string
	client *http.Client
}

// Notify implements Notifier for Telegram bot API URLs.
func (n *TelegramNotifier) Notify(ctx context.Context, alert Alert) error {
	emoji := "⚠️"
	if alert.Severity == SeverityError || alert.Severity == SeverityFatal {
		emoji = "🚨"
	}
	text := fmt.Sprintf("%s *[%s] %s*\n_%s_\n\n%s", emoji, alert.Severity, alert.Source, alert.Subject, alert.Body)
	if len(alert.Tags) > 0 {
		text += "\n"
		for key, value := range alert.Tags {
			text += fmt.Sprintf("\n• %s: `%s`", key, value)
		}
	}
	payload := map[string]interface{}{
		"chat_id":    extractTelegramChatID(n.url),
		"text":       text,
		"parse_mode": "Markdown",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal Telegram payload: %w", err)
	}
	return postWebhook(ctx, n.client, n.url, body, "Telegram")
}

func postWebhook(ctx context.Context, client *http.Client, rawURL string, body []byte, provider string) error {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%s request: %w", provider, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s post: %w", provider, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%s post: status %d", provider, resp.StatusCode)
	}
	return nil
}

func extractTelegramChatID(rawURL string) string {
	const marker = "chat_id="
	idx := strings.Index(rawURL, marker)
	if idx < 0 {
		return ""
	}
	value := rawURL[idx+len(marker):]
	if end := strings.IndexByte(value, '&'); end >= 0 {
		value = value[:end]
	}
	return value
}
