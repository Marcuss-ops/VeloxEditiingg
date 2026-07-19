package translation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	BaseURL string
	Model   string
	HTTP    *http.Client
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Message chatMessage `json:"message"`
}

func (c Client) Translate(ctx context.Context, text, targetLanguage string) (string, error) {
	text = strings.TrimSpace(text)
	targetLanguage = strings.TrimSpace(targetLanguage)
	if text == "" || targetLanguage == "" {
		return "", fmt.Errorf("translation requires non-empty text and target language")
	}
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	model := strings.TrimSpace(c.Model)
	if base == "" || model == "" {
		return "", fmt.Errorf("translation provider is not configured")
	}
	body, err := json.Marshal(chatRequest{
		Model:  model,
		Stream: false,
		Messages: []chatMessage{
			{Role: "system", Content: "Translate faithfully. Return only the translated text, with no commentary, labels, or quotation marks."},
			{Role: "user", Content: "Translate the following scene into " + targetLanguage + ":\n\n" + text},
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal translation request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build translation request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("translation provider request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("translation provider returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var decoded chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode translation response: %w", err)
	}
	out := strings.TrimSpace(decoded.Message.Content)
	if out == "" {
		return "", fmt.Errorf("translation provider returned empty text")
	}
	return out, nil
}
