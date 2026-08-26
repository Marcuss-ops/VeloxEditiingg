package alerts_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"velox-server/internal/alerts"
)

func TestNewWebhookNotifierRoutesCanonicalAlert(t *testing.T) {
	for _, tc := range []struct {
		name       string
		typ        string
		urlSuffix  string
		wantBody   string
		wantMethod string
	}{
		{name: "slack", typ: "slack", wantBody: "source.test", wantMethod: http.MethodPost},
		{name: "telegram", typ: "telegram", urlSuffix: "?chat_id=123", wantBody: "source.test", wantMethod: http.MethodPost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body string
			var method string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method = r.Method
				data, _ := io.ReadAll(r.Body)
				body = string(data)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			notifier := alerts.NewWebhookNotifier(server.URL+tc.urlSuffix, tc.typ)
		if notifier == nil {
			t.Fatal("NewWebhookNotifier returned nil for configured URL")
		}
		err := notifier.Notify(context.Background(), alerts.Alert{
			Source:   "source.test",
			Severity: alerts.SeverityError,
			Subject:  "subject-1",
			Body:     "failure detail",
			Tags:     map[string]string{"job_id": "job-1"},
		})
		if err != nil {
			t.Fatalf("Notify: %v", err)
		}
		if method != tc.wantMethod {
			t.Fatalf("method = %q, want %q", method, tc.wantMethod)
		}
		if !strings.Contains(body, tc.wantBody) {
			t.Fatalf("body = %q, want %q", body, tc.wantBody)
		}
	})
	}
}

func TestNewWebhookNotifierEmptyURLIsDisabled(t *testing.T) {
	if notifier := alerts.NewWebhookNotifier("  ", "slack"); notifier != nil {
		t.Fatal("NewWebhookNotifier returned a notifier for an empty URL")
	}
}
