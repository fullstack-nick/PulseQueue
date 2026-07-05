package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fullstack-nick/PulseQueue/internal/observability"
)

func TestMetricsEndpointDoesNotRequireAuth(t *testing.T) {
	handler := NewServer(nil, nil, "secret", slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics("api-test"))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestOperatorRoutesStillRequireAuth(t *testing.T) {
	handler := NewServer(nil, nil, "secret", slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics("api-test"))

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/jobs status = %d, want %d; body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}
