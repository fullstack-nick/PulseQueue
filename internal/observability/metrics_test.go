package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

func TestMetricsHandlerExposesRecordedCounters(t *testing.T) {
	metrics := NewMetrics("test-service")
	metrics.RecordJobSubmitted("default", "demo.echo", false)
	metrics.RecordHTTPRequest(http.MethodPost, "/jobs", http.StatusCreated, 25*time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	for _, want := range []string{
		`pulsequeue_jobs_submitted_total{existing="false",job_type="demo.echo",queue="default",service="test-service"} 1`,
		`pulsequeue_http_requests_total{method="POST",route="/jobs",service="test-service",status="201"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q\n%s", want, body)
		}
	}
}

func TestJobFailureRecordsDeadLetterCounter(t *testing.T) {
	metrics := NewMetrics("worker-test")
	metrics.RecordJobFailed(storage.Job{
		Queue: "default",
		Type:  "demo.fail",
	}, storage.StatusDeadLetter, time.Second)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), `pulsequeue_jobs_dead_lettered_total{job_type="demo.fail",queue="default",service="worker-test"} 1`) {
		t.Fatalf("expected dead-letter counter in metrics output\n%s", rec.Body.String())
	}
}
