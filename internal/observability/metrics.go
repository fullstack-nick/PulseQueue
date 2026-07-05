package observability

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

const namespace = "pulsequeue"

type Metrics struct {
	serviceName string
	registry    *prometheus.Registry

	httpRequests       *prometheus.CounterVec
	httpDuration       *prometheus.HistogramVec
	jobsSubmitted      *prometheus.CounterVec
	jobsStarted        *prometheus.CounterVec
	jobsSucceeded      *prometheus.CounterVec
	jobsFailed         *prometheus.CounterVec
	jobsRetried        *prometheus.CounterVec
	jobsDeadLettered   *prometheus.CounterVec
	jobDuration        *prometheus.HistogramVec
	jobLatency         *prometheus.HistogramVec
	workerHeartbeat    *prometheus.CounterVec
	schedulerTicks     *prometheus.CounterVec
	schedulerRecovered *prometheus.CounterVec
	cronJobsFired      *prometheus.CounterVec
	natsPublishFailed  *prometheus.CounterVec
}

func NewMetrics(serviceName string) *Metrics {
	if serviceName == "" {
		serviceName = "pulsequeue"
	}
	m := &Metrics{
		serviceName: serviceName,
		registry:    prometheus.NewRegistry(),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_requests_total",
			Help:      "Total HTTP requests handled by PulseQueue.",
		}, []string{"service", "method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds.",
			Buckets:   durationBuckets(),
		}, []string{"service", "method", "route", "status"}),
		jobsSubmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_submitted_total",
			Help:      "Jobs accepted by the API.",
		}, []string{"service", "queue", "job_type", "existing"}),
		jobsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_started_total",
			Help:      "Job attempts started by workers.",
		}, []string{"service", "queue", "job_type"}),
		jobsSucceeded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_succeeded_total",
			Help:      "Jobs completed successfully by workers.",
		}, []string{"service", "queue", "job_type"}),
		jobsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_failed_total",
			Help:      "Job attempts that failed or timed out.",
		}, []string{"service", "queue", "job_type", "final_status"}),
		jobsRetried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_retried_total",
			Help:      "Jobs manually retried through the API.",
		}, []string{"service", "queue", "job_type"}),
		jobsDeadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_dead_lettered_total",
			Help:      "Jobs moved to dead_letter by workers or scheduler recovery.",
		}, []string{"service", "queue", "job_type"}),
		jobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_duration_seconds",
			Help:      "Worker job attempt duration in seconds.",
			Buckets:   durationBuckets(),
		}, []string{"service", "queue", "job_type", "status"}),
		jobLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_latency_seconds",
			Help:      "Time between job creation and worker attempt start.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}, []string{"service", "queue", "job_type"}),
		workerHeartbeat: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "worker_heartbeat_total",
			Help:      "Worker heartbeat updates by queue and worker status.",
		}, []string{"service", "queue", "status"}),
		schedulerTicks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "scheduler_ticks_total",
			Help:      "Scheduler ticks by result.",
		}, []string{"service", "result"}),
		schedulerRecovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "scheduler_recovered_jobs_total",
			Help:      "Expired jobs recovered by the scheduler.",
		}, []string{"service", "queue", "status"}),
		cronJobsFired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cron_jobs_fired_total",
			Help:      "Cron jobs fired by the scheduler.",
		}, []string{"service", "queue", "job_type"}),
		natsPublishFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "nats_publish_failures_total",
			Help:      "NATS publish failures by component and queue.",
		}, []string{"service", "component", "queue"}),
	}

	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.httpRequests,
		m.httpDuration,
		m.jobsSubmitted,
		m.jobsStarted,
		m.jobsSucceeded,
		m.jobsFailed,
		m.jobsRetried,
		m.jobsDeadLettered,
		m.jobDuration,
		m.jobLatency,
		m.workerHeartbeat,
		m.schedulerTicks,
		m.schedulerRecovered,
		m.cronJobsFired,
		m.natsPublishFailed,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		m = NewMetrics("pulsequeue")
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) RegisterStoreCollector(store *storage.Store) {
	if m == nil || store == nil {
		return
	}
	m.registry.MustRegister(newStoreCollector(store))
}

func (m *Metrics) RecordHTTPRequest(method, route string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(m.serviceName, method, normalizeRoute(route), strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(m.serviceName, method, normalizeRoute(route), strconv.Itoa(status)).Observe(duration.Seconds())
}

func (m *Metrics) RecordJobSubmitted(queue, jobType string, existing bool) {
	if m == nil {
		return
	}
	m.jobsSubmitted.WithLabelValues(m.serviceName, normalizeLabel(queue, "default"), normalizeLabel(jobType, "unknown"), strconv.FormatBool(existing)).Inc()
}

func (m *Metrics) RecordJobStarted(job storage.Job) {
	if m == nil {
		return
	}
	m.jobsStarted.WithLabelValues(m.serviceName, normalizeLabel(job.Queue, "default"), normalizeLabel(job.Type, "unknown")).Inc()
	m.jobLatency.WithLabelValues(m.serviceName, normalizeLabel(job.Queue, "default"), normalizeLabel(job.Type, "unknown")).Observe(time.Since(job.CreatedAt).Seconds())
}

func (m *Metrics) RecordJobSucceeded(job storage.Job, duration time.Duration) {
	if m == nil {
		return
	}
	queue := normalizeLabel(job.Queue, "default")
	jobType := normalizeLabel(job.Type, "unknown")
	m.jobsSucceeded.WithLabelValues(m.serviceName, queue, jobType).Inc()
	m.jobDuration.WithLabelValues(m.serviceName, queue, jobType, storage.StatusSucceeded).Observe(duration.Seconds())
}

func (m *Metrics) RecordJobFailed(job storage.Job, finalStatus string, duration time.Duration) {
	if m == nil {
		return
	}
	queue := normalizeLabel(job.Queue, "default")
	jobType := normalizeLabel(job.Type, "unknown")
	status := normalizeLabel(finalStatus, storage.StatusFailed)
	m.jobsFailed.WithLabelValues(m.serviceName, queue, jobType, status).Inc()
	m.jobDuration.WithLabelValues(m.serviceName, queue, jobType, status).Observe(duration.Seconds())
	if finalStatus == storage.StatusDeadLetter {
		m.jobsDeadLettered.WithLabelValues(m.serviceName, queue, jobType).Inc()
	}
}

func (m *Metrics) RecordJobRetried(job storage.Job) {
	if m == nil {
		return
	}
	m.jobsRetried.WithLabelValues(m.serviceName, normalizeLabel(job.Queue, "default"), normalizeLabel(job.Type, "unknown")).Inc()
}

func (m *Metrics) RecordWorkerHeartbeat(queue, status string) {
	if m == nil {
		return
	}
	m.workerHeartbeat.WithLabelValues(m.serviceName, normalizeLabel(queue, "default"), normalizeLabel(status, "unknown")).Inc()
}

func (m *Metrics) RecordSchedulerTick(result string) {
	if m == nil {
		return
	}
	m.schedulerTicks.WithLabelValues(m.serviceName, normalizeLabel(result, "unknown")).Inc()
}

func (m *Metrics) RecordSchedulerRecovered(job storage.Job) {
	if m == nil {
		return
	}
	m.schedulerRecovered.WithLabelValues(m.serviceName, normalizeLabel(job.Queue, "default"), normalizeLabel(job.Status, "unknown")).Inc()
	if job.Status == storage.StatusDeadLetter {
		m.jobsDeadLettered.WithLabelValues(m.serviceName, normalizeLabel(job.Queue, "default"), normalizeLabel(job.Type, "unknown")).Inc()
	}
}

func (m *Metrics) RecordCronJobFired(job storage.Job) {
	if m == nil {
		return
	}
	m.cronJobsFired.WithLabelValues(m.serviceName, normalizeLabel(job.Queue, "default"), normalizeLabel(job.Type, "unknown")).Inc()
}

func (m *Metrics) RecordNATSPublishFailure(component, queue string) {
	if m == nil {
		return
	}
	m.natsPublishFailed.WithLabelValues(m.serviceName, normalizeLabel(component, "unknown"), normalizeLabel(queue, "default")).Inc()
}

func ServeMetrics(ctx context.Context, addr string, handler http.Handler, logger *slog.Logger) error {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && logger != nil {
			logger.Warn("metrics server shutdown failed", "addr", addr, "error", err)
		}
	}()
	go func() {
		if logger != nil {
			logger.Info("metrics server listening", "addr", addr)
		}
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed && logger != nil {
			logger.Error("metrics server failed", "addr", addr, "error", err)
		}
	}()
	return nil
}

func durationBuckets() []float64 {
	return []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}
}

func normalizeLabel(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func normalizeRoute(route string) string {
	if route == "" {
		return "unknown"
	}
	return route
}
