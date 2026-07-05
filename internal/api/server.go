package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fullstack-nick/PulseQueue/internal/observability"
	"github.com/fullstack-nick/PulseQueue/internal/signals"
	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

type Server struct {
	store         *storage.Store
	signals       *signals.Client
	operatorToken string
	logger        *slog.Logger
	metrics       *observability.Metrics
	tracer        trace.Tracer
}

type CreateJobRequest struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int32           `json:"priority"`
	MaxAttempts    int32           `json:"max_attempts"`
	TimeoutSeconds int32           `json:"timeout_seconds"`
	DelaySeconds   int32           `json:"delay_seconds"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type CreateJobResponse struct {
	Job      storage.Job `json:"job"`
	Existing bool        `json:"existing"`
}

type CreateCronJobRequest struct {
	Name           string          `json:"name"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Schedule       string          `json:"schedule"`
	Priority       int32           `json:"priority"`
	MaxAttempts    int32           `json:"max_attempts"`
	TimeoutSeconds int32           `json:"timeout_seconds"`
}

func NewServer(store *storage.Store, signals *signals.Client, operatorToken string, logger *slog.Logger, metrics *observability.Metrics) http.Handler {
	if metrics == nil {
		metrics = observability.NewMetrics("pulsequeue-api")
	}
	s := &Server{
		store:         store,
		signals:       signals,
		operatorToken: operatorToken,
		logger:        logger,
		metrics:       metrics,
		tracer:        observability.Tracer("github.com/fullstack-nick/PulseQueue/internal/api"),
	}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(s.observabilityMiddleware)
	r.Use(s.loggingMiddleware)

	r.Get("/health/live", s.handleLive)
	r.Get("/health/ready", s.handleReady)
	r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.metrics.Handler().ServeHTTP(w, r)
	})

	r.Group(func(r chi.Router) {
		r.Use(s.requireAuth)
		r.Post("/jobs", s.handleCreateJob)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
		r.Get("/jobs/{id}/attempts", s.handleListJobAttempts)
		r.Get("/jobs/{id}/logs", s.handleListJobLogs)
		r.Post("/jobs/{id}/retry", s.handleRetryJob)
		r.Post("/jobs/{id}/cancel", s.handleCancelJob)
		r.Get("/workers", s.handleListWorkers)
		r.Get("/queues", s.handleListQueues)
		r.Post("/cron", s.handleCreateCronJob)
		r.Get("/cron", s.handleListCronJobs)
		r.Post("/cron/{ref}/enable", s.handleEnableCronJob)
		r.Post("/cron/{ref}/disable", s.handleDisableCronJob)
	})

	return r
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not ready")
		return
	}
	if err := s.signals.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "nats not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "api.create_job")
	defer span.End()

	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Queue == "" {
		req.Queue = "default"
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = []byte(`{}`)
	}
	var key *string
	if strings.TrimSpace(req.IdempotencyKey) != "" {
		trimmed := strings.TrimSpace(req.IdempotencyKey)
		key = &trimmed
	}
	traceContext := observability.InjectTraceContext(ctx)
	job, existing, err := s.store.CreateJob(ctx, storage.CreateJobParams{
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		TimeoutSeconds: req.TimeoutSeconds,
		DelaySeconds:   req.DelaySeconds,
		IdempotencyKey: key,
		TraceParent:    traceContext.TraceParent,
		TraceState:     traceContext.TraceState,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	span.SetAttributes(
		attribute.String("job.id", job.ID.String()),
		attribute.String("job.queue", job.Queue),
		attribute.String("job.type", job.Type),
		attribute.Bool("job.existing", existing),
	)
	s.metrics.RecordJobSubmitted(job.Queue, job.Type, existing)
	if !existing && req.DelaySeconds == 0 {
		if err := s.signals.PublishJobAvailable(job.Queue); err != nil {
			s.metrics.RecordNATSPublishFailure("api", job.Queue)
			s.logger.Warn("job persisted but nats publish failed", "job_id", job.ID, "error", err)
		}
	}
	if !existing {
		fields := map[string]any{
			"request_id": middleware.GetReqID(ctx),
			"queue":      job.Queue,
			"job_type":   job.Type,
			"source":     "api",
		}
		addTraceFields(ctx, fields)
		if _, err := s.store.AppendJobLog(ctx, storage.AppendJobLogParams{
			JobID:   job.ID,
			Level:   "info",
			Message: "job submitted",
			Fields:  logFields(fields),
		}); err != nil {
			s.logger.Warn("job submitted but log append failed", "job_id", job.ID, "error", err)
		}
	}
	writeJSON(w, http.StatusCreated, CreateJobResponse{Job: job, Existing: existing})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	limit := int32(50)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = int32(parsed)
	}
	jobs, err := s.store.ListJobs(r.Context(), storage.ListJobsFilter{
		Status: r.URL.Query().Get("status"),
		Queue:  r.URL.Query().Get("queue"),
		Limit:  limit,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		if storage.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) handleListJobAttempts(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	if _, err := s.store.GetJob(r.Context(), id); err != nil {
		if storage.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	attempts, err := s.store.ListJobAttempts(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list job attempts")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"attempts": attempts})
}

func (s *Server) handleListJobLogs(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	limit := int32(100)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = int32(parsed)
	}
	if _, err := s.store.GetJob(r.Context(), id); err != nil {
		if storage.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	logs, err := s.store.ListJobLogs(r.Context(), id, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list job logs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"logs": logs})
}

func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "api.retry_job")
	defer span.End()

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.store.RetryJob(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		writeStorageError(w, err, "failed to retry job")
		return
	}
	span.SetAttributes(
		attribute.String("job.id", job.ID.String()),
		attribute.String("job.queue", job.Queue),
		attribute.String("job.type", job.Type),
	)
	s.metrics.RecordJobRetried(job)
	if err := s.signals.PublishJobAvailable(job.Queue); err != nil {
		s.metrics.RecordNATSPublishFailure("api", job.Queue)
		s.logger.Warn("job retried but nats publish failed", "job_id", job.ID, "queue", job.Queue, "error", err)
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.store.CancelJob(r.Context(), id)
	if err != nil {
		writeStorageError(w, err, "failed to cancel job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.store.ListWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list workers")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := s.store.ListQueues(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list queues")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": queues})
}

func (s *Server) handleCreateCronJob(w http.ResponseWriter, r *http.Request) {
	var req CreateCronJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = []byte(`{}`)
	}
	cronJob, err := s.store.CreateCronJob(r.Context(), storage.CreateCronJobParams{
		Name:           req.Name,
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        req.Payload,
		Schedule:       req.Schedule,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		TimeoutSeconds: req.TimeoutSeconds,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"cron_job": cronJob})
}

func (s *Server) handleListCronJobs(w http.ResponseWriter, r *http.Request) {
	cronJobs, err := s.store.ListCronJobs(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list cron jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cron_jobs": cronJobs})
}

func (s *Server) handleEnableCronJob(w http.ResponseWriter, r *http.Request) {
	s.handleSetCronJobEnabled(w, r, true)
}

func (s *Server) handleDisableCronJob(w http.ResponseWriter, r *http.Request) {
	s.handleSetCronJobEnabled(w, r, false)
}

func (s *Server) handleSetCronJobEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	cronJob, err := s.store.SetCronJobEnabled(r.Context(), chi.URLParam(r, "ref"), enabled)
	if err != nil {
		writeStorageError(w, err, "failed to update cron job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cron_job": cronJob})
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := "Bearer " + s.operatorToken
		if s.operatorToken == "" || r.Header.Get("Authorization") != expected {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		fields := []any{
			"request_id", middleware.GetReqID(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		}
		fields = append(fields, observability.TraceLogFields(r.Context())...)
		s.logger.Info("http request", fields...)
	})
}

func (s *Server) observabilityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := s.tracer.Start(r.Context(), "http "+r.Method)
		defer span.End()

		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		req := r.WithContext(ctx)
		next.ServeHTTP(ww, req)

		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		route := routePattern(req)
		duration := time.Since(start)
		s.metrics.RecordHTTPRequest(r.Method, route, status, duration)
		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.route", route),
			attribute.Int("http.status_code", status),
		)
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	})
}

func routePattern(r *http.Request) string {
	if routeContext := chi.RouteContext(r.Context()); routeContext != nil {
		if pattern := routeContext.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return r.URL.Path
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeStorageError(w http.ResponseWriter, err error, fallback string) {
	switch {
	case storage.IsNotFound(err):
		writeError(w, http.StatusNotFound, strings.TrimPrefix(err.Error(), "ERROR: "))
	case storage.IsInvalidState(err):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, fallback)
	}
}

func logFields(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func addTraceFields(ctx context.Context, fields map[string]any) {
	traceFields := observability.TraceLogFields(ctx)
	for i := 0; i+1 < len(traceFields); i += 2 {
		key, ok := traceFields[i].(string)
		if !ok {
			continue
		}
		fields[key] = traceFields[i+1]
	}
}
