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

	"github.com/fullstack-nick/PulseQueue/internal/signals"
	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

type Server struct {
	store         *storage.Store
	signals       *signals.Client
	operatorToken string
	logger        *slog.Logger
}

type CreateJobRequest struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int32           `json:"priority"`
	MaxAttempts    int32           `json:"max_attempts"`
	TimeoutSeconds int32           `json:"timeout_seconds"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type CreateJobResponse struct {
	Job      storage.Job `json:"job"`
	Existing bool        `json:"existing"`
}

func NewServer(store *storage.Store, signals *signals.Client, operatorToken string, logger *slog.Logger) http.Handler {
	s := &Server{store: store, signals: signals, operatorToken: operatorToken, logger: logger}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(s.loggingMiddleware)

	r.Get("/health/live", s.handleLive)
	r.Get("/health/ready", s.handleReady)

	r.Group(func(r chi.Router) {
		r.Use(s.requireAuth)
		r.Post("/jobs", s.handleCreateJob)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
		r.Get("/jobs/{id}/attempts", s.handleListJobAttempts)
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
	job, existing, err := s.store.CreateJob(r.Context(), storage.CreateJobParams{
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		TimeoutSeconds: req.TimeoutSeconds,
		IdempotencyKey: key,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !existing {
		if err := s.signals.PublishJobAvailable(job.Queue); err != nil {
			s.logger.Warn("job persisted but nats publish failed", "job_id", job.ID, "error", err)
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
		next.ServeHTTP(w, r)
		s.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
