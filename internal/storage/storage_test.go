package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRetryPolicyDelayForAttempt(t *testing.T) {
	policy := RetryPolicy{InitialDelay: 2 * time.Second, MaxDelay: 5 * time.Second}
	cases := []struct {
		attempt int32
		want    time.Duration
	}{
		{attempt: 0, want: 2 * time.Second},
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 3, want: 5 * time.Second},
		{attempt: 10, want: 5 * time.Second},
	}
	for _, tc := range cases {
		if got := policy.DelayForAttempt(tc.attempt); got != tc.want {
			t.Fatalf("DelayForAttempt(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestClaimCreatesAttempt(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	job := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{"message":"hello"}`),
		MaxAttempts: 3,
	})

	claimed, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected claim")
	}
	if claimed.Job.ID != job.ID {
		t.Fatalf("claimed job %s, want %s", claimed.Job.ID, job.ID)
	}
	if claimed.Job.Status != StatusRunning {
		t.Fatalf("claimed status = %s, want %s", claimed.Job.Status, StatusRunning)
	}
	if claimed.Attempt.AttemptNumber != 1 {
		t.Fatalf("attempt number = %d, want 1", claimed.Attempt.AttemptNumber)
	}
	if claimed.Attempt.LeaseToken != *claimed.Job.LeaseToken {
		t.Fatal("attempt lease token does not match job lease token")
	}

	attempts, err := store.ListJobAttempts(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempt count = %d, want 1", len(attempts))
	}
	if attempts[0].Status != AttemptStatusRunning {
		t.Fatalf("attempt status = %s, want %s", attempts[0].Status, AttemptStatusRunning)
	}
}

func TestFailedJobSchedulesAndClaimsDueRetry(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	job := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.fail",
		Payload:     json.RawMessage(`{"message":"fail"}`),
		MaxAttempts: 3,
	})
	claimed, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected first claim")
	}

	updated, err := store.FailJob(ctx, claimed.Job.ID, *claimed.Job.LeaseToken, "first failure", RetryPolicy{
		InitialDelay: time.Hour,
		MaxDelay:     time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != StatusRetryScheduled {
		t.Fatalf("status = %s, want %s", updated.Status, StatusRetryScheduled)
	}
	if updated.LastError == nil || *updated.LastError != "first failure" {
		t.Fatalf("last_error = %v, want first failure", updated.LastError)
	}

	if _, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("retry should not be claimable before available_at")
	}

	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET available_at = now() WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	retry, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected due retry claim")
	}
	if retry.Attempt.AttemptNumber != 2 {
		t.Fatalf("retry attempt = %d, want 2", retry.Attempt.AttemptNumber)
	}
}

func TestFailedJobMovesToDeadLetterWhenAttemptsExhausted(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	job := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.fail",
		Payload:     json.RawMessage(`{"message":"fail"}`),
		MaxAttempts: 1,
	})
	claimed, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected claim")
	}

	updated, err := store.FailJob(ctx, claimed.Job.ID, *claimed.Job.LeaseToken, "exhausted", RetryPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != StatusDeadLetter {
		t.Fatalf("status = %s, want %s", updated.Status, StatusDeadLetter)
	}
	if updated.DeadLetteredAt == nil {
		t.Fatal("expected dead_lettered_at")
	}

	attempts, err := store.ListJobAttempts(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempt count = %d, want 1", len(attempts))
	}
	if attempts[0].Status != AttemptStatusFailed {
		t.Fatalf("attempt status = %s, want %s", attempts[0].Status, AttemptStatusFailed)
	}
}

func TestLeaseFencingRejectsStaleCompletionAndFailure(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{"message":"hello"}`),
		MaxAttempts: 1,
	})
	claimed, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected claim")
	}

	staleToken := uuid.New()
	if err := store.CompleteJob(ctx, claimed.Job.ID, staleToken); err == nil {
		t.Fatal("expected stale completion rejection")
	}
	if _, err := store.FailJob(ctx, claimed.Job.ID, staleToken, "stale", RetryPolicy{}); err == nil {
		t.Fatal("expected stale failure rejection")
	}
	if err := store.CompleteJob(ctx, claimed.Job.ID, *claimed.Job.LeaseToken); err != nil {
		t.Fatalf("valid completion failed: %v", err)
	}
}

func TestConcurrentIdempotencyKeySubmissionCreatesOneJob(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	const workers = 8
	const key = "same-idempotency-key"
	results := make(chan uuid.UUID, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, _, err := store.CreateJob(ctx, CreateJobParams{
				Queue:          "default",
				Type:           "demo.echo",
				Payload:        json.RawMessage(`{"message":"once"}`),
				MaxAttempts:    1,
				IdempotencyKey: stringPtr(key),
			})
			if err != nil {
				errs <- err
				return
			}
			results <- job.ID
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
	var first uuid.UUID
	for id := range results {
		if first == uuid.Nil {
			first = id
			continue
		}
		if id != first {
			t.Fatalf("idempotent submissions returned different ids: %s and %s", first, id)
		}
	}
	var count int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key = $1`, key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("job count = %d, want 1", count)
	}
}

func openIntegrationStore(t *testing.T) *Store {
	t.Helper()
	databaseURL := os.Getenv("PULSEQUEUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set PULSEQUEUE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	store, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	migrationsDir := filepath.Clean(filepath.Join("..", "..", "migrations"))
	if err := store.ApplyMigrations(ctx, migrationsDir); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `TRUNCATE TABLE job_attempts, jobs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	return store
}

func createTestJob(t *testing.T, store *Store, params CreateJobParams) Job {
	t.Helper()
	job, existing, err := store.CreateJob(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	if existing {
		t.Fatal("test setup unexpectedly returned existing job")
	}
	return job
}

func stringPtr(value string) *string {
	return &value
}
