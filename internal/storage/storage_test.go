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

func TestPriorityOrderingAmongDueJobs(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	low := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{"message":"low"}`),
		Priority:    1,
		MaxAttempts: 1,
	})
	high := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{"message":"high"}`),
		Priority:    10,
		MaxAttempts: 1,
	})

	claimed, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected claim")
	}
	if claimed.Job.ID != high.ID {
		t.Fatalf("claimed job %s, want high priority job %s; low was %s", claimed.Job.ID, high.ID, low.ID)
	}
}

func TestDelayedQueuedJobIsNotClaimableBeforeAvailableAt(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	job := createTestJob(t, store, CreateJobParams{
		Queue:        "default",
		Type:         "demo.echo",
		Payload:      json.RawMessage(`{"message":"later"}`),
		MaxAttempts:  1,
		DelaySeconds: 3600,
	})

	if _, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("delayed queued job should not be claimable before available_at")
	}

	queues, err := store.ListDueQueues(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 0 {
		t.Fatalf("due queues = %v, want none", queues)
	}

	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET available_at = now() WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	queues, err = store.ListDueQueues(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 || queues[0] != "default" {
		t.Fatalf("due queues = %v, want [default]", queues)
	}
	claimed, ok, err := store.ClaimJob(ctx, "default", "worker-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected delayed job to become claimable")
	}
	if claimed.Job.ID != job.ID {
		t.Fatalf("claimed job %s, want %s", claimed.Job.ID, job.ID)
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

func TestRecoverExpiredJobIsIdempotentAndRejectsStaleCompletion(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	job := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.sleep",
		Payload:     json.RawMessage(`{"duration_ms":1000}`),
		MaxAttempts: 2,
	})
	claimed, ok, err := store.ClaimJob(ctx, "default", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected claim")
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}

	recovered, err := store.RecoverExpiredJobs(ctx, 10, "lease expired in test")
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 {
		t.Fatalf("recovered count = %d, want 1", len(recovered))
	}
	if recovered[0].Status != StatusRetryScheduled {
		t.Fatalf("recovered status = %s, want %s", recovered[0].Status, StatusRetryScheduled)
	}

	recoveredAgain, err := store.RecoverExpiredJobs(ctx, 10, "lease expired in test")
	if err != nil {
		t.Fatal(err)
	}
	if len(recoveredAgain) != 0 {
		t.Fatalf("second recovery count = %d, want 0", len(recoveredAgain))
	}
	if err := store.CompleteJob(ctx, claimed.Job.ID, *claimed.Job.LeaseToken); err == nil {
		t.Fatal("expected stale completion rejection after recovery")
	}
	if _, err := store.FailJob(ctx, claimed.Job.ID, *claimed.Job.LeaseToken, "stale", RetryPolicy{}); err == nil {
		t.Fatal("expected stale failure rejection after recovery")
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
	if attempts[0].ErrorMessage == nil || *attempts[0].ErrorMessage != "lease expired in test" {
		t.Fatalf("attempt error = %v, want lease expired in test", attempts[0].ErrorMessage)
	}

	retry, ok, err := store.ClaimJob(ctx, "default", "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected recovered job to be claimable")
	}
	if retry.Attempt.AttemptNumber != 2 {
		t.Fatalf("retry attempt = %d, want 2", retry.Attempt.AttemptNumber)
	}
}

func TestConcurrentRecoverExpiredJobsRecoversOnce(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	job := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.sleep",
		Payload:     json.RawMessage(`{"duration_ms":1000}`),
		MaxAttempts: 2,
	})
	if _, ok, err := store.ClaimJob(ctx, "default", "worker-a", time.Minute); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected claim")
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}

	const schedulers = 2
	counts := make(chan int, schedulers)
	errs := make(chan error, schedulers)
	var wg sync.WaitGroup
	for i := 0; i < schedulers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recovered, err := store.RecoverExpiredJobs(ctx, 10, "lease expired concurrently")
			if err != nil {
				errs <- err
				return
			}
			counts <- len(recovered)
		}()
	}
	wg.Wait()
	close(counts)
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
	total := 0
	for count := range counts {
		total += count
	}
	if total != 1 {
		t.Fatalf("total recovered = %d, want 1", total)
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

func TestWorkerRegistrationHeartbeatAndStatuses(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	worker, err := store.RegisterWorker(ctx, RegisterWorkerParams{
		ID:          "worker-a",
		Hostname:    "host-a",
		Queues:      []string{"default", "emails", "default"},
		Concurrency: 3,
		Metadata:    json.RawMessage(`{"test":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status != WorkerStatusRunning {
		t.Fatalf("status = %s, want %s", worker.Status, WorkerStatusRunning)
	}
	if worker.Concurrency != 3 {
		t.Fatalf("concurrency = %d, want 3", worker.Concurrency)
	}
	if len(worker.Queues) != 2 || worker.Queues[0] != "default" || worker.Queues[1] != "emails" {
		t.Fatalf("queues = %v, want [default emails]", worker.Queues)
	}

	if err := store.HeartbeatWorker(ctx, "worker-a", WorkerStatusRunning, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkWorkerStatus(ctx, "worker-a", WorkerStatusDraining); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkWorkerStatus(ctx, "worker-a", WorkerStatusStopped); err != nil {
		t.Fatal(err)
	}
	workers, err := store.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 {
		t.Fatalf("worker count = %d, want 1", len(workers))
	}
	if workers[0].Status != WorkerStatusStopped {
		t.Fatalf("worker status = %s, want %s", workers[0].Status, WorkerStatusStopped)
	}
}

func TestPhase4JobLogsRetryCancelAndQueues(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	job := createTestJob(t, store, CreateJobParams{
		Queue:        "default",
		Type:         "demo.echo",
		Payload:      json.RawMessage(`{"message":"cancel me"}`),
		MaxAttempts:  1,
		DelaySeconds: 60,
	})

	cancelled, err := store.CancelJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != StatusCancelled {
		t.Fatalf("cancelled status = %s, want %s", cancelled.Status, StatusCancelled)
	}

	retried, err := store.RetryJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != StatusQueued {
		t.Fatalf("retried status = %s, want %s", retried.Status, StatusQueued)
	}
	if retried.MaxAttempts < retried.AttemptCount+1 {
		t.Fatalf("max_attempts = %d, attempt_count = %d", retried.MaxAttempts, retried.AttemptCount)
	}

	logs, err := store.ListJobLogs(ctx, job.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 {
		t.Fatalf("log count = %d, want 2", len(logs))
	}
	if logs[0].Message != "job cancelled" || logs[1].Message != "job manually retried" {
		t.Fatalf("unexpected log messages: %#v", logs)
	}

	queues, err := store.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 {
		t.Fatalf("queue count = %d, want 1", len(queues))
	}
	if queues[0].Queue != "default" || queues[0].Queued != 1 || queues[0].TotalJobs != 1 {
		t.Fatalf("unexpected queue summary: %#v", queues[0])
	}
}

func TestPhase4CronFireCreatesOneJobAndLog(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	cronJob, err := store.CreateCronJob(ctx, CreateCronJobParams{
		Name:        "minute-report",
		Queue:       "reports",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{"message":"from cron"}`),
		Schedule:    "* * * * *",
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE cron_jobs SET next_run_at = date_trunc('minute', now()) - interval '1 minute' WHERE id = $1`, cronJob.ID); err != nil {
		t.Fatal(err)
	}

	fires, err := store.FireDueCronJobs(ctx, "scheduler-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fires) != 1 {
		t.Fatalf("fire count = %d, want 1", len(fires))
	}
	if fires[0].Job.Queue != "reports" || fires[0].Job.Type != "demo.echo" {
		t.Fatalf("unexpected fired job: %#v", fires[0].Job)
	}

	logs, err := store.ListJobLogs(ctx, fires[0].Job.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].Message != "cron job fired" {
		t.Fatalf("unexpected logs: %#v", logs)
	}

	firesAgain, err := store.FireDueCronJobs(ctx, "scheduler-b", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(firesAgain) != 0 {
		t.Fatalf("second fire count = %d, want 0", len(firesAgain))
	}
}

func TestPhase4ConcurrentCronFireCreatesOneRun(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	cronJob, err := store.CreateCronJob(ctx, CreateCronJobParams{
		Name:        "concurrent-cron",
		Queue:       "default",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{}`),
		Schedule:    "* * * * *",
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE cron_jobs SET next_run_at = date_trunc('minute', now()) - interval '1 minute' WHERE id = $1`, cronJob.ID); err != nil {
		t.Fatal(err)
	}

	const schedulers = 2
	counts := make(chan int, schedulers)
	errs := make(chan error, schedulers)
	for i := 0; i < schedulers; i++ {
		go func(i int) {
			fires, err := store.FireDueCronJobs(ctx, "scheduler-concurrent", 10)
			if err != nil {
				errs <- err
				return
			}
			counts <- len(fires)
		}(i)
	}

	total := 0
	for i := 0; i < schedulers; i++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		case count := <-counts:
			total += count
		}
	}
	if total != 1 {
		t.Fatalf("total fires = %d, want 1", total)
	}

	var runCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM cron_runs WHERE cron_job_id = $1`, cronJob.ID).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("cron run count = %d, want 1", runCount)
	}
}

func TestCreateJobPersistsTraceContext(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	const traceState = "vendor=value"
	job := createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{"message":"traced"}`),
		MaxAttempts: 1,
		TraceParent: traceParent,
		TraceState:  traceState,
	})

	stored, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.TraceParent == nil || *stored.TraceParent != traceParent {
		t.Fatalf("traceparent = %v, want %q", stored.TraceParent, traceParent)
	}
	if stored.TraceState == nil || *stored.TraceState != traceState {
		t.Fatalf("tracestate = %v, want %q", stored.TraceState, traceState)
	}
}

func TestObservabilitySnapshotCountsQueues(t *testing.T) {
	store := openIntegrationStore(t)
	ctx := context.Background()

	createTestJob(t, store, CreateJobParams{
		Queue:       "default",
		Type:        "demo.echo",
		Payload:     json.RawMessage(`{"message":"queued"}`),
		MaxAttempts: 1,
	})
	running := createTestJob(t, store, CreateJobParams{
		Queue:       "critical",
		Type:        "demo.sleep",
		Payload:     json.RawMessage(`{"duration_ms":1000}`),
		MaxAttempts: 1,
	})
	if _, ok, err := store.ClaimJob(ctx, "critical", "worker-critical", time.Minute); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected critical job claim")
	}
	if _, err := store.RegisterWorker(ctx, RegisterWorkerParams{
		ID:          "worker-default",
		Hostname:    "host",
		Queues:      []string{"default"},
		Concurrency: 1,
		Metadata:    json.RawMessage(`{}`),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.ObservabilitySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := findQueueMetric(snapshot.QueueDepth, "default"); got != 1 {
		t.Fatalf("default queue depth = %d, want 1", got)
	}
	if got := findQueueMetric(snapshot.ActiveJobs, "critical"); got != 1 {
		t.Fatalf("critical active jobs = %d, want 1 for %s", got, running.ID)
	}
	if got := findQueueMetric(snapshot.ActiveWorkers, "default"); got != 1 {
		t.Fatalf("default active workers = %d, want 1", got)
	}
	if got := findStatusMetric(snapshot.JobsByStatus, "critical", StatusRunning); got != 1 {
		t.Fatalf("critical running jobs = %d, want 1", got)
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
	if _, err := store.pool.Exec(ctx, `TRUNCATE TABLE cron_runs, job_logs, job_attempts, cron_jobs, jobs, workers RESTART IDENTITY CASCADE`); err != nil {
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

func findQueueMetric(metrics []QueueMetric, queue string) int64 {
	for _, metric := range metrics {
		if metric.Queue == queue {
			return metric.Value
		}
	}
	return 0
}

func findStatusMetric(metrics []JobStatusMetric, queue, status string) int64 {
	for _, metric := range metrics {
		if metric.Queue == queue && metric.Status == status {
			return metric.Count
		}
	}
	return 0
}
