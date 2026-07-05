package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadPayloadInlineJSON(t *testing.T) {
	payload, err := readPayload(`{"message":"hello"}`)
	if err != nil {
		t.Fatalf("readPayload returned error: %v", err)
	}
	if string(payload) != `{"message":"hello"}` {
		t.Fatalf("unexpected payload: %s", payload)
	}
}

func TestReadPayloadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload.json")
	if err := os.WriteFile(path, []byte(`{"message":"from-file"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, err := readPayload(path)
	if err != nil {
		t.Fatalf("readPayload returned error: %v", err)
	}
	if string(payload) != `{"message":"from-file"}` {
		t.Fatalf("unexpected payload: %s", payload)
	}
}

func TestReadPayloadRejectsInvalidJSON(t *testing.T) {
	if _, err := readPayload(`{"message":`); err == nil {
		t.Fatal("expected invalid JSON error")
	}
}

func TestJobsSubmitIncludesTimeoutFlag(t *testing.T) {
	cmd := newJobsSubmitCommand()
	if cmd.Flags().Lookup("timeout-seconds") == nil {
		t.Fatal("expected timeout-seconds flag")
	}
}

func TestJobsSubmitIncludesDelayFlag(t *testing.T) {
	cmd := newJobsSubmitCommand()
	if cmd.Flags().Lookup("delay-seconds") == nil {
		t.Fatal("expected delay-seconds flag")
	}
}

func TestRootCommandIncludesSchedulerAndWorkers(t *testing.T) {
	cmd := newRootCommand()
	if _, _, err := cmd.Find([]string{"scheduler"}); err != nil {
		t.Fatal("expected scheduler command")
	}
	if _, _, err := cmd.Find([]string{"workers", "list"}); err != nil {
		t.Fatal("expected workers list command")
	}
	if _, _, err := cmd.Find([]string{"queues", "list"}); err != nil {
		t.Fatal("expected queues list command")
	}
	if _, _, err := cmd.Find([]string{"cron", "list"}); err != nil {
		t.Fatal("expected cron list command")
	}
	if _, _, err := cmd.Find([]string{"jobs", "logs", "job-id"}); err != nil {
		t.Fatal("expected jobs logs command")
	}
	if _, _, err := cmd.Find([]string{"jobs", "retry", "job-id"}); err != nil {
		t.Fatal("expected jobs retry command")
	}
	if _, _, err := cmd.Find([]string{"jobs", "cancel", "job-id"}); err != nil {
		t.Fatal("expected jobs cancel command")
	}
}

func TestWorkerCommandAcceptsConcurrencyGreaterThanOne(t *testing.T) {
	cmd := newWorkerCommand()
	if err := cmd.Flags().Set("concurrency", "3"); err != nil {
		t.Fatalf("expected concurrency flag to accept 3: %v", err)
	}
}

func TestCronCreateIncludesPhase4Flags(t *testing.T) {
	cmd := newCronCreateCommand()
	for _, name := range []string{"name", "schedule", "payload", "output"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("expected %s flag", name)
		}
	}
}

func TestJobsLogsIncludesLimitFlag(t *testing.T) {
	cmd := newJobsLogsCommand()
	if cmd.Flags().Lookup("limit") == nil {
		t.Fatal("expected limit flag")
	}
}
