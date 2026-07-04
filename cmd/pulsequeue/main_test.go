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
