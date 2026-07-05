package cronexpr

import (
	"testing"
	"time"
)

func TestNextEveryFiveMinutes(t *testing.T) {
	next, err := Next("*/5 * * * *", time.Date(2026, 7, 5, 12, 2, 30, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 5, 12, 5, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextUsesUTC(t *testing.T) {
	location := time.FixedZone("offset", 2*60*60)
	next, err := Next("0 8 * * *", time.Date(2026, 7, 5, 9, 30, 0, 0, location))
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 5, 8, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestParseRejectsInvalidFieldCount(t *testing.T) {
	if _, err := Parse("* * * *"); err == nil {
		t.Fatal("expected invalid field count error")
	}
}
