package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

func TestExtractAndInjectTraceContext(t *testing.T) {
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := ExtractTraceContext(context.Background(), storage.TraceContext{TraceParent: traceParent})

	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		t.Fatal("expected valid span context")
	}
	if got := spanContext.TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id = %s", got)
	}

	injected := InjectTraceContext(ctx)
	if injected.TraceParent != traceParent {
		t.Fatalf("traceparent = %q, want %q", injected.TraceParent, traceParent)
	}
}

func TestNormalizeOTLPEndpoint(t *testing.T) {
	cases := map[string]string{
		"otel-collector:4317":                 "otel-collector:4317",
		"http://otel-collector:4317":          "otel-collector:4317",
		"https://collector.example/v1/traces": "collector.example",
	}
	for input, want := range cases {
		if got := normalizeOTLPEndpoint(input); got != want {
			t.Fatalf("normalizeOTLPEndpoint(%q) = %q, want %q", input, got, want)
		}
	}
}
