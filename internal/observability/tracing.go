package observability

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

type TracingConfig struct {
	ServiceName string
	Endpoint    string
}

func init() {
	otel.SetTextMapPropagator(defaultPropagator())
}

func InitTracing(ctx context.Context, cfg TracingConfig) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(defaultPropagator())
	if cfg.ServiceName == "" {
		cfg.ServiceName = "pulsequeue"
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(normalizeOTLPEndpoint(cfg.Endpoint)),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", cfg.ServiceName)),
	)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func Tracer(name string) trace.Tracer {
	if name == "" {
		name = "github.com/fullstack-nick/PulseQueue"
	}
	return otel.Tracer(name)
}

func InjectTraceContext(ctx context.Context) storage.TraceContext {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return storage.TraceContext{
		TraceParent: carrier.Get("traceparent"),
		TraceState:  carrier.Get("tracestate"),
	}
}

func ExtractTraceContext(ctx context.Context, traceContext storage.TraceContext) context.Context {
	carrier := propagation.MapCarrier{}
	if traceContext.TraceParent != "" {
		carrier.Set("traceparent", traceContext.TraceParent)
	}
	if traceContext.TraceState != "" {
		carrier.Set("tracestate", traceContext.TraceState)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func TraceContextFromJob(job storage.Job) storage.TraceContext {
	traceContext := storage.TraceContext{}
	if job.TraceParent != nil {
		traceContext.TraceParent = *job.TraceParent
	}
	if job.TraceState != nil {
		traceContext.TraceState = *job.TraceState
	}
	return traceContext
}

func TraceLogFields(ctx context.Context) []any {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return nil
	}
	return []any{
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	}
}

func defaultPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

func normalizeOTLPEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimSuffix(endpoint, "/v1/traces")
	return strings.TrimRight(endpoint, "/")
}
