package main

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"log"
	"time"
)

func SetUpTelemetry(resource *resource.Resource) (*metric.MeterProvider, *sdktrace.TracerProvider) {

	meterProvider, err := NewMeterProvider(resource)
	if err != nil {
		log.Fatalf("failed to create meter provider: %v", err)
	}

	tp := NewTraceExporter(resource)

	return meterProvider, tp
}

func NewResource(serviceName, serviceVersion, actorSystemId string) (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
			attribute.String("actorSystemId", actorSystemId),
		))
}

func NewMeterProvider(res *resource.Resource) (*metric.MeterProvider, error) {
	metricExporter, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	grpc, err := otlpmetricgrpc.New(context.Background(),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint("otel-collector:8889"),
	)

	if err != nil {
		log.Fatalf("failed to create otlp metric exporter: %v", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(
			metric.NewPeriodicReader(grpc, metric.WithInterval(30*time.Second)),
		),
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(
			30*time.Second))),
	)
	return meterProvider, nil
}

func NewTraceExporter(res *resource.Resource) *sdktrace.TracerProvider {
	traceExporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("otel-collector:4317"),
	)
	if err != nil {
		log.Fatalf("failed to create trace exporter: %v", err)
	}

	traceConsoleExporter, err := stdouttrace.New()

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithBatcher(traceConsoleExporter),
		sdktrace.WithResource(res),
	)
}
