package tracing

import (
	"encoding/hex"
	"fmt"
	"github.com/asynkron/protoactor-go/actor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var ErrSpanContextNotFound = fmt.Errorf("spanContext not found")

func spanContextFromMessageHeader(header actor.ReadonlyMessageHeader) (trace.SpanContext, error) {
	if header == nil {
		return trace.SpanContext{}, ErrSpanContextNotFound
	}

	gotSpanId, err := trace.SpanIDFromHex(header.Get("parent-id"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse spanId: %v: %w", err, ErrSpanContextNotFound)
	}

	gotTraceId, err := trace.TraceIDFromHex(header.Get("trace-id"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse traceId: %v: %w", err, ErrSpanContextNotFound)
	}

	tracestate, err := trace.ParseTraceState(header.Get("tracestate"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse tracestate: %v: %w", err, ErrSpanContextNotFound)
	}

	traceFlags, err := hex.DecodeString(header.Get("trace-flags"))
	if err != nil {
		return trace.SpanContext{}, fmt.Errorf("failed to parse traceFlags: %v: %w", err, ErrSpanContextNotFound)
	}

	return trace.NewSpanContext(
		trace.SpanContextConfig{
			SpanID:     gotSpanId,
			TraceID:    gotTraceId,
			TraceState: tracestate,
			TraceFlags: trace.TraceFlags(traceFlags[0]),
			Remote:     true,
		}), nil
}

func setSenderSpanAttributes(sendTargetActorPID *actor.PID, message interface{}, span trace.Span, t *ActorContext) {
	span.SetAttributes(attribute.String("SenderActorPID", t.Self().String()))
	span.SetAttributes(attribute.String("SenderActorType", fmt.Sprintf("%T", t.Actor())))
	span.SetAttributes(attribute.String("TargetActorPID", sendTargetActorPID.String()))
	span.SetAttributes(attribute.String("MessageType", fmt.Sprintf("%T", message)))
}

func wrapEnvelopeWithSpanContext(spanCtx trace.SpanContext, message any, sender *actor.PID) *actor.MessageEnvelope {
	envelope := &actor.MessageEnvelope{
		Message: message,
		Sender:  sender,
	}
	setSpanContextToEnvelope(spanCtx, envelope)
	return envelope
}

func setSpanContextToEnvelope(spanCtx trace.SpanContext, envelope *actor.MessageEnvelope) {
	envelope.SetHeader("parent-id", spanCtx.SpanID().String())
	envelope.SetHeader("trace-id", spanCtx.TraceID().String())
	envelope.SetHeader("tracestate", spanCtx.TraceState().String())
	envelope.SetHeader("trace-flags", fmt.Sprintf("%02x", byte(spanCtx.TraceFlags())))
}

// TraceableRootContext creates a RootContext with tracing capabilities
func TraceableRootContext(rootContext actor.RootContext, spanContext trace.SpanContext) *actor.RootContext {
	return rootContext.Copy().WithSenderMiddleware(RootContextSenderMiddleware()).WithSpawnMiddleware(RootContextSpawnMiddleware()).WithHeaders(MapFromSpanContext(spanContext))
}
