package tracing

import (
	context2 "context"
	"errors"
	"fmt"
	"github.com/asynkron/protoactor-go/actor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

func MapFromSpanContext(spanCtx trace.SpanContext) map[string]string {
	return map[string]string{
		"parent-id":   spanCtx.SpanID().String(),
		"trace-id":    spanCtx.TraceID().String(),
		"tracestate":  spanCtx.TraceState().String(),
		"trace-flags": fmt.Sprintf("%02x", byte(spanCtx.TraceFlags())),
	}
}

func extractSpanContextFromSenderFuncArgs(c actor.SenderContext, envelope *actor.MessageEnvelope) (trace.SpanContext, error) {
	fromCtxMessageHeader, err := spanContextFromMessageHeader(c.MessageHeader())
	if !errors.Is(err, ErrSpanContextNotFound) {
		return fromCtxMessageHeader, nil
	}
	return spanContextFromMessageHeader(envelope.Header)
}

func RootContextSenderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			if _, ok := c.(*actor.RootContext); !ok {
				c.Logger().Debug("Context is not a receiver context", slog.Any("self", c.Self()))
				next(c, target, envelope)
				return
			}

			ctxWithParentSpan := context2.Background()
			spanContext, err := extractSpanContextFromSenderFuncArgs(c, envelope)
			if errors.Is(err, ErrSpanContextNotFound) {
				c.Logger().Debug("No spanContext found", slog.Any("self", c.Self()), slog.Any("error", err))
			} else if err != nil {
				c.Logger().Error("Error extracting spanContext", slog.Any("self", c.Self()), slog.Any("error", err))
			} else {
				ctxWithParentSpan = trace.ContextWithSpanContext(ctxWithParentSpan, spanContext)
			}

			traceExt := c.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
			ctxWithCurrentSpan, span := traceExt.Tracer().Start(ctxWithParentSpan, fmt.Sprintf("message_send/%T", envelope.Message))
			defer span.End()
			span.SetAttributes(attribute.String("SenderActorPID", c.Self().String()))
			span.SetAttributes(attribute.String("SenderActorType", fmt.Sprintf("%T", c.Actor())))
			span.SetAttributes(attribute.String("TargetActorPID", target.String()))
			span.SetAttributes(attribute.String("MessageType", fmt.Sprintf("%T", envelope.Message)))
			setSpanContextToEnvelope(trace.SpanContextFromContext(
				ctxWithCurrentSpan,
			), envelope)

			next(c, target, envelope)
		}
	}
}
