package tracing

import (
	context2 "context"
	"errors"
	"fmt"
	"github.com/asynkron/protoactor-go/actor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"time"
)

// ActorContext is a decorator for actor.Context that adds OpenTelemetry tracing capabilities.
type ActorContext struct {
	actor.Context
	withTracingSpanContext context2.Context
}

var _ actor.Context = (*ActorContext)(nil)

func (ac *ActorContext) Receive(envelope *actor.MessageEnvelope) {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		ac.Context.Receive(envelope)
		return
	}
	ac.withTracingSpanContext = context2.Background()
	defer func() {
		ac.withTracingSpanContext = nil
	}()
	if envelope.Header != nil {
		spanContext, err := spanContextFromMessageHeader(envelope.Header)
		if errors.Is(err, ErrSpanContextNotFound) {
			ac.Logger().Debug("No spanContext found", slog.Any("self", ac.Self()), slog.Any("error", err))
		} else if err != nil {
			ac.Logger().Debug("Error extracting spanContext", slog.Any("self", ac.Self()), slog.Any("error", err))
		} else {
			ac.withTracingSpanContext = trace.ContextWithSpanContext(ac.withTracingSpanContext, spanContext)
		}
	}
	startSpan := func(suffix string) trace.Span {
		ctx, span := traceExt.Tracer().Start(ac.withTracingSpanContext, fmt.Sprintf("message_receive/%T/%s", ac.Actor(), suffix))
		span.SetAttributes(attribute.String("ActorPID", ac.Self().String()))
		span.SetAttributes(attribute.String("ActorType", fmt.Sprintf("%T", ac.Actor())))
		span.SetAttributes(attribute.String("MessageType", fmt.Sprintf("%T", envelope.Message)))
		ac.withTracingSpanContext = ctx
		return span
	}

	switch envelope.Message.(type) {
	case *actor.Started:
		span := startSpan("started")
		defer span.End()
	case *actor.Stopping:
		span := startSpan("stopping")
		defer span.End()
	case *actor.Stopped:
		span := startSpan("stopped")
		defer span.End()
	default:
		span := startSpan(fmt.Sprintf("%T", envelope.Message))
		defer span.End()
	}

	ac.Context.Receive(envelope)
}

func (ac *ActorContext) Send(pid *actor.PID, message interface{}) {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		ac.Context.Send(pid, message)
		return
	}
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, fmt.Sprintf("message_send/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, ac)
	envelop := messageToEnvelop(message, ac, ac.Self())

	ac.Context.Send(pid, envelop)
}

func (ac *ActorContext) Request(pid *actor.PID, message interface{}) {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		ac.Context.Request(pid, message)
		return
	}
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, fmt.Sprintf("message_request/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, ac)
	envelop := messageToEnvelop(message, ac, ac.Self())

	ac.Context.Send(pid, envelop)
}

func (ac *ActorContext) RequestWithCustomSender(pid *actor.PID, message interface{}, sender *actor.PID) {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		ac.Context.RequestWithCustomSender(pid, message, sender)
		return
	}
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, fmt.Sprintf("message_request_with_custom_sender/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, ac)
	span.SetAttributes(attribute.String("CustomSenderActorPID", sender.String()))

	envelop := messageToEnvelop(message, ac, sender)

	ac.Context.Send(pid, envelop)
}

func messageToEnvelop(message interface{}, t *ActorContext, sender *actor.PID) *actor.MessageEnvelope {
	envelop, ok := message.(*actor.MessageEnvelope)
	if ok {
		setSpanContextToEnvelope(trace.SpanContextFromContext(t.withTracingSpanContext), envelop)
	} else {
		envelop = wrapEnvelopeWithSpanContext(trace.SpanContextFromContext(t.withTracingSpanContext), message, sender)
	}
	return envelop
}

func (ac *ActorContext) RequestFuture(pid *actor.PID, message interface{}, timeout time.Duration) *actor.Future {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		return ac.Context.RequestFuture(pid, message, timeout)
	}
	future := actor.NewFuture(ac.ActorSystem(), timeout)
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, fmt.Sprintf("message_request_future/%T", message))
	defer span.End()
	setSenderSpanAttributes(pid, message, span, ac)
	envelop := messageToEnvelop(message, ac, future.PID())

	ac.Context.RequestFuture(pid, envelop, timeout)
	return future

}
func (ac *ActorContext) Respond(response interface{}) {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		ac.Context.Respond(response)
		return
	}
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, fmt.Sprintf("message_respond/%T", response))
	defer span.End()
	setSenderSpanAttributes(ac.Sender(), response, span, ac)
	envelop := messageToEnvelop(response, ac, ac.Self())

	ac.Context.Respond(envelop)
}

func ContextDecorator() func(next actor.ContextDecoratorFunc) actor.ContextDecoratorFunc {
	return func(next actor.ContextDecoratorFunc) actor.ContextDecoratorFunc {
		return func(ctx actor.Context) actor.Context {
			return next(&ActorContext{ctx, nil})
		}
	}
}

func (ac *ActorContext) Spawn(props *actor.Props) *actor.PID {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		return ac.Context.Spawn(props)
	}
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, "spawn")
	defer span.End()
	props.Configure(actor.WithContextDecorator(ContextDecorator()))
	pid := ac.Context.Spawn(props)
	span.SetName(fmt.Sprintf("spawn/%s", pid.Id))
	span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
	return pid
}

func (ac *ActorContext) SpawnPrefix(props *actor.Props, prefix string) *actor.PID {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		return ac.Context.SpawnPrefix(props, prefix)
	}
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, "spawn")
	defer span.End()
	props.Configure(actor.WithContextDecorator(ContextDecorator()))
	pid := ac.Context.SpawnPrefix(props, prefix)
	span.SetAttributes(attribute.String("Prefix", prefix))

	span.SetName(fmt.Sprintf("spawn/%s", pid.Id))
	span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
	return pid
}

func (ac *ActorContext) SpawnNamed(props *actor.Props, id string) (*actor.PID, error) {
	traceExt, ok := ac.ActorSystem().Extensions.Get(extensionID).(*TraceExtension)
	if !ok {
		ac.Logger().Debug("TraceExtension not registered")
		return ac.Context.SpawnNamed(props, id)
	}
	_, span := traceExt.Tracer().Start(ac.withTracingSpanContext, "spawn")
	defer span.End()
	props.Configure(actor.WithContextDecorator(ContextDecorator()))
	span.SetAttributes(attribute.String("ID", id))
	pid, err := ac.Context.SpawnNamed(props, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetName(fmt.Sprintf("spawn/%s", pid.Id))
		span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
	}
	return pid, err
}
