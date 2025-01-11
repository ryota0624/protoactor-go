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
)

func rootContextSpawnMiddleware() actor.SpawnMiddleware {
	return func(next actor.SpawnFunc) actor.SpawnFunc {
		return func(actorSystem *actor.ActorSystem, id string, props *actor.Props, parentContext actor.SpawnerContext) (pid *actor.PID, e error) {
			rootContext, ok := parentContext.(*actor.RootContext)
			if !ok {
				parentContext.Logger().Debug("Context is not rootContext", slog.Any("self", parentContext.Self()))
				pid, err := next(actorSystem, id, props, parentContext)
				return pid, err
			}

			self := parentContext.Self()
			ctxWithParentSpan := context2.Background()

			if rootContext.MessageHeader() != nil {
				spanCtx, err := spanContextFromMessageHeader(rootContext.MessageHeader())
				if errors.Is(err, ErrSpanContextNotFound) {
					rootContext.Logger().Debug("No spanContext found", slog.String("self", "root-context"), slog.Any("error", err))
				} else if err != nil {
					rootContext.Logger().Debug("Error extracting spanContext", slog.String("self", "root-context"), slog.Any("error", err))
				} else {
					ctxWithParentSpan = trace.ContextWithSpanContext(ctxWithParentSpan, spanCtx)
				}
			}
			traceExt := actorSystem.Extensions.Get(extensionID).(*TraceExtension)
			_, span := traceExt.Tracer().Start(ctxWithParentSpan, fmt.Sprintf("spawn/%s", id))
			defer span.End()
			span.SetAttributes(attribute.String("SpawnActorPID", pid.String()))
			pid, err := next(actorSystem, id, props, parentContext)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				actorSystem.Logger().Debug("SPAWN got error trying to spawn", slog.Any("self", self), slog.Any("actor", parentContext.Actor()), slog.Any("error", err))
				return pid, err
			}
			return pid, err
		}
	}
}
