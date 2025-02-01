package tracing

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/actor/middleware/propagator"
)

func RootContextSpawnMiddleware() actor.SpawnMiddleware {
	return propagator.New().
		WithItselfForwarded().
		WithSpawnMiddleware(rootContextSpawnMiddleware()).
		WithContextDecorator(ContextDecorator()).
		SpawnMiddleware
}
