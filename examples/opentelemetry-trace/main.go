package main

import (
	context2 "context"
	"fmt"
	"github.com/asynkron/protoactor-go/actor/middleware/otel/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type (
	hello      struct{ Who string }
	helloActor struct{}
)

func (state *helloActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *hello:
		fmt.Printf("Hello %s\n", msg.Who)
	}
}

func main() {
	traceConsoleExporter, err := stdouttrace.New()
	if err != nil {
		panic(err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceConsoleExporter),
	)

	defer func() {
		err := traceProvider.Shutdown(context2.Background())
		if err != nil {
			fmt.Printf("failed to shutdown trace provider: %v\n", err)
		}
	}()

	system := actor.NewActorSystem()
	system.Extensions.Register(tracing.NewTraceExtension(traceProvider))

	props := actor.PropsFromProducer(func() actor.Actor {
		return &helloActor{}
	})
	root := tracing.TraceableRootContext(system.Root)
	otel.SetTracerProvider(traceProvider)

	pid := root.Spawn(props)
	root.Request(pid, &hello{Who: "with tracing"})
	time.Sleep(100 * time.Millisecond)
	_, _ = console.ReadLine()
}
