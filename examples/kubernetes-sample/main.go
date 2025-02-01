package main

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/actor/middleware/otel/tracing"
	"github.com/asynkron/protoactor-go/cluster"

	"github.com/asynkron/protoactor-go/cluster/clusterproviders/k8s"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/disthash"
	"github.com/asynkron/protoactor-go/remote"
	"k8s.io/utils/env"
)

func main() {
	log.Printf("Starting node\n")

	c, tracerProvider, cleanup := startNode()
	defer c.Shutdown(true)
	defer cleanup()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sendMessages(ctx, c, tracerProvider)

	log.Printf("Shutting down\n")
}

func sendMessages(ctx context.Context, c *cluster.Cluster, tracingProvider trace.TracerProvider) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			identity := strconv.Itoa(rand.Int())
			log.Printf("Sending message to %s\n", identity)
			_, span := tracingProvider.Tracer("hello-actor").Start(context.Background(), "request-to-hello-actor")
			requestSender := c.ActorSystem.Root.Copy().
				WithSenderMiddleware(tracing.RootContextSenderMiddleware()).
				WithHeaders(tracing.MapFromSpanContext(span.SpanContext()))
			if _, err := c.Request(
				identity,
				"helloKind",
				&HelloRequest{Name: fmt.Sprintf("Hello from %s", c.ActorSystem.ID)},
				cluster.WithContext(requestSender),
			); err != nil {
				log.Printf("Error calling actor: %v\n", err)
			} else {
				log.Printf("Successfully called actor\n")
			}
			span.End()
		}
	}
}

func helloGrainReceive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *HelloRequest:
		log.Printf("Got hello %v iam %s\n", msg, ctx.Self().String())
		ctx.Respond(&HelloResponse{})
	}
}

func startNode() (*cluster.Cluster, *sdktrace.TracerProvider, func()) {
	resource, err := NewResource("sample-app", "0.1.0", "TODO")
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}
	meterProvider, traceProvider := SetUpTelemetry(resource)

	config := actor.Configure(actor.WithMetricProviders(meterProvider))
	system := actor.NewActorSystemWithConfig(config)

	system.Extensions.Register(tracing.NewTraceExtension(traceProvider))
	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(meterProvider)
	provider, err := k8s.New()
	if err != nil {
		log.Panic(err)
	}
	lookup := disthash.New()

	host, port, advertisedHost := getHostInformation()
	remoConfig := remote.Configure(host, port, remote.WithAdvertisedHost(advertisedHost))

	props := actor.PropsFromFunc(helloGrainReceive, actor.WithContextDecorator(tracing.ContextDecorator()))
	helloKind := cluster.NewKind("helloKind", props)

	clusterConfig := cluster.Configure("my-cluster", provider, lookup, remoConfig, cluster.WithKinds(helloKind))

	c := cluster.New(system, clusterConfig)
	c.StartMember()

	return c, traceProvider, func() {
		err := meterProvider.Shutdown(context.Background())
		if err != nil {
			log.Printf("failed to shutdown meter provider: %v\n", err)
		}
		err = traceProvider.Shutdown(context.Background())
		if err != nil {
			log.Printf("failed to shutdown trace provider: %v\n", err)
		}
	}
}

func getHostInformation() (host string, port int, advertisedHost string) {
	host = env.GetString("PROTOHOST", "127.0.0.1")
	port, err := env.GetInt("PROTOPORT", 0)
	if err != nil {
		log.Panic(err)
	}
	advertisedHost = env.GetString("PROTOADVERTISEDHOST", "")

	log.Printf("host: %s, port: %d, advertisedHost: %s", host, port, advertisedHost)

	return
}
