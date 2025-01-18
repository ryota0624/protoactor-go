package main

import (
	context2 "context"
	"fmt"
	"github.com/asynkron/protoactor-go/persistence/postgresql"
	"github.com/asynkron/protoactor-go/persistence/postgresql/ddl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/reflect/protoregistry"
	"log"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PersistenceActorActivator struct {
	persistenceActorProps *actor.Props
	persistenceActorName  string
}

func PersistenceActorActivatorProps(
	persistenceActorProps *actor.Props,
	persistenceActorName string,
) (*actor.Props, string) {
	return actor.PropsFromProducer(func() actor.Actor {
		return &PersistenceActorActivator{
			persistenceActorProps: persistenceActorProps,
			persistenceActorName:  persistenceActorName,
		}
	}), fmt.Sprintf("activator-%s", persistenceActorName)
}

func (a *PersistenceActorActivator) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		log.Println("proxy actor started")
	case *actor.Stopped:
		log.Println("proxy actor stopped")
	default:
		if _, ok := ctx.Message().(actor.SystemMessage); ok {
			return
		}
		id, err := ctx.ActorSystem().Root.SpawnNamed(a.persistenceActorProps, a.persistenceActorName)
		if err != nil {
			log.Printf("failed to spawn actor: %s", err)
		}
		ctx.Forward(id)
		if err := ctx.PoisonFuture(id).Wait(); err != nil {
			log.Printf("failed to poison actor: %s", err)
		}
	}
}

type WantPersistentActor struct {
	persistence.Mixin
	state string
}

func (a *WantPersistentActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		log.Println("actor started")
	case *persistence.RequestSnapshot:
		log.Printf("snapshot internal state '%v'", a.state)
		a.PersistSnapshot(&Snapshot{state: a.state})
	case *actor.Stopped:
		log.Println("actor stopped")
	case *Snapshot:
		a.state = msg.state
		log.Printf("recovered from snapshot, internal state changed to '%v'", a.state)
	case *persistence.ReplayComplete:
		log.Printf("replay completed, internal state changed to '%v'", a.state)
	case *Message:
		scenario := "received replayed event"
		if !a.Recovering() {
			a.PersistReceive(msg)
			scenario = "received new message"
		}
		a.state = msg.state
		log.Printf("%s, internal state changed to '%v'\n", scenario, a.state)
		if ctx.Sender() != nil {
			ctx.Respond(&Message{state: fmt.Sprintf("response to message: %s", msg.state)})
		}
	}
}

func main() {
	if err := protoregistry.GlobalTypes.RegisterMessage(&Message{}); err != nil {
		panic(err)
	}
	if err := protoregistry.GlobalTypes.RegisterMessage(&Snapshot{}); err != nil {
		panic(err)
	}
	ctx := context2.Background()
	dbName := "users"
	dbUser := "user"
	dbPassword := "password"

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)

	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}
	connString := postgresContainer.MustConnectionString(ctx, "sslmode=disable", "application_name=test")
	err = prepareTables(ctx, connString)
	if err != nil {
		log.Printf("failed to prepare tables: %s", err)
		return
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Printf("failed to parse connection string: %s", err)
		return
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	system := actor.NewActorSystem()
	provider := postgresql.NewProvider(pool, system, 1000)
	rootContext := system.Root

	activatorPid := func(actorName string) *actor.PID {
		props, activatorName := PersistenceActorActivatorProps(actor.PropsFromProducer(
			func() actor.Actor { return &WantPersistentActor{} },
			actor.WithReceiverMiddleware(
				persistence.Using(provider),
			),
		), actorName)
		pid, err := rootContext.SpawnNamed(props, activatorName)
		if err != nil {
			log.Printf("failed to spawn proxy actor: %s %s", err, activatorName)
			pid = actor.NewPID("nonhost", activatorName)
		}

		return pid
	}

	sendMessage := func(actorName string, message string) {
		pid := activatorPid(actorName)
		rootContext.Send(pid, &Message{state: message})
	}

	requestFMessage := func(actorName string, message string) {
		pid := activatorPid(actorName)
		res, err := rootContext.RequestFuture(pid, &Message{state: message}, time.Second).Result()
		if err != nil {
			log.Printf("request error err=%v", err)
		} else {
			log.Printf("response=%v", res)
		}
	}

	persistenceActorId := "echo-actor-1"
	go func() {
		sendMessage(persistenceActorId, "message1")
	}()
	go func() {
		sendMessage(persistenceActorId, "message2")
	}()
	go func() {
		requestFMessage(persistenceActorId, "message3")
	}()
	_, _ = console.ReadLine()
}

func prepareTables(ctx context2.Context, connString string) error {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return err
	}

	_, err = pool.Exec(context2.Background(), ddl.JournalsSQL)
	if err != nil {
		return err
	}

	_, err = pool.Exec(context2.Background(), ddl.SnapshotsSQL)
	return err
}
