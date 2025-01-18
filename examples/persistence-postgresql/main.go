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

type Actor struct {
	persistence.Mixin
	state string
}

func (a *Actor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		log.Println("actor started")
	case *persistence.RequestSnapshot:
		log.Printf("snapshot internal state '%v'", a.state)
		a.PersistSnapshot(&Snapshot{state: a.state})
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

	case *postgresql.PersistenceErrorOccurred:
		log.Printf("PersistenceErrorOccurred: %s", msg.PersistenceError)
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
	provider := postgresql.NewProvider(pool, system)

	props := actor.PropsFromProducer(func() actor.Actor { return &Actor{} },
		actor.WithReceiverMiddleware(
			postgresql.HandlePersistenceError(),
			persistence.Using(provider),
		),
	)
	rootContext := system.Root

	pid, _ := rootContext.SpawnNamed(props, "echoActor")
	rootContext.Send(pid, &Message{state: "state1"})
	rootContext.Send(pid, &Message{state: "state2"})
	if result, err := rootContext.RequestFuture(pid, &Message{state: "state3"}, time.Second*1).Result(); err != nil {
		fmt.Printf("request error err=%v\n", err)
	} else {
		fmt.Printf("response=%v\n", result)
	}

	err = rootContext.PoisonFuture(pid).Wait()
	if err != nil {
		fmt.Printf("poison err=%v\n", err)
	}
	fmt.Printf("*** restart ***\n")
	pid, _ = rootContext.SpawnNamed(props, "persistent")

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
