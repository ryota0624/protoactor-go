package shared

import (
	"context"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/postgresql"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/postgresql/ddl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"sync"
	"time"
)

var containerRunOnce = &sync.Once{}
var postgresContainer *postgres.PostgresContainer

func NewLockUp(actorSystem *actor.ActorSystem) (cluster.IdentityLookup, func()) {
	ctx := context.Background()
	dbName := "users"
	dbUser := "user"
	dbPassword := "password"
	var err error
	containerRunOnce.Do(func() {
		postgresContainer, err = postgres.Run(ctx,
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
	})

	if err != nil {
		log.Printf("failed to start container: %s", err)
		panic(err)
	}

	connString := postgresContainer.MustConnectionString(ctx, "sslmode=disable", "application_name=test")
	println(connString)
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		panic(err)
	}

	if err := prepareTables(ctx, connString); err != nil {
		panic(err)
	}
	postgresLookup := postgresql.NewStorageLookup(pool, actorSystem.Logger(), actorSystem.Root, time.Second*3, actorSystem.ID)
	postgresLookup.Init()
	return cluster.NewIdentityStorageLookup(postgresLookup), func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}
}

func prepareTables(ctx context.Context, connString string) error {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return err
	}

	queries := []string{
		ddl.ActivationsSql,
		ddl.SpawnLocksSql,
	}

	for _, query := range queries {
		_, err = pool.Exec(ctx, query)
		if err != nil {
			return err
		}
	}

	return nil
}
