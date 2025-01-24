package postgresql

import (
	"context"
	_ "embed"
	"encoding/json"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/postgresql/ddl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestStorageLookup_StoreActivation(t *testing.T) {
	actorSystem := actor.NewActorSystem()
	lookup, cleanup := PrepareLookup(actorSystem)
	defer cleanup()

	clusterIdentity := &cluster.ClusterIdentity{
		Identity: "test",
		Kind:     "test",
	}

	memberID := "test"

	spawnLock := &cluster.SpawnLock{
		LockID:          "test",
		ClusterIdentity: clusterIdentity,
	}

	pid := actor.NewPID("test", "test")
	lookup.StoreActivation(memberID, spawnLock, pid)
	gotActivation := lookup.TryGetExistingActivation(clusterIdentity)
	if gotActivation == nil {
		t.Error("activation not found")
		return
	}

	pidJson, _ := json.Marshal(pid)
	if gotActivation.Pid != string(pidJson) {
		t.Errorf("expected %s, got %s", string(pidJson), gotActivation.Pid)
	}

	if gotActivation.MemberID != memberID {
		t.Errorf("expected %s, got %s", memberID, gotActivation.MemberID)
	}
}

func TestStorageLookup_RemoveActivation(t *testing.T) {
	actorSystem := actor.NewActorSystem()
	lookup, cleanup := PrepareLookup(actorSystem)
	defer cleanup()

	clusterIdentity := &cluster.ClusterIdentity{
		Identity: "test",
		Kind:     "test",
	}

	memberID := "test"

	spawnLock := &cluster.SpawnLock{
		LockID:          "test",
		ClusterIdentity: clusterIdentity,
	}

	pid := actor.NewPID("test", "test")
	lookup.StoreActivation(memberID, spawnLock, pid)
	lookup.RemoveActivation(spawnLock)
	gotActivation := lookup.TryGetExistingActivation(clusterIdentity)
	if gotActivation != nil {
		t.Error("activation not removed")
	}
}

func TestStorageLookup_RemoveMemberId(t *testing.T) {
	actorSystem := actor.NewActorSystem()
	lookup, cleanup := PrepareLookup(actorSystem)
	defer cleanup()

	clusterIdentity := &cluster.ClusterIdentity{
		Identity: "test",
		Kind:     "test",
	}

	memberID := "test"

	spawnLock := &cluster.SpawnLock{
		LockID:          "test",
		ClusterIdentity: clusterIdentity,
	}

	pid := actor.NewPID("test", "test")
	lookup.StoreActivation(memberID, spawnLock, pid)
	lookup.RemoveMemberId(memberID)
	gotActivation := lookup.TryGetExistingActivation(clusterIdentity)
	if gotActivation != nil {
		t.Error("activation not removed")
	}
}

func TestStorageLookup_TryAcquireLock(t *testing.T) {

	t.Run("lock acquired", func(t *testing.T) {
		actorSystem := actor.NewActorSystem()
		lookup, cleanup := PrepareLookup(actorSystem)
		defer cleanup()

		clusterIdentity := &cluster.ClusterIdentity{
			Identity: strconv.FormatFloat(rand.Float64(), 'f', -1, 64),
			Kind:     "test",
		}

		lock := lookup.TryAcquireLock(clusterIdentity)
		if lock == nil {
			t.Error("lock not acquired")
			return
		}

		if lock := lookup.TryAcquireLock(clusterIdentity); lock != nil {
			t.Error("lock acquired twice")
		}
		lookup.RemoveLock(*lock)
		if lock := lookup.TryAcquireLock(clusterIdentity); lock == nil {
			t.Error("lock not acquired")
		}
	})
	t.Run("lock cant acquired when activation exists", func(t *testing.T) {
		actorSystem := actor.NewActorSystem()
		lookup, cleanup := PrepareLookup(actorSystem)
		defer cleanup()

		clusterIdentity := &cluster.ClusterIdentity{
			Identity: strconv.FormatFloat(rand.Float64(), 'f', -1, 64),
			Kind:     "test",
		}

		lock := lookup.TryAcquireLock(clusterIdentity)
		if lock == nil {
			t.Error("lock not acquired")
			return
		}

		lookup.RemoveLock(*lock)
		lookup.StoreActivation("test",
			lock,
			actor.NewPID("test", "test"),
		)
		if lock := lookup.TryAcquireLock(clusterIdentity); lock != nil {
			t.Error("lock acquired twice")
		}
		lookup.RemoveActivation(lock)
		if lock := lookup.TryAcquireLock(clusterIdentity); lock == nil {
			t.Error("lock not acquired")
		}
	})

}

func PrepareLookup(actorSystem *actor.ActorSystem) (*StorageLookup, func()) {
	ctx := context.Background()
	dbName := "users"
	dbUser := "user"
	dbPassword := "password"
	var err error
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
	if err != nil {
		log.Printf("failed to start container: %s", err)
		panic(err)
	}

	connString := postgresContainer.MustConnectionString(ctx, "sslmode=disable", "application_name=test")
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		panic(err)
	}

	if err := prepareTables(ctx, connString); err != nil {
		panic(err)
	}
	postgresLookup := NewStorageLookup(pool, actorSystem.Logger(), actorSystem.Root, time.Second*3, actorSystem.ID)
	postgresLookup.Init()
	return postgresLookup, func() {
		postgresLookup.Shutdown()
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
