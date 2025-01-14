package postgresql

import (
	context2 "context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lithammer/shortuuid/v4"
	"log/slog"
	"strings"
	"time"
)

type StorageLookup struct {
	pool                        *pgxpool.Pool
	logger                      *slog.Logger
	waiting                     *actor.PID
	rootContext                 *actor.RootContext
	waitingForActivationTimeout time.Duration
	memberId                    string
}

var _ cluster.StorageLookup = (*StorageLookup)(nil)

func NewStorageLookup(pool *pgxpool.Pool, logger *slog.Logger, rootContext *actor.RootContext, waitingForActivationTimeout time.Duration, memberId string) *StorageLookup {
	return &StorageLookup{
		pool:                        pool,
		logger:                      logger.With(slog.String("component", "postgresql.StorageLookup"), slog.String("ActorSystemId", rootContext.ActorSystem().ID)),
		rootContext:                 rootContext,
		waitingForActivationTimeout: waitingForActivationTimeout,
		memberId:                    memberId,
	}
}

func (s *StorageLookup) Init() {
	var err error
	s.waiting, err = s.rootContext.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
		return newActivationWaitingActor(s.pool, 10*time.Millisecond, s.logger)
	}), "activation-waiting-actor")
	if err != nil {
		panic(fmt.Sprintf("failed to spawn activation waiting actor: %v", err))
	}
}

func (s *StorageLookup) Shutdown() *cluster.ClusterIdentity {
	s.logger.Info("shutting down")
	s.rootContext.Stop(s.waiting)
	return nil
}

func (s *StorageLookup) TryGetExistingActivation(clusterIdentity *cluster.ClusterIdentity) *cluster.StoredActivation {
	row, err := s.pool.Query(
		context2.Background(),
		"SELECT * FROM activations WHERE identity = $1 AND kind = $2",
		clusterIdentity.Identity,
		clusterIdentity.Kind,
	)
	if err != nil {
		s.logger.Error("error while querying for activation", slog.Any("error", err))
		return nil
	}
	if row, err := pgx.CollectExactlyOneRow(row, pgx.RowToStructByName[ActivationsTableRow]); err == nil {
		return &cluster.StoredActivation{
			Pid:      string(row.Pid),
			MemberID: row.MemberId,
		}
	} else if errors.Is(err, pgx.ErrNoRows) {
		s.logger.Info("activation not found",
			slog.Any("identity", clusterIdentity.Identity),
			slog.Any("kind", clusterIdentity.Kind),
		)
		return nil
	} else {
		s.logger.Error("error while collecting row", slog.Any("error", err))
	}

	return nil
}

func (s *StorageLookup) TryAcquireLock(clusterIdentity *cluster.ClusterIdentity) *cluster.SpawnLock {
	lockId := shortuuid.New()

	effect, err := s.pool.Exec(context2.Background(), `
INSERT INTO spawn_locks (lock_id, identity, kind, member_id)
SELECT $1, $2, $3, $4
WHERE NOT EXISTS (
    SELECT 1
    FROM activations
    WHERE activations.identity = $2
      AND activations.kind = $3
);
`,
		lockId,
		clusterIdentity.Identity,
		clusterIdentity.Kind,
		s.memberId,
	)

	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
			s.logger.Info("lock already exists with constraint failure", slog.Any("identity", clusterIdentity.Identity), slog.Any("kind", clusterIdentity.Kind))
			return nil
		}
		s.logger.Error("error while inserting lock", slog.Any("error", err))
		return nil
	}

	if effect.RowsAffected() == 0 {
		s.logger.Info("lock already exists activation exists yet", slog.Any("identity", clusterIdentity.Identity), slog.Any("kind", clusterIdentity.Kind))
		return nil
	}

	return &cluster.SpawnLock{
		LockID:          lockId,
		ClusterIdentity: clusterIdentity,
	}
}

func (s *StorageLookup) WaitForActivation(clusterIdentity *cluster.ClusterIdentity) *cluster.StoredActivation {
	s.logger.Info("waiting for activation",
		slog.Any("identity", clusterIdentity.Identity),
		slog.Any("kind", clusterIdentity.Kind),
	)
	res, err := s.rootContext.RequestFuture(
		s.waiting, &AddWaiting{clusterIdentity: clusterIdentity},
		s.waitingForActivationTimeout,
	).Result()
	if err != nil {
		s.logger.Error("error while waiting for activation", slog.Any("error", err))
		return nil
	}

	switch res := res.(type) {
	case *Activated:
		return &cluster.StoredActivation{
			Pid:      res.Pid,
			MemberID: res.MemberId,
		}
	case *ActivationCancelled:
		return nil
	case *WaitActivationTimeout:
		return nil
	}

	s.logger.Error("unexpected response", slog.Any("response", res))
	return nil
}

func (s *StorageLookup) RemoveLock(spawnLock cluster.SpawnLock) {
	_, err := s.pool.Exec(context2.Background(), "DELETE FROM spawn_locks WHERE lock_id = $1", spawnLock.LockID)
	s.logger.Info("removing lock", slog.Any("lockId", spawnLock.LockID))
	if err != nil {
		s.logger.Error("error while deleting lock", slog.Any("error", err))
		panic(fmt.Sprintf("error while deleting lock: %v", err))
	}
}

func (s *StorageLookup) StoreActivation(memberID string, spawnLock *cluster.SpawnLock, pid *actor.PID) {
	pidJson, err := json.Marshal(pid)
	if err != nil {
		s.logger.Error("error while marshalling pid", slog.Any("error", err))
		panic(err)
	}
	_, err = s.pool.Exec(context2.Background(), `
INSERT INTO activations (identity, kind, identity_key, member_id, pid, lock_id) VALUES ($1, $2, $3, $4, $5, $6)
`,
		spawnLock.ClusterIdentity.Identity,
		spawnLock.ClusterIdentity.Kind,
		spawnLock.ClusterIdentity.AsKey(),
		memberID,
		pidJson,
		spawnLock.LockID,
	)
	if err != nil {
		s.logger.Error("error while inserting activation", slog.Any("error", err))
		panic(err)
	}
}

func (s *StorageLookup) RemoveActivation(lock *cluster.SpawnLock) {
	rows, err := s.pool.Query(context2.Background(), `DELETE FROM activations WHERE identity = $1 AND kind = $2 RETURNING *`, lock.ClusterIdentity.Identity, lock.ClusterIdentity.Kind)
	if err != nil {
		s.logger.Error("error while deleting activation", slog.Any("error", err))
	}
	s.logger.Info("removing activation", slog.Any("identity", lock.ClusterIdentity.Identity), slog.Any("kind", lock.ClusterIdentity.Kind))
	deletedRow, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[ActivationsTableRow])
	_, err = s.pool.Exec(context2.Background(), "DELETE FROM spawn_locks WHERE lock_id = $1", deletedRow.LockId)
	if err != nil {
		s.logger.Error("error while deleting spawn_locks", slog.Any("error", err))
	}
}

func (s *StorageLookup) RemoveMemberId(memberID string) {
	_, err := s.pool.Exec(context2.Background(), "DELETE FROM activations WHERE member_id = $1", memberID)
	if err != nil {
		s.logger.Error("error while deleting activations", slog.Any("error", err))
	}

	_, err = s.pool.Exec(context2.Background(), "DELETE FROM spawn_locks WHERE member_id = $1", memberID)
	if err != nil {
		s.logger.Error("error while deleting spawn_locks", slog.Any("error", err))
	}
}

type ActivationsTableRow struct {
	Identity    string    `db:"identity"`
	Kind        string    `db:"kind"`
	IdentityKey string    `db:"identity_key"`
	MemberId    string    `db:"member_id"`
	LockId      string    `db:"lock_id"`
	Pid         []byte    `db:"pid"`
	ActivatedAt time.Time `db:"activated_at"`
}

type SpanLocksTableRow struct {
	LockId   string `db:"lock_id"`
	Identity string `db:"identity"`
	Kind     string `db:"kind"`
}
