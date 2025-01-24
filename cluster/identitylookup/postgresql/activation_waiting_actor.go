package postgresql

import (
	context2 "context"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/scheduler"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
	"iter"
	"log/slog"
	"slices"
	"strings"
	"time"
)

type AddWaiting struct {
	clusterIdentity *cluster.ClusterIdentity
}

type Activated struct {
	Pid      string
	MemberId string
}

type ActivationCancelled struct {
}

type WaitActivationTimeout struct {
}

type ReplyWaiting struct {
	replyTo      *actor.PID
	countOfRetry int
}

type queryingTimeReached struct {
}

func ClusterIdentityFromKey(key string) *cluster.ClusterIdentity {
	slice := strings.Split(key, "/")
	return cluster.NewClusterIdentity(slice[1], slice[0])
}

type ActivationWaitingActor struct {
	pool                    *pgxpool.Pool
	poolingQueryInterval    time.Duration
	waitList                map[string] /*clusterIdentity#AsKey*/ []*ReplyWaiting
	inflight                []*cluster.ClusterIdentity
	cancelScheduler         scheduler.CancelFunc
	logger                  *slog.Logger
	actorSystem             *actor.ActorSystem
	maxQueryRetryPerWaiting int
}

func newActivationWaitingActor(pool *pgxpool.Pool, poolingQueryInterval time.Duration, logger *slog.Logger) *ActivationWaitingActor {
	this := &ActivationWaitingActor{
		pool:                 pool,
		poolingQueryInterval: poolingQueryInterval,
		waitList:             make(map[string] /*clusterIdentity#AsKey*/ []*ReplyWaiting),
		logger: logger.With(
			slog.String("actorType", "ActivationWaitingActor"),
		),
		maxQueryRetryPerWaiting: 3,
	}
	return this
}

func (a *ActivationWaitingActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *AddWaiting:
		a.onAddWaiting(context, msg)
	case *actor.Started:
		a.onStarted(context)
	case *queryingTimeReached:
		a.onQueryingTimeReached(context)
	case *actor.Stopping:
		a.onStopping(context)
	}
}

func (a *ActivationWaitingActor) onAddWaiting(context actor.Context, msg *AddWaiting) {
	if _, ok := a.waitList[msg.clusterIdentity.AsKey()]; !ok {
		a.waitList[msg.clusterIdentity.AsKey()] = []*ReplyWaiting{}
	}

	a.waitList[msg.clusterIdentity.AsKey()] = append(a.waitList[msg.clusterIdentity.AsKey()], &ReplyWaiting{
		replyTo:      context.Sender(),
		countOfRetry: 0,
	})
}

func (a *ActivationWaitingActor) onStarted(context actor.Context) {
	a.logger = a.logger.With(slog.String("pid", context.Self().Id))
	a.cancelScheduler = scheduler.NewTimerScheduler(context).SendRepeatedly(a.poolingQueryInterval, a.poolingQueryInterval, context.Self(), &queryingTimeReached{})
}

func (a *ActivationWaitingActor) onQueryingTimeReached(context actor.Context) {
	if len(a.waitList) == 0 {
		return
	}

	if len(a.inflight) > 0 {
		return
	}

	clear(a.inflight)
	for key := range a.waitList {
		clusterIdentity := ClusterIdentityFromKey(key)
		a.inflight = append(a.inflight, clusterIdentity)
	}

	pull, _ := iter.Pull(slices.Chunk(a.inflight, 100))
	for {
		identities, hasNext := pull()
		if !hasNext {
			break
		}

		var keys []string
		for _, identity := range identities {
			keys = append(keys, identity.AsKey())
		}

		a.logger.Info("Querying activations", slog.Any("keys", keys))
		result, err := a.pool.Query(context2.Background(), `
SELECT * FROM activations WHERE identity_key = ANY($1)
`, pq.Array(keys))

		if err != nil {
			context.Logger().Error("error while querying activations", slog.Any("error", err))
		}

		activations, err := pgx.CollectRows(result, pgx.RowToStructByName[ActivationsTableRow])
		if err != nil {
			context.Logger().Error("error while collecting activations", slog.Any("error", err))
		}

		for _, activation := range activations {
			for _, replayTo := range a.waitList[activation.IdentityKey] {
				a.logger.Info("Activation found", slog.Any("identityKey", activation.IdentityKey), slog.Any("memberId", activation.MemberId))
				context.Send(replayTo.replyTo, &Activated{
					Pid:      string(activation.Pid),
					MemberId: activation.MemberId,
				})
			}
			delete(a.waitList, activation.IdentityKey)
		}

		for _, identity := range identities {
			if _, ok := a.waitList[identity.AsKey()]; ok {
				remainWaitList := make([]*ReplyWaiting, 0)
				for _, replayTo := range a.waitList[identity.AsKey()] {
					replayTo.countOfRetry++
					if replayTo.countOfRetry >= a.maxQueryRetryPerWaiting {
						a.logger.Info("Activation not found by retry count exceeded", slog.Any("identity", identity))
						context.Send(replayTo.replyTo, &WaitActivationTimeout{})
					} else {
						remainWaitList = append(remainWaitList, replayTo)
					}
				}
				if len(remainWaitList) > 0 {
					a.waitList[identity.AsKey()] = remainWaitList
				} else {
					delete(a.waitList, identity.AsKey())
				}
			}
		}

		a.inflight = make([]*cluster.ClusterIdentity, 0)
	}

}

func (a *ActivationWaitingActor) onStopping(context actor.Context) {
	if a.cancelScheduler != nil {
		a.cancelScheduler()
	}

	for _, waiting := range a.waitList {
		for _, waiting := range waiting {
			context.Send(waiting.replyTo, &ActivationCancelled{})
		}
	}
}
