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

type ReplyTo struct {
	pid *actor.PID
}

type queryingTimeReached struct {
}

func ClusterIdentityFromKey(key string) *cluster.ClusterIdentity {
	slice := strings.Split(key, "/")
	return cluster.NewClusterIdentity(slice[1], slice[0])
}

type ActivationWaitingActor struct {
	pool                 *pgxpool.Pool
	poolingQueryInterval time.Duration
	waitList             map[string] /*clusterIdentity#AsKey*/ []*ReplyTo
	inflight             []*cluster.ClusterIdentity
	cancelScheduler      scheduler.CancelFunc
}

func newActivationWaitingActor(pool *pgxpool.Pool, poolingQueryInterval time.Duration) *ActivationWaitingActor {
	this := &ActivationWaitingActor{
		pool:                 pool,
		poolingQueryInterval: poolingQueryInterval,
		waitList:             make(map[string] /*clusterIdentity#AsKey*/ []*ReplyTo),
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
		a.waitList[msg.clusterIdentity.AsKey()] = []*ReplyTo{}
	}

	a.waitList[msg.clusterIdentity.AsKey()] = append(a.waitList[msg.clusterIdentity.AsKey()], &ReplyTo{
		pid: context.Sender(),
	})
}

func (a *ActivationWaitingActor) onStarted(context actor.Context) {
	/// TODO: clusterの停止に伴ってすべての待ちリストへゴメンを送る
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
		next, hasNext := pull()
		if !hasNext {
			break
		}

		var keys []string
		for _, identity := range next {
			keys = append(keys, identity.AsKey())
		}

		result, err := a.pool.Query(context2.Background(), `
SELECT * FROM activations WHERE identity_key in ($1)
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

				context.Send(replayTo.pid, &Activated{
					Pid:      string(activation.Pid),
					MemberId: activation.MemberId,
				})
			}
			delete(a.waitList, activation.IdentityKey)
		}

		clear(a.inflight)
	}

}

func (a *ActivationWaitingActor) onStopping(context actor.Context) {
	if a.cancelScheduler != nil {
		a.cancelScheduler()
	}

	for _, replayTo := range a.waitList {
		for _, reply := range replayTo {
			context.Send(reply.pid, &ActivationCancelled{})
		}
	}
}
