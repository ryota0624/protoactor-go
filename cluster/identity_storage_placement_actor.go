package cluster

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/eventstream"
	"log/slog"
)

type GrainMeta struct {
	ID  *ClusterIdentity
	PID *actor.PID
}

type IdentityStoragePlacementActor struct {
	cluster               *Cluster
	identityStorageLookup *IdentityStorageLookup
	subscription          *eventstream.Subscription
	actors                map[string] /* clusterIdentity*/ GrainMeta
	logger                *slog.Logger
}

func newIdentityStoragePlacementActor(cluster *Cluster, identityStorageLookup *IdentityStorageLookup) *IdentityStoragePlacementActor {
	this := &IdentityStoragePlacementActor{
		cluster:               cluster,
		identityStorageLookup: identityStorageLookup,
		actors:                make(map[string] /* clusterIdentity*/ GrainMeta),
		logger: cluster.Logger().With(
			slog.String("actorType", "IdentityStoragePlacementActor"),
		),
	}
	return this
}

func (i *IdentityStoragePlacementActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *actor.Started:
		i.onStarted(context)
	case *actor.Stopping:
		i.onStopping(context)
	case *ActivationTerminating:
		i.onActivationTerminating(context, msg)
	case *ActivationRequest:
		i.onActivationRequest(context, msg)
	}
}

func (i *IdentityStoragePlacementActor) onStarted(context actor.Context) {
	i.logger.Info("IdentityStoragePlacementActor started")
	i.logger = i.logger.With(slog.String("pid", context.Self().Id))
	i.subscription = i.cluster.ActorSystem.EventStream.Subscribe(func(evt interface{}) {
		context.Send(context.Self(), evt)
	})
}

func (i *IdentityStoragePlacementActor) onStopping(_ actor.Context) {
	i.logger.Info("IdentityStoragePlacementActor stopping")
	i.subscription.Deactivate()
	for _, meta := range i.actors {
		/// TODO: need to throttle?
		i.cluster.ActorSystem.Root.Stop(meta.PID)
	}
}

func (i *IdentityStoragePlacementActor) onActivationTerminating(_ actor.Context, msg *ActivationTerminating) {
	if grainMeta, ok := i.actors[msg.ClusterIdentity.AsKey()]; ok {
		if grainMeta.PID != msg.Pid {
			i.logger.Error("PID mismatch", slog.Any("clusterIdentity", msg.ClusterIdentity), slog.Any("expectedPid", grainMeta.PID), slog.Any("actualPid", msg.Pid))
			return
		}
		delete(i.actors, msg.ClusterIdentity.AsKey())
		i.cluster.PidCache.Remove(msg.ClusterIdentity.Identity, msg.ClusterIdentity.Kind)
		i.identityStorageLookup.Storage.RemoveActivation(newSpawnLock(grainMeta.PID.String(), msg.ClusterIdentity))
	} else {
		i.logger.Error("IdentityStoragePlacementActor#onActivationTerminating activation not found", slog.Any("clusterIdentity", msg.ClusterIdentity))
	}
}

func (i *IdentityStoragePlacementActor) onActivationRequest(context actor.Context, msg *ActivationRequest) {
	if grainMeta, ok := i.actors[msg.ClusterIdentity.AsKey()]; ok {
		context.Respond(&ActivationResponse{Pid: grainMeta.PID})
		return
	}

	clusterKind := i.cluster.GetClusterKind(msg.ClusterIdentity.Kind)
	if clusterKind == nil {
		context.Logger().Error("Cluster kind not found", slog.Any("clusterIdentity", msg.ClusterIdentity), slog.String("kind", msg.ClusterIdentity.Kind))
		context.Respond(&ActivationResponse{Pid: nil, Failed: true})
		return
	}

	i.spawnActor(context, msg, clusterKind)
}

func (i *IdentityStoragePlacementActor) spawnActor(ctx actor.Context, req *ActivationRequest, kind *ActivatedKind) {
	props := WithClusterIdentity(kind.Props, req.ClusterIdentity)
	pid := ctx.SpawnPrefix(props, req.ClusterIdentity.Identity)

	storeActivation := func(pid *actor.PID) (ok bool) {
		ok = false
		defer func() {
			if r := recover(); r != nil {
				i.logger.Error("Failed to store activation", slog.Any("error", r))
				ctx.Stop(pid)
				ok = false
			}
		}()
		i.identityStorageLookup.Storage.StoreActivation(i.cluster.ActorSystem.ID, newSpawnLock(req.RequestId, req.ClusterIdentity), pid)
		return true
	}

	ok := storeActivation(pid)
	if !ok {
		ctx.Respond(&ActivationResponse{Pid: nil, Failed: true})
		return
	}
	kind.Inc()

	/// TODO: member selectionを考慮
	i.logger.Info("Activation Stored", slog.Any("pid", pid), slog.Any("clusterIdentity", req.ClusterIdentity), slog.String("key", req.ClusterIdentity.AsKey()))
	i.actors[req.ClusterIdentity.AsKey()] = GrainMeta{
		ID:  req.ClusterIdentity,
		PID: pid,
	}
	i.cluster.PidCache.Set(req.ClusterIdentity.Identity, req.ClusterIdentity.Kind, pid)
	ctx.Respond(&ActivationResponse{Pid: pid})
}
