package cluster

import (
	"github.com/lithammer/shortuuid/v4"
	"log/slog"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

const (
	placementActorName           = "placement-activator"
	workerActorName              = "identity-storage-worker"
	pidClusterIdentityStartIndex = len(placementActorName) + 1
)

// IdentityStorageLookup contains
type IdentityStorageLookup struct {
	Storage        StorageLookup
	cluster        *Cluster
	isClient       bool
	placementActor *actor.PID
	system         *actor.ActorSystem
	worker         *actor.PID
	memberID       string
}

var _ IdentityLookup = (*IdentityStorageLookup)(nil)

func NewIdentityStorageLookup(storage StorageLookup) *IdentityStorageLookup {
	this := &IdentityStorageLookup{
		Storage: storage,
	}
	return this
}

// RemoveMember from identity storage
func (i *IdentityStorageLookup) RemoveMember(memberID string) {
	i.Storage.RemoveMemberId(memberID)
}

// RemotePlacementActor returns the PID of the remote placement actor
func RemotePlacementActor(address string) *actor.PID {
	return actor.NewPID(address, placementActorName)
}

//
// Interface: IdentityLookup
//

// Get returns a PID for a given ClusterIdentity
func (i *IdentityStorageLookup) Get(clusterIdentity *ClusterIdentity) *actor.PID {
	i.system.Logger().Info("Get", slog.Any("clusterIdentity", clusterIdentity))
	msg := newGetPid(clusterIdentity)
	timeout := 5 * time.Second

	res, err := i.system.Root.RequestFuture(i.worker, msg, timeout).Result()
	if err != nil {
		i.system.Logger().Error("Failed to get pid", slog.Any("error", err))
		return nil
	}
	response := res.(*PidResult)
	return response.Pid
}

func (i *IdentityStorageLookup) RemovePid(clusterIdentity *ClusterIdentity, _ *actor.PID) {
	if i.system.IsStopped() {
		return
	}

	i.Storage.RemoveActivation(newSpawnLock(shortuuid.New(), clusterIdentity))
}

func (i *IdentityStorageLookup) Shutdown() {
	i.system.Root.Stop(i.worker)
	if !i.isClient {
		i.system.Root.Stop(i.placementActor)
	}

	i.RemoveMember(i.memberID)
	i.system.Root.Stop(i.worker)
}

func (i *IdentityStorageLookup) Setup(cluster *Cluster, _ []string, isClient bool) {
	i.cluster = cluster
	i.system = cluster.ActorSystem
	i.memberID = cluster.ActorSystem.ID
	i.isClient = isClient

	workerProps := actor.PropsFromProducer(func() actor.Actor { return newIdentityStorageWorker(i) })
	var err error
	i.worker, err = i.system.Root.SpawnNamed(workerProps, workerActorName)
	if err != nil {
		panic(err)
	}
	i.cluster.ActorSystem.EventStream.SubscribeWithPredicate(func(message interface{}) {
		ct := message.(*ClusterTopology)
		for _, member := range ct.Left {
			i.RemoveMember(member.Id)
		}
	}, func(m interface{}) bool {
		_, ok := m.(*ClusterTopology)
		return ok
	})

	if i.isClient {
		return
	}

	i.placementActor, err = i.system.Root.SpawnNamed(actor.PropsFromProducer(func() actor.Actor { return newIdentityStoragePlacementActor(cluster, i) }), placementActorName)
	i.cluster.Logger().Info("placement actor started", slog.Any("actor", i.placementActor))
	if err != nil {
		panic(err)
	}
}
