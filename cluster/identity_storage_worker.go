package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

type IdentityStorageWorker struct {
	cluster *Cluster
	lookup  *IdentityStorageLookup
	storage StorageLookup
	logger  *slog.Logger
}

func newIdentityStorageWorker(storageLookup *IdentityStorageLookup) *IdentityStorageWorker {
	return &IdentityStorageWorker{
		cluster: storageLookup.cluster,
		lookup:  storageLookup,
		storage: storageLookup.Storage,
		logger:  storageLookup.cluster.Logger().With(slog.String("actorType", "IdentityStorageWorker")),
	}
}

// Receive func
func (ids *IdentityStorageWorker) Receive(c actor.Context) {
	m := c.Message()
	getPid, ok := m.(*GetPid)

	if !ok {
		if _, ok := m.(*actor.Started); ok {
			ids.logger = ids.logger.With(slog.String("pid", c.Self().Id))
		}

		return
	}

	if c.Sender() == nil {
		log.Println("No sender in GetPid request")
		return
	}

	existing, _ := ids.cluster.PidCache.Get(getPid.ClusterIdentity.Identity, getPid.ClusterIdentity.Kind)

	if existing != nil {
		ids.logger.Info("Found pid in pidcache", slog.Any("clusterIdentity", m.(*GetPid).ClusterIdentity.ToShortString()))
		c.Respond(newPidResult(existing))
	}

	activation := ids.storage.TryGetExistingActivation(getPid.ClusterIdentity)
	if activation != nil {
		pid := &actor.PID{}
		err := json.Unmarshal([]byte(activation.Pid), pid)
		if err != nil {
			panic(fmt.Errorf("IdentityStorageWorker: Failed to unmarshal pid: %v", err))
		}
		ids.logger.Info("Found activation in storage", slog.Any("pid", pid), slog.Any("clusterIdentity", m.(*GetPid).ClusterIdentity.ToShortString()))
		if ids.cluster.MemberList.ContainsMemberID(activation.MemberID) {
			ids.cluster.PidCache.Set(getPid.ClusterIdentity.Identity, getPid.ClusterIdentity.Kind, pid)
			c.Respond(newPidResult(pid))
			return
		} else {
			/// TODO: 1 memberID につき一度だけ発生させる
			c.Logger().Info("MemberID not found in MemberList, removing activation")
			ids.storage.RemoveMemberId(activation.MemberID)
		}
	}

	/// round robinでは普通にNode同士でズレる
	activator := ids.cluster.MemberList.GetActivatorMember(getPid.ClusterIdentity.Kind, getPid.ClusterIdentity.Identity)

	if activator == "" {
		c.Respond(newPidResult(nil))
		return
	}

	// TODO: TryGetExistingActivationからlock取得までに別のNodeでActivateが発生した場合に,
	//  lockはすでに消されてていて、lockが取得できてしまいこのNodeでActivateが発生してしまう
	//  lockを消すのを遅延させる？

	// TryGetExistingActivation -> ない
	// TryAcquireLock -> ない
	// 両方ないのはまずい

	// Lockには有効期限をもうける?
	//  有効期限が切れたら消す。
	//  それまでは消さない。
	// LockはNodeを消す時に同時に消す
	// Nodeが消えるときにLockが消せなかったら？
	lock := ids.storage.TryAcquireLock(getPid.ClusterIdentity)
	if lock == nil {
		activation := ids.storage.WaitForActivation(getPid.ClusterIdentity)
		if activation != nil {
			pid := &actor.PID{}
			err := json.Unmarshal([]byte(activation.Pid), pid)
			if err != nil {
				panic(fmt.Sprintf("IdentityStorageWorker: Failed to unmarshal pid: %v", err))
			}
			if ids.cluster.MemberList.ContainsMemberID(activation.MemberID) {
				ids.cluster.PidCache.Set(getPid.ClusterIdentity.Identity, getPid.ClusterIdentity.Kind, pid)
				c.Respond(newPidResult(pid))
				return
			} else {
				/// TODO: 1 memberID につき一度だけ発生させる
				c.Logger().Info("MemberID not found in MemberList, removing activation")
				ids.storage.RemoveMemberId(activation.MemberID)
			}
		}
		c.Respond(newPidResult(nil))
		return
	}
	defer func() {
		ids.storage.RemoveLock(*lock)
	}()
	ids.logger.Info("Lock acquired",
		slog.Any("lockId", lock.LockID),
		slog.Any("clusterIdentity", getPid.ClusterIdentity.ToShortString()),
		slog.Any("activator", activator),
	)

	pid := ids.SpawnActivation(activator, *lock)
	c.Respond(newPidResult(pid))
	return
}

func (ids *IdentityStorageWorker) SpawnActivation(activatorAddress string, lock SpawnLock) *actor.PID {
	ids.logger.Info("Spawning activation", slog.Any("spawnTo", activatorAddress), slog.Any("clusterIdentity", lock.ClusterIdentity))
	remotePid := RemotePlacementActor(activatorAddress)
	activateRequest := &ActivationRequest{
		ClusterIdentity: lock.ClusterIdentity,
		RequestId:       lock.LockID,
	}

	activationResult, err := ids.cluster.ActorSystem.Root.RequestFuture(remotePid, activateRequest, 10*time.Second).Result()
	if err != nil {
		ids.logger.Error("Failed to activate", slog.Any("error", err))
		return nil
	}

	activationResponse := activationResult.(*ActivationResponse)
	if activationResponse.Failed {
		ids.logger.Error("Failed to activate")
		return nil
	}
	ids.cluster.PidCache.Set(lock.ClusterIdentity.Identity, lock.ClusterIdentity.Kind, activationResponse.Pid)
	return activationResponse.Pid
}
