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
}

func newIdentityStorageWorker(storageLookup *IdentityStorageLookup) *IdentityStorageWorker {
	this := &IdentityStorageWorker{
		cluster: storageLookup.cluster,
		lookup:  storageLookup,
		storage: storageLookup.Storage,
	}
	return this
}

// Receive func
func (ids *IdentityStorageWorker) Receive(c actor.Context) {
	m := c.Message()
	getPid, ok := m.(*GetPid)

	if !ok {
		return
	}

	if c.Sender() == nil {
		log.Println("No sender in GetPid request")
		return
	}

	existing, _ := ids.cluster.PidCache.Get(getPid.ClusterIdentity.Identity, getPid.ClusterIdentity.Kind)

	if existing != nil {
		log.Printf("Found %s in pidcache", m.(*GetPid).ClusterIdentity.ToShortString())
		c.Respond(newPidResult(existing))
	}

	activation := ids.storage.TryGetExistingActivation(getPid.ClusterIdentity)
	if activation != nil {
		pid := &actor.PID{}
		err := json.Unmarshal([]byte(activation.Pid), pid)
		if err != nil {
			panic(fmt.Errorf("IdentityStorageWorker: Failed to unmarshal pid: %v", err))
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

	activator := ids.cluster.MemberList.GetActivatorMember(getPid.ClusterIdentity.Kind, getPid.ClusterIdentity.Identity)

	if activator == "" {
		c.Respond(newPidResult(nil))
		return
	}

	lock := ids.storage.TryAcquireLock(getPid.ClusterIdentity)
	if lock == nil {
		activation := ids.storage.WaitForActivation(getPid.ClusterIdentity)
		if activation != nil {
			pid := &actor.PID{}
			err := json.Unmarshal([]byte(activation.Pid), pid)
			if err != nil {
				panic(err)
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

	pid := ids.SpawnActivation(activator, *lock)
	c.Respond(newPidResult(pid))
	return
}

func (ids *IdentityStorageWorker) SpawnActivation(activatorAddress string, lock SpawnLock) *actor.PID {
	ids.cluster.Logger().Info("Spawning activation", slog.Any("spawnTo", activatorAddress), slog.Any("clusterIdentity", lock.ClusterIdentity))
	remotePid := RemotePlacementActor(activatorAddress)
	activateRequest := &ActivationRequest{
		ClusterIdentity: lock.ClusterIdentity,
		RequestId:       lock.LockID,
	}

	activationResult, err := ids.cluster.ActorSystem.Root.RequestFuture(remotePid, activateRequest, 10*time.Second).Result()
	if err != nil {
		ids.cluster.Logger().Error("Failed to activate", slog.Any("error", err))
		return nil
	}

	activationResponse := activationResult.(*ActivationResponse)
	if activationResponse.Failed {
		ids.cluster.Logger().Error("Failed to activate")
		return nil
	}
	ids.cluster.PidCache.Set(lock.ClusterIdentity.Identity, lock.ClusterIdentity.Kind, activationResponse.Pid)
	return activationResponse.Pid
}
