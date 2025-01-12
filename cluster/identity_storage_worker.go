package cluster

import (
	"encoding/json"

	"log"

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
	getPid, ok := m.(GetPid)

	if !ok {
		return
	}

	if c.Sender() == nil {
		log.Println("No sender in GetPid request")
		return
	}

	existing, _ := ids.cluster.PidCache.Get(getPid.ClusterIdentity.Identity, getPid.ClusterIdentity.Kind)

	if existing != nil {
		log.Printf("Found %s in pidcache", m.(GetPid).ClusterIdentity.ToShortString())
		c.Respond(newPidResult(existing))
	}

	activation := ids.storage.TryGetExistingActivation(getPid.ClusterIdentity)
	if activation != nil {
		pid := &actor.PID{}
		go func() {
			err := json.Unmarshal([]byte(activation.Pid), pid)
			if err != nil {
				panic(err)
			}
			ids.cluster.PidCache.Set(getPid.ClusterIdentity.Identity, getPid.ClusterIdentity.Kind, pid)
		}()

		c.Respond(newPidResult(pid))
		return
	}

	return
}
