package shared

import "github.com/asynkron/protoactor-go/cluster"

func NewLockUp() cluster.IdentityLookup {
	return cluster.NewIdentityStorageLookup(nil)
}
