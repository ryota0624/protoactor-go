package main

import (
	"cluster-identitylookup-postgresql/shared"
	"fmt"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/automanaged"
	"github.com/asynkron/protoactor-go/remote"
)

func main() {
	c, clean := startNode()
	defer clean()

	fmt.Print("\nBoot other nodes and press Enter\n")
	console.ReadLine()
	pid := c.Get("abc", "hello")
	fmt.Printf("Got pid %v", pid)
	res, _ := c.Request("abc", "hello", &shared.HelloRequest{Name: "Roger"})
	fmt.Printf("Got response %v", res)

	fmt.Println()
	console.ReadLine()
	c.Shutdown(true)
}

func startNode() (*cluster.Cluster, func()) {
	system := actor.NewActorSystem()

	provider := automanaged.New()
	lookup, clean := shared.NewLockUp(system)
	config := remote.Configure("localhost", 0)
	clusterConfig := cluster.Configure("my-cluster", provider, lookup, config)
	c := cluster.New(system, clusterConfig)
	c.StartMember()

	return c, clean
}
