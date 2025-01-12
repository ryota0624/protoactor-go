package main

import (
	"fmt"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/automanaged"

	"cluster-identitylookup-postgresql/shared"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/remote"
)

func main() {
	cluster, clean := startNode()
	defer clean()
	pid := cluster.Get("abc", "hello")
	fmt.Printf("Got pid %v\n", pid)
	res, _ := cluster.Request("abc", "hello", &shared.HelloRequest{Name: "Roger"})
	fmt.Printf("Got response %v\n", res)
	fmt.Print("\nBoot other nodes and press Enter\n")
	for {
		st, _ := console.ReadLine()
		if st == "exit" {
			break
		}
		pid := cluster.Get("abc", "hello")
		fmt.Printf("Got pid %v", pid)
		res, _ := cluster.Request("abc", "hello", &shared.HelloRequest{Name: "Roger"})
		fmt.Printf("Got response %v", res)
	}

	cluster.Shutdown(true)
}

func startNode() (*cluster.Cluster, func()) {
	system := actor.NewActorSystem()

	provider := automanaged.New()
	lookup, clean := shared.NewLockUp(system)
	config := remote.Configure("localhost", 0)

	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *actor.Started:
			fmt.Printf("Started %v", msg)
		case *shared.HelloRequest:
			fmt.Printf("Hello %v\n", msg.Name)
			ctx.Respond(&shared.HelloResponse{})
		}
	})
	helloKind := cluster.NewKind("hello", props)
	clusterConfig := cluster.Configure("my-cluster", provider, lookup, config, cluster.WithKinds(helloKind))
	c := cluster.New(system, clusterConfig)

	c.StartMember()

	return c, clean
}
