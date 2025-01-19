package main

import (
	"fmt"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/automanaged"
	"time"

	"cluster-identitylookup-postgresql/shared"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/remote"
)

func main() {
	c, clean := startNode(autoManagePorts[0])
	defer clean()

	c2, clean2 := startNode(autoManagePorts[1])
	defer clean2()

	for {
		if c.MemberList.Length() > 1 {
			break
		}
		c.Logger().Info("Waiting for other nodes to join")
		time.Sleep(time.Second)
	}

	pid := c.Get("abc", "hello")
	fmt.Printf("Got pid %v\n", pid)
	res, _ := c.Request("abc", "hello", &shared.HelloRequest{Name: "Roger"})
	fmt.Printf("Got response %v\n", res)
	fmt.Print("\nBoot other nodes and press Enter\n")
	for {
		st, _ := console.ReadLine()
		if st == "exit" {
			break
		}
		go func() {
			messageToCluster(c, st)
		}()
		go func() {
			messageToCluster(c2, st)
		}()
	}

	c.Shutdown(true)
}

func messageToCluster(c *cluster.Cluster, st string) {
	pid := c.Get(st, "hello")
	c.Logger().Info("Got pid %v", pid)
	res, _ := c.Request(st, "hello", &shared.HelloRequest{Name: "Roger"})
	c.Logger().Info("Got response %v", res)
}

var autoManagePorts = []int{
	8081,
	8082,
	//8083,
}

var (
	autoManageHosts []string
)

func init() {
	for _, port := range autoManagePorts {
		autoManageHosts = append(autoManageHosts, fmt.Sprintf("localhost:%v", port))
	}
}

func startNode(autoManagePort int) (*cluster.Cluster, func()) {
	system := actor.NewActorSystem()

	provider := automanaged.NewWithConfig(time.Second*2, autoManagePort, autoManageHosts...)
	lookup, clean := shared.NewLockUp(system)
	config := remote.Configure("localhost", 0)

	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *actor.Started:
			fmt.Printf("Started %v", msg)
			go func() {
				time.Sleep(time.Second * 15)
				ctx.Stop(ctx.Self())
			}()
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
