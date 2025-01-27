package main

import (
	"fmt"
	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/automanaged"
	"github.com/asynkron/protoactor-go/persistence"
	"log"
	"log/slog"
	"time"

	"cluster-identitylookup-postgresql-persistence/shared"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/remote"
)

func main() {
	c, clean := startNode(autoManagePorts[0])
	defer clean()

	_, clean2 := startNode(autoManagePorts[1])
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
	i := 0
	for {
		st, _ := console.ReadLine()
		if st == "exit" {
			break
		}
		messageToCluster(c, st, i)
		i += 1
	}

	c.Shutdown(true)
}

func messageToCluster(c *cluster.Cluster, st string, i int) {
	pid := c.Get(st, "hello")
	c.Logger().Info("Got pid %v", pid)
	f, err := c.RequestFuture(st, "hello", &shared.HelloRequest{Name: fmt.Sprintf("message %v", i)}, cluster.WithTimeout(time.Second))
	if err != nil {
		log.Printf("failed to request: %s", err)
	}

	res, err := f.Result()
	if err != nil {
		log.Printf("failed to get result: %s", err)
	}
	c.Logger().Info("Got response", slog.Any("res", res))
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
	lookup, persistenceProvider, clean := shared.NewLockUp(system)
	config := remote.Configure("localhost", 0)

	props := actor.PropsFromProducer(func() actor.Actor {
		return &PersistenceActorActivator{
			persistenceActorProps: actor.PropsFromProducer(func() actor.Actor {
				return &HelloActor{}
			}, actor.WithReceiverMiddleware(
				persistence.Using(persistenceProvider),
			)),
		}
	})

	helloKind := cluster.NewKind("hello", props)
	clusterConfig := cluster.Configure("my-cluster", provider, lookup, config, cluster.WithKinds(helloKind))
	c := cluster.New(system, clusterConfig)

	c.StartMember()

	return c, clean
}

type PersistenceActorActivator struct {
	persistenceActorProps *actor.Props
	persistenceActorName  string
}

func (a *PersistenceActorActivator) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		clusterIdentity := cluster.GetClusterIdentity(ctx)
		if clusterIdentity != nil {
			a.persistenceActorName = fmt.Sprintf("%s-%s", clusterIdentity.Kind, clusterIdentity.Identity)
		}
		log.Println("proxy actor started")
	case *actor.Stopped:
		log.Println("proxy actor stopped")
	default:

		if _, ok := ctx.Message().(actor.SystemMessage); ok {
			return
		}
		id, err := ctx.ActorSystem().Root.SpawnNamed(a.persistenceActorProps, a.persistenceActorName)
		if err != nil {
			log.Printf("failed to spawn actor: %s", err)
		}

		ctx.RequestWithCustomSender(id, ctx.Message(), ctx.Sender())
		if err := ctx.PoisonFuture(id).Wait(); err != nil {
			log.Printf("failed to poison actor: %s", err)
		}
	}
}

type HelloActor struct {
	lastReceivedName string
	persistence.Mixin
}

func (h *HelloActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		fmt.Printf("Started %v", msg)
		go func() {
			time.Sleep(time.Second * 15)
			ctx.Stop(ctx.Self())
		}()
	case *shared.HelloRequest:
		h.lastReceivedName = msg.Name
		if h.Recovering() {
			fmt.Printf("Recovering %v\n", msg.Name)
			return
		}
		h.PersistReceive(msg)
		fmt.Printf("Hello %v\n", msg.Name)
		if ctx.Sender() != nil {
			fmt.Printf("Sender %v\n", ctx.Sender())
			ctx.Respond(&shared.HelloRequest{
				Name: "iam response",
			})
		} else {
			fmt.Printf("No sender\n")
		}
	}
}
