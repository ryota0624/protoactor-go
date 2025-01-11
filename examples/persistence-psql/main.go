package main

import (
	context2 "context"
	"fmt"
	"github.com/asynkron/protoactor-go/persistence/postgresql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"log"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/descriptorpb"
)

type protoMsg struct {
	state string
	set   bool
	value string
}

func (p *protoMsg) Reset()         {}
func (p *protoMsg) String() string { return p.state }
func (p *protoMsg) ProtoMessage()  {}

type (
	Message  struct{ protoMsg }
	Snapshot struct{ protoMsg }
)

func (m *protoMsg) ProtoReflect() protoreflect.Message { return (*message)(m) }

type message protoMsg

type messageType struct{}

func (messageType) New() protoreflect.Message                  { return &message{} }
func (messageType) Zero() protoreflect.Message                 { return (*message)(nil) }
func (messageType) Descriptor() protoreflect.MessageDescriptor { return fileDesc.Messages().Get(0) }

func (m *message) New() protoreflect.Message                  { return &message{} }
func (m *message) Descriptor() protoreflect.MessageDescriptor { return fileDesc.Messages().Get(0) }
func (m *message) Type() protoreflect.MessageType             { return messageType{} }
func (m *message) Interface() protoreflect.ProtoMessage       { return (*protoMsg)(m) }
func (m *message) ProtoMethods() *protoiface.Methods          { return nil }

var fieldDescS = fileDesc.Messages().Get(0).Fields().Get(0)

func (m *message) Range(f func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {
	if m.set {
		f(fieldDescS, protoreflect.ValueOf(m.value))
	}
}

func (m *message) Has(fd protoreflect.FieldDescriptor) bool {
	if fd == fieldDescS {
		return m.set
	}
	panic("invalid field descriptor")
}

func (m *message) Clear(fd protoreflect.FieldDescriptor) {
	if fd == fieldDescS {
		m.value = ""
		m.set = false
		return
	}
	panic("invalid field descriptor")
}

func (m *message) Get(fd protoreflect.FieldDescriptor) protoreflect.Value {
	if fd == fieldDescS {
		return protoreflect.ValueOf(m.value)
	}
	panic("invalid field descriptor")
}

func (m *message) Set(fd protoreflect.FieldDescriptor, v protoreflect.Value) {
	if fd == fieldDescS {
		m.value = v.String()
		m.set = true
		return
	}
	panic("invalid field descriptor")
}

func (m *message) Mutable(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("invalid field descriptor")
}

func (m *message) NewField(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("invalid field descriptor")
}

func (m *message) WhichOneof(protoreflect.OneofDescriptor) protoreflect.FieldDescriptor {
	panic("invalid oneof descriptor")
}

func (m *message) GetUnknown() protoreflect.RawFields { return nil }

// func (m *message) SetUnknown(protoreflect.RawFields)  { return }
func (m *message) SetUnknown(protoreflect.RawFields) {}

func (m *message) IsValid() bool {
	return m != nil
}

var fileDesc = func() protoreflect.FileDescriptor {
	p := &descriptorpb.FileDescriptorProto{}
	if err := prototext.Unmarshal([]byte(descriptorText), p); err != nil {
		panic(err)
	}
	file, err := protodesc.NewFile(p, nil)
	if err != nil {
		panic(err)
	}
	return file
}()

const descriptorText = `
  name: "internal/testprotos/irregular/irregular.proto"
  package: "goproto.proto.thirdparty"
  message_type {
    name: "IrregularMessage"
    field {
      name: "s"
      number: 1
      label: LABEL_OPTIONAL
      type: TYPE_STRING
      json_name: "s"
    }
  }
  options {
    go_package: "google.golang.org/protobuf/internal/testprotos/irregular"
  }
`

type AberrantMessage int

func (m AberrantMessage) ProtoMessage()            {}
func (m AberrantMessage) Reset()                   {}
func (m AberrantMessage) String() string           { return "" }
func (m AberrantMessage) Marshal() ([]byte, error) { return nil, nil }
func (m AberrantMessage) Unmarshal([]byte) error   { return nil }

type Actor struct {
	persistence.Mixin
	state string
}

func (a *Actor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		log.Println("actor started")
	case *persistence.RequestSnapshot:
		log.Printf("snapshot internal state '%v'", a.state)
		a.PersistSnapshot(&Snapshot{protoMsg: protoMsg{state: a.state}})
	case *Snapshot:
		a.state = msg.state
		log.Printf("recovered from snapshot, internal state changed to '%v'", a.state)
	case *persistence.ReplayComplete:
		log.Printf("replay completed, internal state changed to '%v'", a.state)
	case *Message:
		scenario := "received replayed event"
		if !a.Recovering() {
			a.PersistReceive(msg)
			scenario = "received new message"
		}
		a.state = msg.state
		log.Printf("%s, internal state changed to '%v'\n", scenario, a.state)
	}
}

func main() {
	ctx := context2.Background()
	dbName := "users"
	dbUser := "user"
	dbPassword := "password"

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)

	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}

	connString := postgresContainer.MustConnectionString(ctx, "sslmode=disable", "application_name=test")

	err = prepareTables(ctx, connString)
	if err != nil {
		log.Printf("failed to prepare tables: %s", err)
		return
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Printf("failed to parse connection string: %s", err)
		return
	}
	system := actor.NewActorSystem()
	provider := postgresql.NewProvider(config, system)

	rootContext := system.Root
	props := actor.PropsFromProducer(func() actor.Actor { return &Actor{} },
		actor.WithReceiverMiddleware(persistence.Using(provider)))
	pid, _ := rootContext.SpawnNamed(props, "persistent")
	rootContext.Send(pid, &Message{protoMsg: protoMsg{state: "state4"}})
	rootContext.Send(pid, &Message{protoMsg: protoMsg{state: "state5"}})

	rootContext.PoisonFuture(pid).Wait()
	fmt.Printf("*** restart ***\n")
	pid, _ = rootContext.SpawnNamed(props, "persistent")

	_, _ = console.ReadLine()
}

func prepareTables(ctx context2.Context, connString string) error {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return err
	}

	_, err = pool.Exec(context2.Background(), `
create table if not exists event_journals
(
    actor_name   varchar(255) not null,
    event_index  bigint       not null,
    event        json         not null,
    message_type varchar(255) not null,
    primary key (actor_name, event_index)
);
	`)
	if err != nil {
		return err
	}

	_, err = pool.Exec(context2.Background(), `
create table if not exists snapshots
(
    actor_name        varchar(255) not null,
    snapshot_index    bigint       not null,
    snapshot          json         not null,
    message_type varchar(255) not null,
    primary key (id)
);
	`)
	return err
}
