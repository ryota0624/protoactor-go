package main

import (
	context2 "context"
	"fmt"
	"github.com/asynkron/protoactor-go/persistence/postgresql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/reflect/protoregistry"
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

type Message struct {
	state string
	set   bool
	value string
}

func (m *Message) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, m.state)), nil
}

func (m *Message) UnmarshalJSON(bytes []byte) error {
	m.state = string(bytes)
	m.set = true
	return nil
}

func (m *Message) Zero() protoreflect.Message {
	return &Message{}
}

func (m *Message) Reset()         {}
func (m *Message) String() string { return m.state }
func (m *Message) ProtoMessage()  {}

type Snapshot struct {
	state string
	set   bool
	value string
}

func (s *Snapshot) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, s.state)), nil
}

func (s *Snapshot) UnmarshalJSON(bytes []byte) error {
	s.state = string(bytes)
	s.set = true
	return nil
}

func (s *Snapshot) Zero() protoreflect.Message {
	return &Snapshot{}
}

func (s *Snapshot) Reset()         {}
func (s *Snapshot) String() string { return s.state }
func (s *Snapshot) ProtoMessage()  {}

func (m *Message) ProtoReflect() protoreflect.Message  { return m }
func (s *Snapshot) ProtoReflect() protoreflect.Message { return s }

type messageType struct{}

func (messageType) New() protoreflect.Message  { return &Message{} }
func (messageType) Zero() protoreflect.Message { return (*Message)(nil) }
func (messageType) Descriptor() protoreflect.MessageDescriptor {
	return fileDescForMessage.Messages().Get(0)
}

type snapshotType struct{}

func (snapshotType) New() protoreflect.Message  { return &Snapshot{} }
func (snapshotType) Zero() protoreflect.Message { return (*Snapshot)(nil) }
func (snapshotType) Descriptor() protoreflect.MessageDescriptor {
	return fileDescForSnapshot.Messages().Get(0)
}

func (m *Message) New() protoreflect.Message { return &Message{} }
func (m *Message) Descriptor() protoreflect.MessageDescriptor {
	return fileDescForMessage.Messages().Get(0)
}
func (m *Message) Type() protoreflect.MessageType       { return messageType{} }
func (m *Message) Interface() protoreflect.ProtoMessage { return m }
func (m *Message) ProtoMethods() *protoiface.Methods    { return nil }

func (s *Snapshot) New() protoreflect.Message { return &Snapshot{} }
func (s *Snapshot) Descriptor() protoreflect.MessageDescriptor {
	return fileDescForSnapshot.Messages().Get(0)
}
func (s *Snapshot) Type() protoreflect.MessageType       { return snapshotType{} }
func (s *Snapshot) Interface() protoreflect.ProtoMessage { return s }
func (s *Snapshot) ProtoMethods() *protoiface.Methods    { return nil }

var fieldDescSForMessage = fileDescForMessage.Messages().Get(0).Fields().Get(0)

func (m *Message) Range(f func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {
	if m.set {
		f(fieldDescSForMessage, protoreflect.ValueOf(m.value))
	}
}

func (m *Message) Has(fd protoreflect.FieldDescriptor) bool {
	if fd == fieldDescSForMessage {
		return m.set
	}
	panic("invalid field descriptor")
}

func (m *Message) Clear(fd protoreflect.FieldDescriptor) {
	if fd == fieldDescSForMessage {
		m.value = ""
		m.set = false
		return
	}
	panic("invalid field descriptor")
}

func (m *Message) Get(fd protoreflect.FieldDescriptor) protoreflect.Value {
	if fd == fieldDescSForMessage {
		return protoreflect.ValueOf(m.value)
	}
	panic("invalid field descriptor")
}

func (m *Message) Set(fd protoreflect.FieldDescriptor, v protoreflect.Value) {
	if fd == fieldDescSForMessage {
		m.value = v.String()
		m.set = true
		return
	}
	panic("invalid field descriptor")
}

func (m *Message) Mutable(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("invalid field descriptor")
}

func (m *Message) NewField(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("invalid field descriptor")
}

func (m *Message) WhichOneof(protoreflect.OneofDescriptor) protoreflect.FieldDescriptor {
	panic("invalid oneof descriptor")
}

func (m *Message) GetUnknown() protoreflect.RawFields { return nil }

// func (m *message) SetUnknown(protoreflect.RawFields)  { return }
func (m *Message) SetUnknown(protoreflect.RawFields) {}

func (m *Message) IsValid() bool {
	return m != nil
}

var fileDescForMessage = func() protoreflect.FileDescriptor {
	p := &descriptorpb.FileDescriptorProto{}
	if err := prototext.Unmarshal([]byte(descriptorTextForMessage), p); err != nil {
		panic(err)
	}
	file, err := protodesc.NewFile(p, nil)
	if err != nil {
		panic(err)
	}
	return file
}()

const descriptorTextForMessage = `
  name: "internal/testprotos/irregular/irregular.proto"
  package: "goproto.proto.thirdparty"
  message_type {
    name: "Message"
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

var fieldDescSForSnapshot = fileDescForSnapshot.Messages().Get(0).Fields().Get(0)

func (s *Snapshot) Range(f func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {
	if s.set {
		f(fieldDescSForSnapshot, protoreflect.ValueOf(s.value))
	}
}

func (s *Snapshot) Has(fd protoreflect.FieldDescriptor) bool {
	if fd == fieldDescSForSnapshot {
		return s.set
	}
	panic("invalid field descriptor")
}

func (s *Snapshot) Clear(fd protoreflect.FieldDescriptor) {
	if fd == fieldDescSForSnapshot {
		s.value = ""
		s.set = false
		return
	}
	panic("invalid field descriptor")
}

func (s *Snapshot) Get(fd protoreflect.FieldDescriptor) protoreflect.Value {
	if fd == fieldDescSForSnapshot {
		return protoreflect.ValueOf(s.value)
	}
	panic("invalid field descriptor")
}

func (s *Snapshot) Set(fd protoreflect.FieldDescriptor, v protoreflect.Value) {
	if fd == fieldDescSForSnapshot {
		s.value = v.String()
		s.set = true
		return
	}
	panic("invalid field descriptor")
}

func (s *Snapshot) Mutable(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("invalid field descriptor")
}

func (s *Snapshot) NewField(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("invalid field descriptor")
}

func (s *Snapshot) WhichOneof(protoreflect.OneofDescriptor) protoreflect.FieldDescriptor {
	panic("invalid oneof descriptor")
}

func (s *Snapshot) GetUnknown() protoreflect.RawFields { return nil }

func (s *Snapshot) SetUnknown(protoreflect.RawFields) {}

func (s *Snapshot) IsValid() bool {
	return s != nil
}

var fileDescForSnapshot = func() protoreflect.FileDescriptor {
	p := &descriptorpb.FileDescriptorProto{}
	if err := prototext.Unmarshal([]byte(descriptorTextForSnapshot), p); err != nil {
		panic(err)
	}
	file, err := protodesc.NewFile(p, nil)
	if err != nil {
		panic(err)
	}
	return file
}()

const descriptorTextForSnapshot = `
  name: "internal/testprotos/irregular/irregular.proto"
  package: "goproto.proto.thirdparty"
  message_type {
    name: "Snapshot"
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
		a.PersistSnapshot(&Snapshot{state: a.state})
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
	case *postgresql.PersistenceErrorOccurred:
		log.Printf("PersistenceErrorOccurred: %s", msg.PersistenceError)
	}
}

func main() {
	if err := protoregistry.GlobalTypes.RegisterMessage(&Message{}); err != nil {
		panic(err)
	}
	if err := protoregistry.GlobalTypes.RegisterMessage(&Snapshot{}); err != nil {
		panic(err)
	}
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
	pool, err := pgxpool.NewWithConfig(ctx, config)
	system := actor.NewActorSystem()
	provider := postgresql.NewProvider(pool, system)

	rootContext := system.Root
	props := actor.PropsFromProducer(func() actor.Actor { return &Actor{} },
		actor.WithReceiverMiddleware(postgresql.HandlePersistenceError(), persistence.Using(provider)))
	pid, _ := rootContext.SpawnNamed(props, "persistent")
	rootContext.Send(pid, &Message{state: "state4"})
	rootContext.Send(pid, &Message{state: "state5"})
	if result, err := rootContext.RequestFuture(pid, &Message{state: "state6"}, time.Second*1).Result(); err != nil {
		fmt.Printf("request error err=%v\n", err)
	} else {
		fmt.Printf("response=%v\n", result)
	}

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
    actor_name     varchar(255) not null,
    snapshot_index bigint       not null,
    snapshot       json         not null,
    message_type   varchar(255) not null,
    constraint snapshots_pk primary key (actor_name)
);
	`)
	return err
}
