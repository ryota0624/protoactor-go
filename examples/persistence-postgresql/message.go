package main

import (
	"fmt"
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
