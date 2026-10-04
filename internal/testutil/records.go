package testutil

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

type RecordFixture struct {
	StoreName string
	Command   *pb.Command
}

func RecordRequest(store string, message proto.Message) RecordFixture {
	command := &pb.Command{}
	switch request := message.(type) {
	case *pb.ReadRequest:
		operation := &pb.Command_Read{Read: request}
		command.Operation = operation
	case *pb.MutateRequest:
		operation := &pb.Command_Mutate{Mutate: request}
		command.Operation = operation
	default:
		panic("record fixture requires a read or mutation request")
	}
	fixture := RecordFixture{StoreName: store, Command: command}
	return fixture
}

func NativeCommand(open *pb.NativeOpen, body []byte) *pb.Command {
	request := &pb.NativeRequest{Open: open, Body: body}
	operation := &pb.Command_Native{Native: request}
	command := &pb.Command{Operation: operation}
	return command
}
