//go:build integration

package app

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func searchBudgetPut(root, id string) *pb.MutateRequest {
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
	action := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: root + "/s:" + id, Action: action}
	return request
}
