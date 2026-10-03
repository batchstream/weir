//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"time"
)

// Each logical operation is sent once. An absent transport result stays UNKNOWN;
// direct GET is evidence only for these isolated, uniquely named fixture records.
func load(ctx context.Context, client pb.StoreServiceClient, prefix string) error {
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("%s-%03d", prefix, i)
		if err := mutation(ctx, client, id); err != nil {
			return err
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func mutation(ctx context.Context, client pb.StoreServiceClient, id string) error {
	call, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	request := put(id)
	result, err := testutil.ExecuteRecord(call, client, testutil.RecordCommand(request))
	reply := result.GetMutation()
	outcome := "UNKNOWN"
	if err == nil {
		outcome = reply.GetOutcome().String()
	}
	fmt.Printf("operation id=%s outcome=%s failure=%s rpc=%v\n", id, outcome, reply.GetFailure().GetCode(), err)
	if err == nil && reply.GetOutcome() == pb.MutationOutcome_MUTATION_OUTCOME_UNSPECIFIED {
		return errors.New("missing outcome")
	}
	return verifyEffect(ctx, id, outcome)
}

func verifyEffect(ctx context.Context, id, outcome string) error {
	code, raw, err := admin(ctx, "GET", "/records/_doc/"+id, "")
	if err != nil {
		return err
	}
	var record struct {
		Version int  `json:"_version"`
		Found   bool `json:"found"`
	}
	if json.Unmarshal(raw, &record) != nil || (code != 200 && code != 404) {
		return fmt.Errorf("invalid readback %d %s", code, raw)
	}
	if record.Found && record.Version != 1 {
		return fmt.Errorf("replay id=%s version=%d", id, record.Version)
	}
	if outcome == "APPLIED" && !record.Found {
		return errors.New("false APPLIED")
	}
	if (outcome == "NOT_STARTED" || outcome == "NOT_APPLIED") && record.Found {
		return errors.New("false non-application")
	}
	fmt.Printf("readback id=%s found=%t version=%d client=%s\n", id, record.Found, record.Version, outcome)
	return nil
}

func hold(ctx context.Context, client pb.StoreServiceClient, prefix string) error {
	for first := 0; first < 40; first += 2 {
		requests := make([]*pb.MutateRequest, 0, 2)
		for i := first; i < first+2; i++ {
			id := fmt.Sprintf("%s-%03d", prefix, i)
			fixture := testutil.RecordCommand(put(id))
			requests = append(requests, fixture.Operation.GetMutate())
		}
		batch := &pb.MutateBatchRequest{StoreName: "records", Requests: requests}
		response, err := client.Mutate(ctx, batch)
		if err != nil || len(response.GetResults()) != len(requests) {
			return fmt.Errorf("hold batch: %v %v", response, err)
		}
		for i, result := range response.Results {
			if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
				return fmt.Errorf("hold result[%d]: %v", first+i, result)
			}
			id := fmt.Sprintf("%s-%03d", prefix, first+i)
			if err := persisted(ctx, id); err != nil {
				return err
			}
			fmt.Printf("HOLD index=%d APPLIED\n", first+i)
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fmt.Println("HOLD all 40 ordered mutation results and final RPC OK")
	return nil
}
