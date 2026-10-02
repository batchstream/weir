//go:build integration

package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func searchFinitePage(t *testing.T, adapter *Adapter, request *pb.ScanRequest) ([]*pb.Document, *pb.ScanEnd) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	defer adapter.closeScan(ctx, work)
	var documents []*pb.Document
	var end *pb.ScanEnd
	emit := func(_ *execution.Plan, event *pb.Event) error {
		if document := event.GetDocument(); document != nil {
			documents = append(documents, document)
		}
		if value := event.GetScanEnd(); value != nil {
			end = value
		}
		return nil
	}
	for step := uint64(0); step <= uint64(request.PageSize)+2; step++ {
		adapter.streamScan(ctx, work, emit)
		if !work.Continue {
			return documents, end
		}
	}
	t.Fatal("finite page did not end")
	return nil, nil
}

func TestSearchScanCheckpointSurvivesLostReplyAndTerminalReplay(t *testing.T) {
	for _, mode := range []string{"drop", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			first, backend := setupSearch(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for i := 0; i < 4; i++ {
				backend.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", backend.Index, i), `{"n":1}`)
			}
			backend.Do(t, "POST", "/"+backend.Index+"/_refresh", "")
			request := &pb.ScanRequest{Resource: "weir://search/" + backend.Index, PageSize: 2}
			documents, end := searchFinitePage(t, first, request)
			if end.Failure != nil || len(documents) != 2 || end.Exhausted || len(end.NextContinuationToken) == 0 {
				t.Fatal("initial page", end, len(documents))
			}
			config := first.config
			_ = first.Close()
			request.ContinuationToken = end.NextContinuationToken
			fault := scanProxyFault{target: "fetch", mode: mode, at: 1}
			endpoint, _ := scanFaultProxy(t, backend, fault)
			faultConfig := config
			faultConfig.URL = endpoint
			failing, err := Open(ctx, faultConfig)
			if err != nil {
				t.Fatal(err)
			}
			failedDocuments, failed := searchFinitePage(t, failing, request)
			_ = failing.Close()
			if failed.Failure == nil || len(failedDocuments) != 0 || failed.Exhausted || len(failed.NextContinuationToken) != 0 {
				t.Fatal("lost or failed page exposed checkpoint", failed)
			}
			retry, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			again, resumed := searchFinitePage(t, retry, request)
			_ = retry.Close()
			if resumed.Failure != nil || len(again) != 2 || resumed.Exhausted {
				t.Fatal("committed checkpoint was destroyed on failure", resumed)
			}
			replay, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			repeated, repeatEnd := searchFinitePage(t, replay, request)
			_ = replay.Close()
			if repeatEnd.Failure != nil || len(repeated) != len(again) {
				t.Fatal("nonterminal replay", repeatEnd)
			}
			for i := range again {
				if !bytes.Equal(again[i].Data, repeated[i].Data) {
					t.Fatal("native PIT page changed on replay")
				}
			}
			request.ContinuationToken = resumed.NextContinuationToken
			for attempt := 0; attempt < 2; attempt++ {
				final, err := Open(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				last, finalEnd := searchFinitePage(t, final, request)
				_ = final.Close()
				if finalEnd.Failure != nil || !finalEnd.Exhausted || len(last) != 0 || len(finalEnd.NextContinuationToken) != 0 {
					t.Fatal("terminal page did not remain replayable", attempt, finalEnd)
				}
			}
			stale, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer stale.Close()
			work, failure := stale.prepareScan(request)
			if failure != nil {
				t.Fatal(failure)
			}
			checkpoint := work.Backend.(*scanPlan)
			endpoint = "/_pit"
			body := map[string]any{"id": checkpoint.pit}
			if backend.Product == OpenSearchProduct {
				endpoint = "/_search/point_in_time"
				body = map[string]any{"pit_id": []string{checkpoint.pit}}
			}
			raw, _ := json.Marshal(body)
			status, _ := backend.Do(t, "DELETE", endpoint, string(raw))
			if status != 200 {
				t.Fatal("failed to invalidate owned PIT", status)
			}
			expiredDocuments, expired := searchFinitePage(t, stale, request)
			if expired.Failure == nil || len(expiredDocuments) != 0 || expired.Exhausted || len(expired.NextContinuationToken) != 0 {
				t.Fatal("expired PIT was silently restarted", expired)
			}
		})
	}
}
