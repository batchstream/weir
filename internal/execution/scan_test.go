package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestScanPublicationCountsAcrossPages(t *testing.T) {
	var progress ScanProgress
	work := &Plan{}
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{}`)}
	page := &ScanPage{Documents: []*pb.Document{document, document}}
	var end *pb.ScanEnd
	documents := 0
	emit := func(plan *Plan, event *pb.Event) error {
		if plan != work {
			t.Fatal("publication lost plan association")
		}
		if event.GetDocument() != nil {
			documents++
		} else {
			end = event.GetScanEnd()
		}
		return nil
	}
	transferred := progress.PublishPage(context.Background(), work, page, emit)
	if transferred || !work.Continue || progress.Count != 2 || documents != 2 || end != nil {
		t.Fatal("nonterminal page ended or lost successful documents", transferred, work.Continue, progress.Count, documents, end)
	}
	token := []byte("checkpoint")
	page = &ScanPage{Documents: []*pb.Document{document}, Complete: true, NextContinuationToken: token}
	transferred = progress.PublishPage(context.Background(), work, page, emit)
	if !transferred || work.Continue || progress.Count != 3 || documents != 3 || end.GetDocumentCount() != 3 || end.GetFailure() != nil || end.GetExhausted() || string(end.GetNextContinuationToken()) != string(token) {
		t.Fatal("logical page completion lost count or checkpoint", transferred, work.Continue, progress.Count, documents, end)
	}
}

func TestScanPublicationFailureStopsPageAndHidesCheckpoint(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "publication_error"
		if canceled {
			name = "caller_canceled"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var progress ScanProgress
			work := &Plan{Continue: true}
			document := &pb.Document{Data: []byte(`{}`)}
			page := &ScanPage{Documents: []*pb.Document{document, document, document}, Complete: true, NextContinuationToken: []byte("checkpoint")}
			var end *pb.ScanEnd
			attempts := 0
			emit := func(_ *Plan, event *pb.Event) error {
				if event.GetDocument() != nil {
					attempts++
					if attempts == 2 {
						if canceled {
							cancel()
						}
						return errors.New("consumer stopped")
					}
				} else {
					end = event.GetScanEnd()
				}
				return nil
			}
			transferred := progress.PublishPage(ctx, work, page, emit)
			code := pb.FailureCode_INTERNAL
			if canceled {
				code = pb.FailureCode_CANCELLED
			}
			if transferred || work.Continue || attempts != 2 || progress.Count != 1 || end.GetDocumentCount() != 1 || end.GetFailure().GetCode() != code || end.GetExhausted() || len(end.GetNextContinuationToken()) != 0 {
				t.Fatal("failed publication continued or exposed unaccepted checkpoint", transferred, work.Continue, attempts, progress.Count, end)
			}
		})
	}
}

func TestScanPublicationRequiresAcceptedTerminalEventToTransferCheckpoint(t *testing.T) {
	var progress ScanProgress
	work := &Plan{Continue: true}
	document := &pb.Document{Data: []byte(`{}`)}
	page := &ScanPage{Documents: []*pb.Document{document}, Complete: true, NextContinuationToken: []byte("checkpoint")}
	var end *pb.ScanEnd
	emit := func(_ *Plan, event *pb.Event) error {
		if event.GetScanEnd() != nil {
			end = event.GetScanEnd()
			return errors.New("terminal event rejected")
		}
		return nil
	}
	transferred := progress.PublishPage(context.Background(), work, page, emit)
	if transferred || work.Continue || progress.Count != 1 || end.GetDocumentCount() != 1 || len(end.GetNextContinuationToken()) == 0 {
		t.Fatal("unaccepted terminal event transferred checkpoint ownership", transferred, work.Continue, progress.Count, end)
	}
}

func TestScanPublicationTerminatesBackendFailureOrExhaustion(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "exhausted"
		if failed {
			name = "backend_failure"
		}
		t.Run(name, func(t *testing.T) {
			progress := ScanProgress{Count: 4}
			work := &Plan{Continue: true}
			page := &ScanPage{Exhausted: true}
			if failed {
				page.Failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend failed")
				page.NextContinuationToken = []byte("unusable checkpoint")
			}
			var end *pb.ScanEnd
			emit := func(_ *Plan, event *pb.Event) error {
				end = event.GetScanEnd()
				return nil
			}
			transferred := progress.PublishPage(context.Background(), work, page, emit)
			if transferred || work.Continue || end.GetDocumentCount() != 4 || end.GetExhausted() == failed || len(end.GetNextContinuationToken()) != 0 || end.GetFailure() != page.Failure {
				t.Fatal("backend terminal state changed", transferred, work.Continue, end)
			}
		})
	}
}
