package protocol

import (
	"bytes"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
)

func TestScanContinuationBindingAndBounds(t *testing.T) {
	selector := &pb.Document{MediaType: "application/json", Data: []byte(`{"query":{"match_all":{}}}`)}
	request := &pb.ScanRequest{Resource: "weir://search/records", Selector: selector, PageSize: 2}
	fingerprint := ScanFingerprint(request, "search:elasticsearch")
	state := []byte(`{"pit":"native","after":3}`)
	token, err := EncodeScanToken("search:elasticsearch", fingerprint, state)
	if err != nil {
		t.Fatal(err)
	}
	request.PageSize = 3
	request.ContinuationToken = token
	if ScanFingerprint(request, "search:elasticsearch") != fingerprint {
		t.Fatal("changing page size changed traversal identity")
	}
	decoded, err := DecodeScanToken(token, "search:elasticsearch", fingerprint)
	if err != nil || !bytes.Equal(decoded, state) {
		t.Fatal("continuation roundtrip", err)
	}
	for _, resource := range []string{"weir://other/records", "weir://search/other"} {
		request.Resource = resource
		if _, err := DecodeScanToken(token, "search:elasticsearch", ScanFingerprint(request, "search:elasticsearch")); err == nil {
			t.Fatal("accepted different traversal", resource)
		}
	}
	if _, err := DecodeScanToken(token, "search:opensearch", fingerprint); err == nil {
		t.Fatal("accepted another backend dialect")
	}
	corrupt := bytes.Replace(token, []byte(`"version":1`), []byte(`"version":2`), 1)
	for _, invalid := range [][]byte{nil, append(token, ' '), corrupt, []byte(strings.Repeat("x", MaxScanToken+1)), []byte(`{"version":1,"state":"broken"}`)} {
		if _, err := DecodeScanToken(invalid, "search:elasticsearch", fingerprint); err == nil {
			t.Fatal("accepted malformed continuation")
		}
	}
	if _, err := EncodeScanToken("search:elasticsearch", fingerprint, make([]byte, MaxScanState+1)); err == nil {
		t.Fatal("encoded excessive native state")
	}
}

func TestScanEndRequiresOneSuccessCheckpoint(t *testing.T) {
	failure := Fail(pb.FailureCode_UNAVAILABLE, "fixture")
	cases := []struct {
		end   *pb.ScanEnd
		valid bool
	}{
		{end: &pb.ScanEnd{Exhausted: true}, valid: true},
		{end: &pb.ScanEnd{NextContinuationToken: []byte("token")}, valid: true},
		{end: &pb.ScanEnd{Failure: failure}, valid: true},
		{end: &pb.ScanEnd{}, valid: false},
		{end: &pb.ScanEnd{Exhausted: true, NextContinuationToken: []byte("token")}, valid: false},
		{end: &pb.ScanEnd{Failure: failure, Exhausted: true}, valid: false},
		{end: &pb.ScanEnd{Failure: failure, NextContinuationToken: []byte("token")}, valid: false},
	}
	for _, test := range cases {
		variant := &pb.Event_ScanEnd{ScanEnd: test.end}
		event := &pb.Event{Version: 1, Value: variant}
		if (ValidateEvent(event) == nil) != test.valid {
			t.Fatal("invalid ScanEnd evidence accepted", test.end)
		}
	}
}
