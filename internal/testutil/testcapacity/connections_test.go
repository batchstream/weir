package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOwnedTCPStatesExcludesNamespacePeersAndTimeWait(t *testing.T) {
	raw := "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n" +
		"0: 0100007F:23F0 0100007F:AC01 01 00000000:00000000 00:00000000 00000000 1000 0 100\n" +
		"1: 0100007F:AC01 0100007F:23F0 01 00000000:00000000 00:00000000 00000000 1000 0 101\n" +
		"2: 0100007F:23F0 0100007F:AC02 01 00000000:00000000 00:00000000 00000000 1000 0 102\n" +
		"3: 00000000:23F0 00000000:0000 0A 00000000:00000000 00:00000000 00000000 1000 0 103\n" +
		"4: 0100007F:23F0 0100007F:AC03 06 00000000:00000000 00:00000000 00000000 1000 0 0\n" +
		"5: 0100007F:242B 0100007F:AC04 01 00000000:00000000 00:00000000 00000000 1000 0 104\n"
	owned := map[string]bool{"100": true, "101": true, "103": true, "104": true}
	states, err := ownedTCPStates(raw, owned, 9200)
	if err != nil || states["01"] != 1 || states["0A"] != 1 || states["06"] != 0 {
		t.Fatal("database owner/local-port intersection", states, err)
	}
	if len(states) != 2 {
		t.Fatal("unrelated sockets attributed to database", states)
	}
}

func TestOwnedTCPStatesIPv6AndMalformedEvidence(t *testing.T) {
	header := "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
	row := "0: 00000000000000000000000001000000:6981 00000000000000000000000001000000:A000 01 00000000:00000000 00:00000000 00000000 999 0 200\n"
	owned := map[string]bool{"200": true}
	states, err := ownedTCPStates(header+row, owned, 27009)
	if err != nil || states["01"] != 1 {
		t.Fatal(states, err)
	}
	for _, raw := range []string{"bad header", header + "short", header + strings.Replace(row, ":6981", ":FFFFF", 1)} {
		if _, err := ownedTCPStates(raw, owned, 27009); err == nil {
			t.Fatal("invalid observer evidence accepted")
		}
	}
}

func TestConnectionProbeReadOnlyFixedOfferedWorkAndClose(t *testing.T) {
	var calls, mutations atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/records/_doc/read-") {
			mutations.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		calls.Add(1)
		id := strings.TrimPrefix(r.URL.Path, "/records/_doc/")
		fmt.Fprintf(w, `{"_index":"records","_id":%q,"found":true,"_version":1,"_source":%s}`, id, payload(id))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	opts := ConnectionProbeOptions{
		Backend: server.URL, Pool: 2, Workers: 4, Rate: 24, Seconds: 1,
		StartAt: time.Now().Add(10 * time.Millisecond),
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	if err := connectionProbe(context.Background(), encoder, opts); err != nil {
		t.Fatal(err)
	}
	var report ConnectionProbeReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Planned != 24 || report.Started != 24 || report.Success != 24 || report.ClientDrop != 0 || len(report.Failures) != 0 || calls.Load() != 24 || mutations.Load() != 0 {
		t.Fatal("read-only offered work ledger", report, calls.Load(), mutations.Load())
	}
	if report.End.Before(report.Start) || report.IdleEnd.Before(report.End) || report.Closed.Before(report.IdleEnd) {
		t.Fatal("invalid connection phase times", report)
	}
}
