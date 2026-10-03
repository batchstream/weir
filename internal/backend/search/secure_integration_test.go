//go:build integration

package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testdns"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

func secureConfig(b *testsearch.Backend) Config {
	connection := &Connection{Username: b.Username, Password: b.Password, CAFile: b.CAFile}
	cfg := Config{Store: "search", URL: b.URL, Pool: 4, Connection: connection}
	return cfg
}
func secureAdapter(t *testing.T, b *testsearch.Backend) *Adapter {
	t.Helper()
	cfg := secureConfig(b)
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(); _ = a.Close() })
	return a
}
func TestSecureSearchProductionOpen(t *testing.T) {
	fixture := testsearch.OpenSecure(t)
	a := secureAdapter(t, fixture.Backend)
	assertOutcome(t, runSearch(t, a, searchPlan(t, a, "put", searchResource(fixture.Backend.Index, "minimal"))), pb.MutationOutcome_APPLIED, 0)
	result := runSearch(t, a, searchPlan(t, a, "read", searchResource(fixture.Backend.Index, "minimal")))
	if result.GetRead().GetDocument() == nil {
		t.Fatal("production read after write failed", result)
	}
}

func TestSecureSearchQualification(t *testing.T) {
	fixture := testsearch.OpenSecure(t)
	b := fixture.Backend
	a := secureAdapter(t, b)
	t.Run("native-database-DNS-SNI", func(t *testing.T) {
		dns := testdns.Start(t)
		answer := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
		dns.Set("search.test", answer)
		_, port, _ := net.SplitHostPort(strings.TrimPrefix(b.URL, "https://"))
		cfg := secureConfig(b)
		cfg.URL = "https://search.test:" + port
		cfg.Resolver = dns.Resolver()
		a, err := Open(context.Background(), cfg)
		if err != nil {
			t.Fatal("native DB DNS/SAN connection", err)
		}
		defer a.Close()
		call := exchange{path: "/", limit: metadataLimit}
		if status, _, err := a.request(context.Background(), call); err != nil || status != 200 {
			t.Fatal("native DNS read")
		}
		if dns.A.Load() == 0 || dns.AAAA.Load() == 0 {
			t.Fatal("expected real DNS A/AAAA")
		}
		t.Log("native database Open and independent read through search.test with standard SAN/SNI and owned loopback DNS")
	})
	t.Run("direct-operations", func(t *testing.T) {
		for _, action := range []string{"put", "replace", "delete", "create"} {
			assertOutcome(t, runSearch(t, a, searchPlan(t, a, action, searchResource(b.Index, "record"))), pb.MutationOutcome_APPLIED, 0)
		}
		assertOutcome(t, runSearch(t, a, searchPlan(t, a, "create", searchResource(b.Index, "record"))), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
		source := runSearch(t, a, searchPlan(t, a, "read", searchResource(b.Index, "record"))).GetRead().GetDocument().GetData()
		if !strings.Contains(string(source), "9223372036854775807") {
			t.Fatal("int64 source changed")
		}
		works := []*execution.Plan{searchPlan(t, a, "put", searchResource(b.Index, "bulk-a")), searchPlan(t, a, "put", searchResource(b.Index, "bulk-b"))}
		replies, _ := a.executeRecords(context.Background(), works)
		if len(replies) != 2 {
			t.Fatal("bulk association")
		}
		for _, reply := range replies {
			assertOutcome(t, reply, pb.MutationOutcome_APPLIED, 0)
		}
		assertOutcome(t, runSearch(t, a, searchPlan(t, a, "put", searchResource(b.Index, "counter"))), pb.MutationOutcome_APPLIED, 0)
		assertOutcome(t, runSearch(t, a, searchExpression(t, a, b.Index, `{"doc":{"n":9007199254740993}}`)), pb.MutationOutcome_APPLIED, 0)
		source = runSearch(t, a, searchPlan(t, a, "read", searchResource(b.Index, "counter"))).GetRead().GetDocument().GetData()
		if !strings.Contains(string(source), "9007199254740993") || !strings.Contains(string(source), `"keep":"source"`) {
			t.Fatal("expression changed unrelated source")
		}
		open := nativeOpen(t, b.Index, "POST", "/_bulk")
		body := "{\"index\":{\"_id\":\"native\"}}\n{\"n\":9007199254740993}\n"
		end, capture := runNative(t, a, open, io.NopCloser(strings.NewReader(body)))
		if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || !strings.Contains(capture.body.String(), `"errors":false`) {
			t.Fatal("native bulk", end, capture.body.String())
		}
		fixture.Admin.Do(t, "POST", "/"+b.Index+"/_refresh", "")
		scan := scanWork(t, a, b.Index)
		count := 0
		exhausted := false
		for step := 0; step < 12; step++ {
			page, _ := a.fetchScan(context.Background(), scan)
			if page.Failure != nil {
				t.Fatal("secure PIT", page.Failure)
			}
			count += len(page.Documents)
			if page.Exhausted {
				exhausted = true
				break
			}
		}
		if !exhausted || count != 5 {
			t.Fatal("PIT completeness", count, exhausted)
		}
		if failure := a.closeScan(context.Background(), scan); failure != nil {
			t.Fatal(failure)
		}
		t.Log("direct native TLS application account: Read CRUD/conflict Bulk PIT/Scan Native BackendExpression correctness passed")
	})
	t.Run("actual-401-and-403", func(t *testing.T) {
		wrong := *b
		wrong.Password = "wrong-generated-test-value"
		status, _ := wrong.Do(t, "GET", "/", "")
		if status != 401 {
			t.Fatal("expected actual native 401", status)
		}
		cfg := secureConfig(&wrong)
		rejected, err := Open(context.Background(), cfg)
		if err == nil || rejected != nil || strings.Contains(err.Error(), wrong.Password) {
			t.Fatal("authentication preflight/redaction")
		}
		denied := secureAdapter(t, fixture.Denied)
		work := searchPlan(t, denied, "put", searchResource(b.Index, "forbidden"))
		result := runSearch(t, denied, work)
		if result.GetMutation().Outcome == pb.MutationOutcome_APPLIED {
			t.Fatal("reader wrote")
		}
		code, _ := fixture.Denied.Do(t, "POST", "/_bulk", "{\"index\":{\"_index\":\""+b.Index+"\",\"_id\":\"forbidden\"}}\n{\"n\":1}\n")
		if code != 403 {
			t.Fatal("expected native 403", code)
		}
		code, _ = fixture.Admin.Do(t, "GET", "/"+b.Index+"/_doc/forbidden", "")
		if code != 404 {
			t.Fatal("denied write took effect")
		}
		t.Log("actual 401 and 403 from native security; bad pair rejects Open, restricted reader cannot mutate")
	})
	t.Run("commit-then-drain", func(t *testing.T) { secureReplyFault(t, fixture, "ordinary", "drain") })
	for _, operation := range []string{"ordinary", "bulk", "expression", "native"} {
		for _, fault := range []string{"drop", "truncate", "malformed", "401", "403", "stale"} {
			t.Run(operation+"-"+fault, func(t *testing.T) { secureReplyFault(t, fixture, operation, fault) })
		}
	}
}

func secureReplyFault(t *testing.T, fixture *testsearch.SecureFixture, operation, fault string) {
	t.Helper()
	b := fixture.Backend
	id := operation + "-" + fault
	if operation == "expression" {
		fixture.Admin.Do(t, "PUT", "/"+b.Index+"/_doc/"+id, `{"n":1}`)
	}
	committed := make(chan struct{}, 1)
	var commands atomic.Int32
	var completed atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		business := strings.HasSuffix(r.URL.Path, "/_bulk") || strings.Contains(r.URL.Path, "/_update/")
		request, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error("fault request construction")
			return
		}
		request.Header = r.Header.Clone()
		request.GetBody = nil
		if business && fault == "401" {
			request.SetBasicAuth(b.Username, "wrong-generated-value")
		}
		if business && fault == "403" {
			request.SetBasicAuth(fixture.Denied.Username, fixture.Denied.Password)
		}
		if business {
			commands.Add(1)
		}
		response, err := b.Client.Do(request)
		if err != nil {
			t.Error("native TLS forwarding failed")
			w.WriteHeader(502)
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			t.Error("native response incomplete")
			return
		}
		if business {
			expected := 200
			if fault == "401" {
				expected = 401
			}
			if fault == "403" {
				expected = 403
			}
			if response.StatusCode != expected {
				t.Error("unexpected native business status", response.StatusCode, expected)
			}
			if response.StatusCode == 200 {
				completed.Add(1)
			}
			committed <- struct{}{}
			switch fault {
			case "drain":
				<-r.Context().Done()
				return
			case "drop":
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
				return
			case "truncate":
				w.Header().Set("Content-Length", fmt.Sprint(len(raw)+10))
				raw = raw[:len(raw)/2]
			case "malformed":
				raw = []byte(`{"malformed":`)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(raw)
	})
	proxy, connection := tlsEndpoint(t, handler, false)
	connection.Username = b.Username
	connection.Password = b.Password
	cfg := secureConfig(b)
	cfg.URL = proxy.URL
	cfg.Connection = connection
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if fault == "stale" {
		proxy.CloseClientConnections()
	}
	switch operation {
	case "ordinary", "bulk":
		works := []*execution.Plan{searchPlan(t, a, "put", searchResource(b.Index, id))}
		if operation == "bulk" {
			works = append(works, searchPlan(t, a, "put", searchResource(b.Index, id+"-second")))
		}
		var replies []*pb.Result
		if fault == "drain" {
			limits := store.DefaultLimits()
			owner, err := store.New(a, limits)
			if err != nil {
				t.Fatal(err)
			}
			ticket, failure, _ := owner.Submit(context.Background(), works[0], nil)
			if failure != nil {
				t.Fatal(failure)
			}
			select {
			case <-committed:
			case <-time.After(time.Second):
				t.Fatal("write was not independently acknowledged")
			}
			snap := owner.Snapshot()
			if snap.Active != 1 || snap.Retained != 1 || snap.ResultBytes > limits.ResultBytes {
				t.Fatal("pre-drain ledger", snap)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			start := time.Now()
			_ = owner.Close(ctx)
			cancel()
			reply, waitErr := ticket.Wait(context.Background())
			if waitErr != nil {
				t.Fatal(waitErr)
			}
			ticket.Ack()
			replies = []*pb.Result{reply}
			snap = owner.Snapshot()
			if snap.Active != 0 || snap.Retained != 0 || snap.Pending != 0 || len(a.dialer.slots) != 0 || time.Since(start) > time.Second {
				t.Fatal("drain leaked ledger/socket", snap)
			}
			t.Logf("confirmed commit then drain=%s; all ledger/socket counts zero", time.Since(start))
		} else {
			replies, _ = a.executeRecords(context.Background(), works)
		}
		for _, reply := range replies {
			if fault == "stale" {
				assertOutcome(t, reply, pb.MutationOutcome_APPLIED, 0)
			} else {
				assertOutcome(t, reply, pb.MutationOutcome_UNKNOWN, pb.FailureCode_UNAVAILABLE)
			}
		}
	case "expression":
		op := expressionOperation("weir://search/"+b.Index+"/s:"+id, `{"doc":{"n":2}}`)
		p, f := prepareTestRecord(a, op)
		if f != nil {
			t.Fatal(f)
		}
		reply := runSearch(t, a, p)
		if fault == "stale" {
			assertOutcome(t, reply, pb.MutationOutcome_APPLIED, 0)
		} else {
			assertOutcome(t, reply, pb.MutationOutcome_UNKNOWN, pb.FailureCode_UNAVAILABLE)
		}
	case "native":
		open := nativeOpen(t, b.Index, "POST", "/_bulk")
		body := "{\"index\":{\"_id\":\"" + id + "\"}}\n{\"n\":2}\n"
		end, _ := runNative(t, a, open, io.NopCloser(strings.NewReader(body)))
		want := pb.NativeCompletion_RESPONSE_INCOMPLETE
		// Native preserves a complete native byte response; it does not parse JSON.
		if fault == "malformed" || fault == "401" || fault == "403" || fault == "stale" {
			want = pb.NativeCompletion_RESPONSE_COMPLETE
		}
		if end.Completion != want {
			t.Fatal("native completion evidence", end)
		}
	}
	wantCompleted := int32(1)
	denied := fault == "401" || fault == "403"
	if denied {
		wantCompleted = 0
	}
	if commands.Load() != 1 || completed.Load() != wantCompleted {
		t.Fatal("business request replay or missing commit", commands.Load(), completed.Load())
	}
	status, raw := fixture.Admin.Do(t, "GET", "/"+b.Index+"/_doc/"+id, "")
	var got struct {
		Version int             `json:"_version"`
		Source  json.RawMessage `json:"_source"`
	}
	want := 1
	if operation == "expression" {
		want = 2
	}
	if denied {
		if operation == "expression" {
			want = 1
		} else {
			want = 0
		}
	}
	if want == 0 {
		if status != 404 {
			t.Fatal("denied operation changed data")
		}
	} else if status != 200 || json.Unmarshal(raw, &got) != nil || got.Version != want {
		t.Fatal("independent version evidence disagrees", status, got.Version, want)
	}
	// Recovery is a new independent read; no old mutation is retried.
	call := exchange{path: "/", limit: metadataLimit}
	if status, _, err := a.request(context.Background(), call); fault != "drain" && (err != nil || status != 200) {
		t.Fatal("new independent read did not recover")
	}
	time.Sleep(10 * time.Millisecond)
	if commands.Load() != 1 {
		t.Fatal("late implicit replay")
	}
	t.Logf("%s/%s backend completed=%d forwarded business requests=1 version=%d; independent state verified", operation, fault, completed.Load(), got.Version)
}
