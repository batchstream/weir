//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

type sdkVisibleSearchOptions struct {
	backend *testsearch.Backend
	wantN   int
}

func assertSDKVisibleSearch(t *testing.T, client *weirclient.Client, opts sdkVisibleSearchOptions) {
	t.Helper()
	path := "/" + opts.backend.Index + "/_search?filter_path=hits.hits._id,hits.hits._source"
	body := `{"query":{"ids":{"values":["sdk"]}}}`
	request, err := http.NewRequest(http.MethodPost, "http://ignored.invalid"+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	native, err := weirclient.NewHTTPNativeRequest("", request)
	if err != nil {
		t.Fatal("SDK Native backend root", err)
	}
	var observed bytes.Buffer
	consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if observed.Len()+len(chunk) > 64<<10 {
			return errors.New("fixture search response exceeded its bound")
		}
		_, err := observed.Write(chunk)
		return err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := client.Native(ctx, "records", native, weirclient.WithChunks(consume))
	if err != nil || result == nil || result.Completion != weirclient.NativeResponseComplete || result.Failure != nil {
		t.Fatal("SDK Native search completion", err, result)
	}
	metadata, err := weirclient.ParseHTTPNativeResponse(result.Response)
	if err != nil || metadata.StatusCode != http.StatusOK {
		t.Fatal("SDK Native changed the native HTTP status", err, metadata)
	}
	status, direct := opts.backend.Do(t, http.MethodPost, path, body)
	if status != metadata.StatusCode || !bytes.Equal(direct, observed.Bytes()) {
		t.Fatal("SDK Native changed backend status or raw search body", status, string(direct), observed.String())
	}
	var reply struct {
		Hits struct {
			Hits []struct {
				ID     string `json:"_id"`
				Source struct {
					N        int
					Business string
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if json.Unmarshal(observed.Bytes(), &reply) != nil || len(reply.Hits.Hits) != 1 || reply.Hits.Hits[0].ID != "sdk" || reply.Hits.Hits[0].Source.N != opts.wantN || reply.Hits.Hits[0].Source.Business != "kept" {
		t.Fatal("SDK mutation success did not expose exact immediate search state", opts.wantN, observed.String())
	}
}

func TestSDKExecuteSearchVisibilityAndNativeBackendRoot(t *testing.T) {
	backend := testsearch.Open(t)
	statusCode, raw := backend.Do(t, http.MethodPut, "/"+backend.Index+"/_settings", `{"index":{"refresh_interval":"100ms"}}`)
	if statusCode != http.StatusOK {
		t.Fatal("fixture refresh interval", statusCode, string(raw))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	config := search.Config{URL: backend.URL}
	adapter, err := search.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	nodes, _ := routeBackendServer(t, adapter)
	client := sdkClient(t, nodes[0].address)
	resource := backend.Index + "/" + weirclient.EncodeSegment("s:sdk")
	document := &weirclient.Document{ContentType: "application/json", Data: []byte(`{"n":1,"business":"kept"}`)}
	put := &weirclient.MutateRequest{Resource: resource, Action: weirclient.MutationPut, Document: document}
	results, err := client.Mutate(ctx, "records", weirclient.WithRequests(put), weirclient.WithQueryVisible())
	if err != nil || results[0] == nil || results[0].Outcome != weirclient.MutationApplied || results[0].Failure != nil {
		t.Fatal("SDK visible Put acknowledgement", err, results)
	}
	searchOptions := sdkVisibleSearchOptions{backend: backend, wantN: 1}
	assertSDKVisibleSearch(t, client, searchOptions)
	program := &weirclient.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
	transform := &weirclient.MutateRequest{Resource: resource, Action: weirclient.MutationAtomicTransform, Lua: program}
	results, err = client.Mutate(ctx, "records", weirclient.WithRequests(transform), weirclient.WithQueryVisible())
	if err != nil || results[0] == nil || results[0].Outcome != weirclient.MutationApplied || results[0].Failure != nil {
		t.Fatal("SDK visible Lua acknowledgement", err, results)
	}
	searchOptions.wantN = 2
	assertSDKVisibleSearch(t, client, searchOptions)
	// Shared streams outlive their calls, so the handlers return once the Client closes.
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	assertRouteAcceptanceIdle(t, nodes)
}
