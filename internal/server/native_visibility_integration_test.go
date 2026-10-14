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
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

type sdkVisibleSearchOptions struct {
	backend *testsearch.Backend
	wantN   int
}

func assertSDKVisibleSearch(t *testing.T, client pb.StoreServiceClient, opts sdkVisibleSearchOptions) {
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
	options := weirclient.NativeOptions{StoreName: "records", Request: native}
	options.Consume = func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if observed.Len()+len(chunk) > 64<<10 {
			return errors.New("fixture search response exceeded its bound")
		}
		_, err := observed.Write(chunk)
		return err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := weirclient.Native(ctx, client, options)
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
	config := search.Config{Store: "records", URL: backend.URL}
	adapter, err := search.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	nodes, client := routeBackendServer(t, adapter)
	resource := backend.Index + "/" + weirclient.EncodeSegment("s:sdk")
	document := &weirclient.Document{ContentType: "application/json", Data: []byte(`{"n":1,"business":"kept"}`)}
	mutationOptions := &weirclient.MutationOptions{Visibility: weirclient.MutationSearchVisible}
	put := &weirclient.WriteRequest{Resource: resource, Document: document, Options: mutationOptions}
	writeOptions := weirclient.WriteOptions{StoreName: "records", Request: put}
	result, err := weirclient.Put(ctx, client, writeOptions)
	if err != nil || result == nil || result.Outcome != weirclient.MutationApplied || result.Failure != nil {
		t.Fatal("SDK visible Put acknowledgement", err, result)
	}
	searchOptions := sdkVisibleSearchOptions{backend: backend, wantN: 1}
	assertSDKVisibleSearch(t, client, searchOptions)
	program := &weirclient.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
	transform := &weirclient.AtomicTransformRequest{Resource: resource, Lua: program, Options: mutationOptions}
	transformOptions := weirclient.AtomicTransformOptions{StoreName: "records", Request: transform}
	result, err = weirclient.AtomicTransform(ctx, client, transformOptions)
	if err != nil || result == nil || result.Outcome != weirclient.MutationApplied || result.Failure != nil {
		t.Fatal("SDK visible Lua acknowledgement", err, result)
	}
	searchOptions.wantN = 2
	assertSDKVisibleSearch(t, client, searchOptions)
	assertRouteAcceptanceIdle(t, nodes)
}
