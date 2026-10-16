//go:build integration

package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

type nativeJSONRequest struct {
	resource string
	method   string
	path     string
	body     any
}

func nativeJSON(t *testing.T, adapter *Adapter, options nativeJSONRequest) (int, []byte) {
	t.Helper()
	var body []byte
	if options.body != nil {
		var err error
		body, err = json.Marshal(options.body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequest(options.method, "http://ignored.invalid"+options.path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	native, err := weirclient.NewHTTPNativeRequest(options.resource, request)
	if err != nil {
		t.Fatal(err)
	}
	end, capture := runNative(t, adapter, native, body)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
		t.Fatal("Native HTTP request incomplete", options.path, end)
	}
	metadata := nativeMetadata(t, capture.head)
	return metadata.StatusCode, capture.body.Bytes()
}

type nativeSearchPage struct {
	ScrollID string `json:"_scroll_id"`
	PITID    string `json:"pit_id"`
	Hits     struct {
		Hits []struct {
			ID   string            `json:"_id"`
			Sort []json.RawMessage `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
}

func TestSearchNativeScrollPaginationAndCallerCleanup(t *testing.T) {
	adapter, backend := setupSearch(t)
	for i := range 5 {
		status, raw := backend.Do(t, http.MethodPut, fmt.Sprintf("/%s/_doc/%d", backend.Index, i), fmt.Sprintf(`{"n":%d}`, i))
		if status != http.StatusCreated {
			t.Fatal("seed scroll", status, string(raw))
		}
	}
	status, raw := backend.Do(t, http.MethodPost, "/"+backend.Index+"/_refresh", "")
	if status != http.StatusOK {
		t.Fatal(status, string(raw))
	}
	openBody := map[string]any{"size": 2, "sort": []string{"_doc"}}
	open := nativeJSONRequest{resource: backend.Index, method: http.MethodPost, path: "/_search?scroll=1m", body: openBody}
	status, raw = nativeJSON(t, adapter, open)
	var page nativeSearchPage
	if status != http.StatusOK || json.Unmarshal(raw, &page) != nil || page.ScrollID == "" {
		t.Fatal("Native scroll open", status, string(raw))
	}
	cursor := page.ScrollID
	closed := false
	t.Cleanup(func() {
		if closed {
			return
		}
		body := map[string]any{"scroll_id": []string{cursor}}
		request := nativeJSONRequest{method: http.MethodDelete, path: "/_search/scroll", body: body}
		status, raw := nativeJSON(t, adapter, request)
		if status != http.StatusOK {
			t.Error("caller scroll cleanup", status, string(raw))
		}
	})
	seen := make(map[string]bool)
	for step := 0; ; step++ {
		if step > 4 {
			t.Fatal("Native scroll failed to exhaust", seen)
		}
		for _, hit := range page.Hits.Hits {
			if hit.ID == "" || seen[hit.ID] {
				t.Fatal("Native scroll duplicated or lost hit identity", hit.ID)
			}
			seen[hit.ID] = true
		}
		if len(page.Hits.Hits) == 0 {
			break
		}
		body := map[string]any{"scroll": "1m", "scroll_id": cursor}
		request := nativeJSONRequest{method: http.MethodPost, path: "/_search/scroll", body: body}
		status, raw = nativeJSON(t, adapter, request)
		page = nativeSearchPage{}
		if status != http.StatusOK || json.Unmarshal(raw, &page) != nil || page.ScrollID == "" {
			t.Fatal("Native scroll continuation", status, string(raw))
		}
		cursor = page.ScrollID
	}
	if len(seen) != 5 {
		t.Fatal("Native scroll omitted records", seen)
	}
	releaseBody := map[string]any{"scroll_id": []string{cursor}}
	release := nativeJSONRequest{method: http.MethodDelete, path: "/_search/scroll", body: releaseBody}
	status, raw = nativeJSON(t, adapter, release)
	if status != http.StatusOK && status != http.StatusNotFound {
		t.Fatal("exhausted Native scroll cleanup", status, string(raw))
	}
	closed = true
	// A caller can abandon a fresh cursor and explicitly reclaim it.
	status, raw = nativeJSON(t, adapter, open)
	page = nativeSearchPage{}
	if status != http.StatusOK || json.Unmarshal(raw, &page) != nil || len(page.Hits.Hits) != 2 || page.ScrollID == "" {
		t.Fatal("Native early-stop scroll open", status, string(raw))
	}
	cursor = page.ScrollID
	closed = false
	body := map[string]any{"scroll_id": []string{cursor}}
	cleanup := nativeJSONRequest{method: http.MethodDelete, path: "/_search/scroll", body: body}
	status, raw = nativeJSON(t, adapter, cleanup)
	var cleared struct {
		Succeeded bool `json:"succeeded"`
		Freed     int  `json:"num_freed"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &cleared) != nil || !cleared.Succeeded || cleared.Freed < 1 {
		t.Fatal("caller did not reclaim Native scroll", status, string(raw))
	}
	closed = true
	body = map[string]any{"scroll": "1m", "scroll_id": cursor}
	continuation := nativeJSONRequest{method: http.MethodPost, path: "/_search/scroll", body: body}
	status, raw = nativeJSON(t, adapter, continuation)
	if status != http.StatusNotFound {
		t.Fatal("Native concealed expired scroll error", status, string(raw))
	}
}

func TestSearchNativePITPaginationAndCallerCleanup(t *testing.T) {
	adapter, backend := setupSearch(t)
	for i := range 5 {
		status, raw := backend.Do(t, http.MethodPut, fmt.Sprintf("/%s/_doc/%d", backend.Index, i), fmt.Sprintf(`{"n":%d}`, i))
		if status != http.StatusCreated {
			t.Fatal("seed PIT", status, string(raw))
		}
	}
	status, raw := backend.Do(t, http.MethodPost, "/"+backend.Index+"/_refresh", "")
	if status != http.StatusOK {
		t.Fatal(status, string(raw))
	}
	openPath, cleanupPath := "/_pit?keep_alive=1m", "/_pit"
	if adapter.dialect == OpenSearchProduct {
		openPath, cleanupPath = "/_search/point_in_time?keep_alive=1m", "/_search/point_in_time"
	}
	open := nativeJSONRequest{resource: backend.Index, method: http.MethodPost, path: openPath}
	status, raw = nativeJSON(t, adapter, open)
	var opened struct {
		ID    string `json:"id"`
		PITID string `json:"pit_id"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &opened) != nil {
		t.Fatal("Native PIT open", status, string(raw))
	}
	cursor := opened.ID
	if cursor == "" {
		cursor = opened.PITID
	}
	if cursor == "" {
		t.Fatal("Native PIT identity missing", string(raw))
	}
	closed := false
	cleanupBody := func() map[string]any {
		body := map[string]any{"id": cursor}
		if adapter.dialect == OpenSearchProduct {
			body = map[string]any{"pit_id": []string{cursor}}
		}
		return body
	}
	t.Cleanup(func() {
		if closed {
			return
		}
		request := nativeJSONRequest{method: http.MethodDelete, path: cleanupPath, body: cleanupBody()}
		status, raw := nativeJSON(t, adapter, request)
		if status != http.StatusOK {
			t.Error("caller PIT cleanup", status, string(raw))
		}
	})
	seen := make(map[string]bool)
	var after []json.RawMessage
	for step := 0; ; step++ {
		if step > 4 {
			t.Fatal("Native PIT search_after failed to exhaust", seen)
		}
		pit := map[string]any{"id": cursor, "keep_alive": "1m"}
		body := map[string]any{"size": 2, "pit": pit, "sort": []string{"n"}}
		if len(after) != 0 {
			body["search_after"] = after
		}
		request := nativeJSONRequest{method: http.MethodPost, path: "/_search", body: body}
		status, raw = nativeJSON(t, adapter, request)
		var page nativeSearchPage
		if status != http.StatusOK || json.Unmarshal(raw, &page) != nil {
			t.Fatal("Native PIT pagination", status, string(raw))
		}
		if page.PITID != "" {
			cursor = page.PITID
		}
		if len(page.Hits.Hits) == 0 {
			break
		}
		for _, hit := range page.Hits.Hits {
			if hit.ID == "" || seen[hit.ID] || len(hit.Sort) == 0 {
				t.Fatal("Native PIT hit identity or sort changed", hit.ID, hit.Sort)
			}
			seen[hit.ID] = true
			after = hit.Sort
		}
	}
	if len(seen) != 5 {
		t.Fatal("Native PIT omitted records", seen)
	}
	cleanup := nativeJSONRequest{method: http.MethodDelete, path: cleanupPath, body: cleanupBody()}
	status, raw = nativeJSON(t, adapter, cleanup)
	if status != http.StatusOK {
		t.Fatal("Native PIT caller cleanup", status, string(raw))
	}
	var freed struct {
		Succeeded bool `json:"succeeded"`
		PITs      []struct {
			Successful bool `json:"successful"`
		} `json:"pits"`
	}
	if json.Unmarshal(raw, &freed) != nil || (!freed.Succeeded && (len(freed.PITs) != 1 || !freed.PITs[0].Successful)) {
		t.Fatal("Native PIT cleanup acknowledgement changed", string(raw))
	}
	closed = true
	pit := map[string]any{"id": cursor, "keep_alive": "1m"}
	body := map[string]any{"size": 1, "pit": pit, "sort": []string{"n"}}
	request := nativeJSONRequest{method: http.MethodPost, path: "/_search", body: body}
	status, raw = nativeJSON(t, adapter, request)
	if status != http.StatusNotFound {
		t.Fatal("Native concealed reclaimed PIT error", status, string(raw))
	}
}
