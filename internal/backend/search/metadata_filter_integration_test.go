//go:build integration

package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestSearchMetadataFilterPreservesQualificationEvidence(t *testing.T) {
	adapter, backend := setupSearch(t)
	cases := []struct {
		name, mapping, settings, alias               string
		unsupported, source, write, native, writable bool
	}{
		{name: "standard", source: true, write: true, native: true, writable: true},
		{name: "source_disabled", mapping: `"_source":{"enabled":false}`, native: true, writable: true},
		{name: "source_pruned", mapping: `"_source":{"excludes":["private"]}`, native: true, writable: true},
		{name: "required_routing", mapping: `"_routing":{"required":true}`, unsupported: true},
		{name: "final_pipeline", settings: `,"final_pipeline":"qualification-final"`, source: true, writable: true},
		{name: "default_pipeline", settings: `,"default_pipeline":"qualification-default"`, source: true, write: true, writable: true},
		{name: "plain_alias", alias: `{}`, source: true, write: true, native: true, writable: true},
		{name: "filtered_alias", alias: `{"filter":{"term":{"private":"value"}}}`, unsupported: true},
		{name: "routed_alias", alias: `{"routing":"route"}`, unsupported: true},
		{name: "read_alias", alias: `{"is_write_index":false}`, source: true, write: true, native: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			index := backend.Index + "_" + test.name
			var properties strings.Builder
			for i := range 128 {
				if i != 0 {
					properties.WriteByte(',')
				}
				fmt.Fprintf(&properties, `"field_%d":{"type":"keyword"}`, i)
			}
			mapping := test.mapping
			if mapping != "" {
				mapping += ","
			}
			body := fmt.Sprintf(`{"settings":{"number_of_shards":1,"number_of_replicas":0%s},"mappings":{%s"properties":{%s}}}`, test.settings, mapping, properties.String())
			backend.Create(t, index, body)
			target := index
			if test.alias != "" {
				target += "_alias"
				var add map[string]any
				if err := json.Unmarshal([]byte(test.alias), &add); err != nil {
					t.Fatal(err)
				}
				add["index"], add["alias"] = index, target
				action := map[string]any{"add": add}
				actions := []map[string]any{action}
				request := map[string]any{"actions": actions}
				encoded, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				status, raw := backend.Do(t, "POST", "/_aliases", string(encoded))
				if status != http.StatusOK {
					t.Fatal("create owned alias", status, string(raw))
				}
			}
			status, full := backend.Do(t, "GET", "/"+target+"?flat_settings=true", "")
			if status != http.StatusOK || !strings.Contains(string(full), `"properties"`) {
				t.Fatal("full metadata fixture missing field mappings", status, string(full))
			}
			captured := make(chan []byte, 1)
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				forward, err := http.NewRequestWithContext(request.Context(), request.Method, backend.URL+request.URL.RequestURI(), request.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				response, err := backend.Client.Do(forward)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				defer response.Body.Close()
				raw, err := io.ReadAll(response.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				if request.URL.Path == "/"+target {
					captured <- raw
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(raw)
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			config := adapter.config
			config.URL = proxy.URL
			through, err := Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer through.Close(context.Background())
			caps, failure := through.inspect(t.Context(), target, false)
			if test.unsupported {
				if failure.GetCode() != pb.FailureCode_UNSUPPORTED {
					t.Fatal("filtered metadata allowed an unsupported target", caps, failure)
				}
			} else if failure != nil || caps.source != test.source || caps.write != test.write || caps.nativeWrite != test.native || caps.writable != test.writable || caps.index != index {
				t.Fatal("filtered metadata changed target capabilities", caps, failure)
			}
			var filtered []byte
			select {
			case filtered = <-captured:
			default:
				t.Fatal("qualification did not request fresh metadata")
			}
			if len(filtered) >= len(full) || strings.Contains(string(filtered), `"properties"`) {
				t.Fatal("unused field mappings remain in the response", len(full), len(filtered))
			}
			var before, after map[string]map[string]any
			if json.Unmarshal(full, &before) != nil || json.Unmarshal(filtered, &after) != nil {
				t.Fatal("invalid real metadata response")
			}
			for _, controls := range before {
				delete(controls["mappings"].(map[string]any), "properties")
				if len(controls["mappings"].(map[string]any)) == 0 {
					delete(controls, "mappings")
				}
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("response filtering changed qualification controls", before, after)
			}
			t.Logf("metadata bytes full=%d filtered=%d", len(full), len(filtered))
		})
	}
	t.Run("missing_index", func(t *testing.T) {
		_, failure := adapter.inspect(t.Context(), backend.Index+"_missing", false)
		if failure.GetCode() != pb.FailureCode_TARGET_NOT_FOUND {
			t.Fatal("response filtering lost the native error classification", failure)
		}
	})
}
