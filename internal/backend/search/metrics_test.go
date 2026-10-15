package search

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func searchMetricFamilies(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}
	return byName
}

func searchMetric(t *testing.T, family *dto.MetricFamily, labels map[string]string) *dto.Metric {
	t.Helper()
	for _, metric := range family.GetMetric() {
		if len(metric.GetLabel()) != len(labels) {
			continue
		}
		matches := true
		for _, pair := range metric.GetLabel() {
			if labels[pair.GetName()] != pair.GetValue() {
				matches = false
			}
		}
		if matches {
			return metric
		}
	}
	t.Fatalf("metric %s missing labels %v", family.GetName(), labels)
	return nil
}

func TestSearchMetricsMeasureActualRecordRequestsAndLua(t *testing.T) {
	for _, visible := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "visible"}[visible], func(t *testing.T) {
			fixture := &visibilityFixture{}
			adapter := visibilityAdapter(t, fixture)
			registry := prometheus.NewPedanticRegistry()
			if err := registry.Register(adapter); err != nil {
				t.Fatal(err)
			}
			before := searchMetricFamilies(t, registry)
			if before["weir_search_http_requests_total"] != nil {
				t.Fatal("request fabricated before dispatch")
			}
			if before["weir_search_http_request_body_bytes"] != nil {
				t.Fatal("body bytes fabricated before dispatch")
			}
			works := make([]*execution.Plan, 0, 2)
			for _, id := range []string{"first", "second"} {
				opts := visibilityPlanOptions{action: "program", id: id, wait: visible, program: `return function(current) current.n = 2; return current end`}
				works = append(works, visibilityPlan(t, adapter, opts))
			}
			for _, result := range adapter.executeRecords(t.Context(), works) {
				if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil {
					t.Fatal(result)
				}
			}
			families := searchMetricFamilies(t, registry)
			visibility := "none"
			if visible {
				visibility = "wait_for"
			}
			for _, operation := range []string{"metadata", "mget", "bulk"} {
				requestVisibility := "none"
				if operation == "bulk" {
					requestVisibility = visibility
				}
				labels := map[string]string{"operation": operation, "outcome": "success", "visibility": requestVisibility}
				counter := searchMetric(t, families["weir_search_http_requests_total"], labels)
				duration := searchMetric(t, families["weir_search_http_request_duration_seconds"], labels)
				if counter.GetCounter().GetValue() != 1 || duration.GetHistogram().GetSampleCount() != 1 || duration.GetHistogram().GetSampleSum() <= 0 {
					t.Fatal("HTTP attempt counted incorrectly", operation, counter, duration)
				}
				if operation == "metadata" {
					continue
				}
				labels = map[string]string{"operation": operation, "visibility": requestVisibility}
				records := searchMetric(t, families["weir_search_http_request_records"], labels).GetHistogram()
				if records.GetSampleCount() != 1 || records.GetSampleSum() != 2 || records.Bucket[0].GetCumulativeCount() != 0 || records.Bucket[1].GetCumulativeCount() != 1 {
					t.Fatal("record histogram used execution count instead of actual request records", operation, records)
				}
			}
			labels := map[string]string{"outcome": "success"}
			lua := searchMetric(t, families["weir_search_lua_evaluation_duration_seconds"], labels).GetHistogram()
			if lua.GetSampleCount() != 2 || lua.GetSampleSum() <= 0 {
				t.Fatal("Lua evaluation not measured once per actual call", lua)
			}
		})
	}
}

func TestSearchHTTPMetricsClassifyFailuresAndBoundLabels(t *testing.T) {
	cases := []struct {
		name, operation, outcome string
		status                   int
		body, encoding           string
	}{
		{name: "http error", operation: "mget", outcome: "http_error", status: 503, body: `{"error":"busy"}`},
		{name: "malformed json", operation: "bulk", outcome: "invalid_response", status: 200, body: `{`},
		{name: "bulk item error is HTTP success", operation: "bulk", outcome: "success", status: 200, body: `{"errors":true,"items":[]}`},
		{name: "unexpected encoding", operation: "refresh", outcome: "invalid_response", status: 200, body: `{}`, encoding: "gzip"},
		{name: "search", operation: "search", outcome: "success", status: 200, body: `{}`},
		{name: "bounded operation", operation: "caller-controlled-value", outcome: "success", status: 200, body: `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{URL: server.URL}
			adapter := &Adapter{config: config, client: server.Client(), ctx: context.Background()}
			registry := prometheus.NewPedanticRegistry()
			if err := registry.Register(adapter); err != nil {
				t.Fatal(err)
			}
			call := exchange{path: "/private-index/_bulk?refresh=wait_for", body: []byte("{}\n"), operation: tc.operation, records: 3}
			_, _, _ = adapter.request(t.Context(), call)
			families := searchMetricFamilies(t, registry)
			operation, visibility := tc.operation, "none"
			if operation == "caller-controlled-value" {
				operation = "other"
			}
			if operation == "bulk" {
				visibility = "wait_for"
			}
			labels := map[string]string{"operation": operation, "outcome": tc.outcome, "visibility": visibility}
			counter := searchMetric(t, families["weir_search_http_requests_total"], labels)
			duration := searchMetric(t, families["weir_search_http_request_duration_seconds"], labels)
			if counter.GetCounter().GetValue() != 1 || duration.GetHistogram().GetSampleCount() != 1 {
				t.Fatal("failed attempt omitted or counted twice", counter, duration)
			}
			byteLabels := map[string]string{"operation": operation, "visibility": visibility}
			bodyBytes := searchMetric(t, families["weir_search_http_request_body_bytes"], byteLabels).GetHistogram()
			if bodyBytes.GetSampleCount() != 1 || bodyBytes.GetSampleSum() != float64(len(call.body)) {
				t.Fatal("attempted body bytes omitted or counted twice", bodyBytes)
			}
			if operation == "bulk" || operation == "mget" {
				recordLabels := map[string]string{"operation": operation, "visibility": visibility}
				records := searchMetric(t, families["weir_search_http_request_records"], recordLabels).GetHistogram()
				if records.GetSampleCount() != 1 || records.GetSampleSum() != 3 {
					t.Fatal("failed request omitted from actual record counts", records)
				}
			}
			for _, family := range families {
				for _, metric := range family.GetMetric() {
					for _, label := range metric.GetLabel() {
						if strings.Contains(label.GetValue(), "private-index") || strings.Contains(label.GetValue(), "caller-controlled") {
							t.Fatal("unbounded request data leaked into a metric", label)
						}
					}
				}
			}
		})
	}
}

func TestSearchHTTPMetricsCountTransportFailure(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{}`) })
	server := httptest.NewServer(handler)
	client := server.Client()
	server.Close()
	config := Config{URL: server.URL}
	adapter := &Adapter{config: config, client: client, ctx: context.Background()}
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	call := exchange{path: "/_bulk", body: []byte("{}\n"), operation: "bulk", records: 3, mutation: true}
	_, _, err := adapter.request(t.Context(), call)
	if err != errWriteNotSent {
		t.Fatal("connection refusal did not retain unsent mutation evidence", err)
	}
	families := searchMetricFamilies(t, registry)
	labels := map[string]string{"operation": "bulk", "outcome": "transport_error", "visibility": "none"}
	counter := searchMetric(t, families["weir_search_http_requests_total"], labels)
	duration := searchMetric(t, families["weir_search_http_request_duration_seconds"], labels)
	if counter.GetCounter().GetValue() != 1 || duration.GetHistogram().GetSampleCount() != 1 {
		t.Fatal("failed connection acquisition omitted from attempts", counter, duration)
	}
	byteLabels := map[string]string{"operation": "bulk", "visibility": "none"}
	bodyBytes := searchMetric(t, families["weir_search_http_request_body_bytes"], byteLabels).GetHistogram()
	if bodyBytes.GetSampleCount() != 1 || bodyBytes.GetSampleSum() != float64(len(call.body)) {
		t.Fatal("connection failure lost attempted body size", bodyBytes)
	}
}

func TestSearchHTTPMetricsCountCancellationAfterDispatchOnly(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "timeout"}[timeout], func(t *testing.T) {
			started := make(chan struct{})
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, `{"incomplete":`)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{URL: server.URL}
			adapter := &Adapter{config: config, client: server.Client(), ctx: context.Background()}
			registry := prometheus.NewPedanticRegistry()
			if err := registry.Register(adapter); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			if timeout {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			}
			defer cancel()
			call := exchange{path: "/_bulk?refresh=wait_for", body: []byte("{}\n"), operation: "bulk", records: 4, mutation: true}
			done := make(chan error, 1)
			go func() {
				_, _, err := adapter.request(ctx, call)
				done <- err
			}()
			<-started
			if !timeout {
				cancel()
			}
			if err := <-done; err != errCanceled && err != errTimeout {
				t.Fatal("unexpected request result", err)
			}
			// A subsequent canceled mutation exits before client.Do and adds no sample.
			_, _, _ = adapter.request(ctx, call)
			outcome := "canceled"
			if timeout {
				outcome = "timeout"
			}
			families := searchMetricFamilies(t, registry)
			labels := map[string]string{"operation": "bulk", "outcome": outcome, "visibility": "wait_for"}
			counter := searchMetric(t, families["weir_search_http_requests_total"], labels)
			duration := searchMetric(t, families["weir_search_http_request_duration_seconds"], labels)
			recordLabels := map[string]string{"operation": "bulk", "visibility": "wait_for"}
			records := searchMetric(t, families["weir_search_http_request_records"], recordLabels)
			bodyBytes := searchMetric(t, families["weir_search_http_request_body_bytes"], recordLabels).GetHistogram()
			if counter.GetCounter().GetValue() != 1 || duration.GetHistogram().GetSampleCount() != 1 || records.GetHistogram().GetSampleCount() != 1 || records.GetHistogram().GetSampleSum() != 4 {
				t.Fatal("cancellation was omitted or pre-dispatch rejection was counted", counter, duration, records)
			}
			if bodyBytes.GetSampleCount() != 1 || bodyBytes.GetSampleSum() != float64(len(call.body)) {
				t.Fatal("pre-dispatch cancellation changed attempted body bytes", bodyBytes)
			}
		})
	}
}

func TestSearchMetricsConcurrentFirstUseAndCollection(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{}`) })
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{URL: server.URL}
	adapter := &Adapter{config: config, client: server.Client(), ctx: context.Background()}
	registry := prometheus.NewPedanticRegistry()
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Go(func() {
			call := exchange{path: "/records", operation: "metadata"}
			_, _, err := adapter.request(t.Context(), call)
			if err != nil {
				t.Error(err)
			}
		})
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	_ = searchMetricFamilies(t, registry)
	group.Wait()
	families := searchMetricFamilies(t, registry)
	labels := map[string]string{"operation": "metadata", "outcome": "success", "visibility": "none"}
	metric := searchMetric(t, families["weir_search_http_requests_total"], labels)
	if metric.GetCounter().GetValue() != 8 {
		t.Fatal("concurrent first calls lost samples", metric)
	}
}

func TestSearchLuaMetricsExcludeSourceDecodingAndClassifyFailures(t *testing.T) {
	adapter := &Adapter{}
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	type luaMetricCase struct {
		name, source, outcome string
		stored                []byte
		context               context.Context
	}
	cases := []luaMetricCase{
		{name: "source decoding", stored: []byte(`{"n":`), source: `return function() return {} end`, context: t.Context()},
		{name: "syntax", source: `invalid lua`, context: t.Context(), outcome: "error"},
		{name: "success", source: `return function() return {} end`, context: t.Context(), outcome: "success"},
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	canceledCase := luaMetricCase{name: "canceled", source: `return function() return {} end`, context: canceled, outcome: "canceled"}
	cases = append(cases, canceledCase)
	deadline, cancelDeadline := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	deadlineCase := canceledCase
	deadlineCase.name, deadlineCase.context, deadlineCase.outcome = "deadline", deadline, "timeout"
	cases = append(cases, deadlineCase)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			program := &luaengine.Program{Source: tc.source}
			native := &plan{program: program}
			found := len(tc.stored) != 0
			current := &getReply{Found: &found, Source: tc.stored}
			_, _ = adapter.evaluateProgram(tc.context, native, current)
			families := searchMetricFamilies(t, registry)
			if tc.outcome == "" {
				if families["weir_search_lua_evaluation_duration_seconds"] != nil {
					t.Fatal("source decode failure counted as Lua engine work")
				}
				return
			}
			labels := map[string]string{"outcome": tc.outcome}
			metric := searchMetric(t, families["weir_search_lua_evaluation_duration_seconds"], labels)
			if metric.GetHistogram().GetSampleCount() != 1 {
				t.Fatal("Lua evaluation missing or counted twice", metric)
			}
		})
	}
}
