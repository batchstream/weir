// Package testmetrics provides standard Prometheus parsing for offline and opted-in tests.
package testmetrics

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"net/http/httptest"
)

func Parse(t *testing.T, input io.Reader) map[string]*dto.MetricFamily {
	t.Helper()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(input)
	if err != nil {
		t.Fatal(err)
	}
	return families
}
func Gather(t *testing.T, collector prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(collector); err != nil {
		t.Fatal(err)
	}
	return Registry(t, registry)
}
func Registry(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
	opts := promhttp.HandlerOpts{DisableCompression: true}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(registry, opts).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatal(recorder.Code, recorder.Body.String())
	}
	return Parse(t, bytes.NewReader(recorder.Body.Bytes()))
}
func Scrape(t *testing.T, address string) map[string]*dto.MetricFamily {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(response.StatusCode)
	}
	return Parse(t, response.Body)
}
func Sample(families map[string]*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	for _, metric := range families[name].GetMetric() {
		match := true
		for key, value := range labels {
			found := false
			for _, label := range metric.Label {
				if label.GetName() == key && label.GetValue() == value {
					found = true
				}
			}
			if !found {
				match = false
			}
		}
		if match {
			return metric
		}
	}
	return nil
}
func Sum(families map[string]*dto.MetricFamily, name string) float64 {
	var sum float64
	for _, metric := range families[name].GetMetric() {
		sum += metric.GetCounter().GetValue() + metric.GetGauge().GetValue()
	}
	return sum
}
func Series(families map[string]*dto.MetricFamily) int {
	count := 0
	for _, family := range families {
		for _, metric := range family.Metric {
			if h := metric.Histogram; h != nil {
				count += len(h.Bucket) + 2
			} else {
				count++
			}
		}
	}
	return count
}

func ScrapeCollector(t *testing.T, collector prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(collector); err != nil {
		t.Fatal(err)
	}
	opts := promhttp.HandlerOpts{DisableCompression: true}
	server := httptest.NewServer(promhttp.HandlerFor(registry, opts))
	defer server.Close()
	return Scrape(t, server.Listener.Addr().String())
}
