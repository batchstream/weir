package directory

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (d *Directory) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(d, ch) }

func (d *Directory) Collect(ch chan<- prometheus.Metric) {
	d.mu.Lock()
	live := 0
	now := time.Now()
	for _, owned := range d.records {
		if !owned.announcement.Withdrawn && now.Before(owned.expires) {
			live++
		}
	}
	records := len(d.records)
	d.mu.Unlock()
	values := map[string]int{"live_nodes": live, "retained_nodes": records}
	for name, value := range values {
		desc := prometheus.NewDesc("weir_directory_"+name, "Bounded owner announcements, including expired replay watermarks.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(value))
	}
}
