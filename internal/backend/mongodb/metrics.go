package mongodb

import "github.com/prometheus/client_golang/prometheus"

func (a *Adapter) Describe(ch chan<- *prometheus.Desc) { a.lua.Describe(ch) }

func (a *Adapter) Collect(ch chan<- prometheus.Metric) { a.lua.Collect(ch) }
