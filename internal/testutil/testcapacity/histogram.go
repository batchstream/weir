package main

import (
	"encoding/json"
	"time"
)

// Upper bounds: 100us through 10ms, 1ms through 1s, 10ms through 2s,
// then an explicit overflow bucket. Counts, never percentiles, are merged.
type Histogram struct {
	Counts [1191]uint64
	MaxNS  int64
}
type Bucket struct {
	UpperUS int64  `json:"upper_us"`
	Count   uint64 `json:"count"`
}

func upper(i int) int64 {
	if i < 100 {
		return int64(i+1) * 100
	}
	if i < 1090 {
		return 10000 + int64(i-99)*1000
	}
	if i < 1190 {
		return 1000000 + int64(i-1089)*10000
	}
	return -1
}
func (h *Histogram) Add(d time.Duration) {
	n := d.Nanoseconds()
	if n < 0 {
		n = 0
	}
	if n > h.MaxNS {
		h.MaxNS = n
	}
	us := (n + 999) / 1000
	i := 1190
	switch {
	case us <= 10000:
		i = int((us+99)/100) - 1
	case us <= 1000000:
		i = 100 + int((us-10000+999)/1000) - 1
	case us <= 2000000:
		i = 1090 + int((us-1000000+9999)/10000) - 1
	}
	if i < 0 {
		i = 0
	}
	h.Counts[i]++
}
func (h *Histogram) Merge(other Histogram) {
	for i, n := range other.Counts {
		h.Counts[i] += n
	}
	if other.MaxNS > h.MaxNS {
		h.MaxNS = other.MaxNS
	}
}
func (h Histogram) Percentile(p uint64) int64 {
	var total uint64
	for _, n := range h.Counts {
		total += n
	}
	if total == 0 {
		return -1
	}
	want := (total*p + 99) / 100
	var sum uint64
	for i, n := range h.Counts {
		sum += n
		if sum >= want {
			return upper(i)
		}
	}
	return -1
}
func (h Histogram) MarshalJSON() ([]byte, error) {
	buckets := make([]Bucket, 0, 64)
	for i, n := range h.Counts {
		if n > 0 {
			b := Bucket{UpperUS: upper(i), Count: n}
			buckets = append(buckets, b)
		}
	}
	value := struct {
		Buckets []Bucket `json:"buckets"`
		P50US   int64    `json:"p50_us"`
		P95US   int64    `json:"p95_us"`
		P99US   int64    `json:"p99_us"`
		MaxNS   int64    `json:"max_ns"`
	}{buckets, h.Percentile(50), h.Percentile(95), h.Percentile(99), h.MaxNS}
	return json.Marshal(value)
}
