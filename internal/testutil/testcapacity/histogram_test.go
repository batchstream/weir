package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestHistogramMergeBounds(t *testing.T) {
	var a, b Histogram
	for _, d := range []time.Duration{
		time.Microsecond,
		5 * time.Millisecond,
		100 * time.Millisecond,
		250 * time.Millisecond,
		2*time.Second + 1,
	} {
		a.Add(d)
		b.Add(d)
	}
	a.Merge(b)
	if a.Percentile(99) != -1 || a.MaxNS != int64(2*time.Second+1) {
		t.Fatal(a)
	}
	var c Histogram
	c.Add(100 * time.Millisecond)
	if c.Percentile(95) != 100000 {
		t.Fatal(c)
	}
	c.Add(100*time.Millisecond + 1)
	if c.Percentile(99) != 101000 {
		t.Fatal(c)
	}
	raw, err := json.Marshal(a)
	if err != nil || !strings.Contains(string(raw), `"upper_us":-1`) {
		t.Fatal(string(raw), err)
	}
}

func TestHistogramWeightedMerge(t *testing.T) {
	var fast, slow Histogram
	for i := 0; i < 999; i++ {
		fast.Add(time.Millisecond)
	}
	slow.Add(time.Second)
	fast.Merge(slow)
	if fast.Percentile(99) != 1000 || fast.MaxNS != int64(time.Second) {
		t.Fatal(fast)
	}
}
