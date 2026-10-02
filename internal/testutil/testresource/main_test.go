package main

import (
	"strings"
	"testing"
)

func TestIOCountersKeepDevicesSeparate(t *testing.T) {
	raw := "259:0 rbytes=1 wbytes=2 rios=3 wios=4\n259:1 rbytes=5 wbytes=6 rios=7 wios=8\n"
	values, err := ioCounters(raw)
	if err != nil || len(values) != 2 || values["259:0"]["wbytes"] != 2 || values["259:1"]["wios"] != 8 {
		t.Fatal(values, err)
	}
	values, err = ioCounters("")
	if err != nil || len(values) != 0 {
		t.Fatal("empty idle cgroup is valid", values, err)
	}
}

func TestIOCountersRejectAmbiguousAndUnboundedEvidence(t *testing.T) {
	for _, raw := range []string{
		"259:0 wbytes=1 wbytes=2",
		"259:0 wbytes=1\n259:0 wbytes=2",
		"259:0 wbytes=-1",
		"259:0 invalid",
		"259:0 " + strings.Repeat("x=1 ", 17),
	} {
		if _, err := ioCounters(raw); err == nil {
			t.Fatal("accepted malformed IO counters", raw)
		}
	}
}
