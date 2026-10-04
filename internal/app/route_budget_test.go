package app

import (
	"strings"
	"testing"
)

func TestProcessBudgetCoversDeclaredStoreResources(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Basic.Memory = ByteSize(cfg.ReservedMemory())
	if err := cfg.Validate(); err != nil {
		t.Fatal("exact declared envelope rejected", err)
	}
	cfg.Basic.Memory--
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "cannot cover") {
		t.Fatal("underfunded process accepted", err)
	}
	cfg.Basic.Memory = 1 << 30
	cfg.Basic.Transport.MaxSessions = 64
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "cannot cover") {
		t.Fatal("active RPC budget did not scale with session cap", err)
	}
}
