//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminRejectsTruncatedEvidence(t *testing.T) {
	for _, size := range []int{64 << 10, (64 << 10) + 1} {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat("x", size)))
		})
		server := httptest.NewServer(handler)
		backendURL = server.URL
		code, raw, err := admin(context.Background(), "GET", "/", "")
		server.Close()
		if code != http.StatusOK {
			t.Fatal(code)
		}
		if size == 64<<10 && (err != nil || len(raw) != size) {
			t.Fatalf("exact bound: %d %v", len(raw), err)
		}
		if size > 64<<10 && (err == nil || raw != nil) {
			t.Fatal("truncated evidence silently accepted")
		}
	}
}
