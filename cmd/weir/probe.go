package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

const probeTimeout = 750 * time.Millisecond

// Probe has no app/configuration dependency. Each invocation owns one bounded
// HTTP/1 connection, and accepts only the exact diagnostic success response.
func runProbe(ctx context.Context, mode, address string) error {
	want := ""
	switch mode {
	case "live":
		want = "ok\n"
	case "ready":
		want = "ready\n"
	default:
		return errors.New("probe requires live or ready")
	}

	target, err := netip.ParseAddrPort(address)
	if err != nil || !target.Addr().IsLoopback() || target.Addr().Zone() != "" || target.Port() == 0 {
		return errors.New("probe requires a loopback IP and nonzero port")
	}

	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	dialer := &net.Dialer{Timeout: probeTimeout}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		MaxConnsPerHost:        1,
		MaxResponseHeaderBytes: 1024,
		ResponseHeaderTimeout:  probeTimeout,
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   probeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+target.String()+"/"+mode+"z", nil)
	if err != nil {
		return errors.New("probe request invalid")
	}

	response, err := client.Do(request)
	if err != nil {
		return errors.New("probe unavailable")
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, 65))
	if err != nil || response.StatusCode != http.StatusOK || string(body) != want {
		return errors.New("probe unhealthy")
	}
	return nil
}
