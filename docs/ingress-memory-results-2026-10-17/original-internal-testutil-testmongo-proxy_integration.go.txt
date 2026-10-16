//go:build integration

package testmongo

import "testing"

type ProxyOptions struct {
	Fixture       *Fixture
	ListenAddress string
}

// StartProxyOn uses an explicit test listener, including a host interface that
// an owned Linux CLI container can reach. StartProxy retains its loopback default.
func StartProxyOn(t *testing.T, options ProxyOptions) *Proxy {
	t.Helper()
	if options.Fixture == nil || options.ListenAddress == "" {
		t.Fatal("explicit owned fixture and proxy listener required")
	}
	fixture := options.Fixture
	opts := proxyOptions{uri: fixture.URI, listenAddress: options.ListenAddress, serverTLS: fixture.serverTLS, clientTLS: fixture.clientTLS}
	return startProxy(t, opts)
}

func StartProxy(t *testing.T, fixture *Fixture) *Proxy {
	t.Helper()
	opts := proxyOptions{uri: fixture.URI, serverTLS: fixture.serverTLS, clientTLS: fixture.clientTLS}
	return startProxy(t, opts)
}
