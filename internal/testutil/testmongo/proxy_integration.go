//go:build integration

package testmongo

import "testing"

func StartProxy(t *testing.T, fixture *Fixture) *Proxy {
	t.Helper()
	opts := proxyOptions{uri: fixture.URI, serverTLS: fixture.serverTLS, clientTLS: fixture.clientTLS}
	return startProxy(t, opts)
}
