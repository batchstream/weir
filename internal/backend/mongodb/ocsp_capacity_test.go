package mongodb

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"
)

func TestMongoOCSPHeadersCrossDefaultHTTPTransportCapacity(t *testing.T) {
	var payload []byte
	header := strings.Repeat("x", 11<<20)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Large-Metadata", header)
		_, _ = writer.Write(payload)
	})
	responder := httptest.NewServer(handler)
	defer responder.Close()
	options := certificateOptions{responders: []string{responder.URL}}
	certificate := newTestCertificate(t, options)
	response := ocsp.Response{Status: ocsp.Good, ThisUpdate: time.Now().Add(-time.Minute), NextUpdate: time.Now().Add(time.Hour)}
	payload = testOCSP(t, certificate, response)
	address := tlsWireServer(t, certificate.config, nil)
	owner := newConnectionOwner()
	owner.tlsConfig = &tls.Config{RootCAs: certificate.roots}
	defer owner.close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	connection, err := owner.DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal("valid OCSP response hit the default HTTP header quota", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}
