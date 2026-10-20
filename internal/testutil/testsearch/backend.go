//go:build integration

// Package testsearch owns only unique test indexes on fixed loopback test backends.
package testsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var sequence atomic.Uint64

type Backend struct {
	URL, Product, Index string
	// Profile records the fixed fixture version, never runtime admission policy.
	Profile                    string
	Username, Password, CAFile string
	Client                     *http.Client
}

func Open(t *testing.T) *Backend {
	t.Helper()
	var endpoint, profile string
	product := os.Getenv("WEIR_SEARCH_INTEGRATION")
	switch product {
	case "elasticsearch":
		endpoint, profile = "http://127.0.0.1:19200", "elasticsearch-8.19.22"
	case "opensearch":
		endpoint, profile = "http://127.0.0.1:19202", "opensearch-3.3.0"
	case "opensearch-2.19":
		endpoint, profile, product = "http://127.0.0.1:19201", "opensearch-2.19.6", "opensearch"
	case "":
		t.Skip("Search real-backend suite requires explicit WEIR_SEARCH_INTEGRATION=elasticsearch|opensearch")
	default:
		t.Fatal("invalid search integration profile")
	}
	transport := &http.Transport{Proxy: nil, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, DisableCompression: true}
	client := &http.Client{
		Transport:     transport,
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	index := fmt.Sprintf("weir_search_%d_%d", os.Getpid(), sequence.Add(1))
	b := &Backend{
		URL:     endpoint,
		Product: product,
		Profile: profile,
		Index:   index,
		Client:  client,
	}
	status, raw := b.Do(t, "GET", "/", "")
	if status != 200 {
		t.Fatal("wrong isolated backend", status)
	}
	cluster := "weir-local-" + product
	if profile == "opensearch-3.3.0" {
		cluster = "weir-local-opensearch-3.3"
	}
	verifyVersion(t, raw, profile, cluster)
	t.Cleanup(transport.CloseIdleConnections)
	b.Create(t, index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"n":{"type":"long"}}}}`)
	return b
}

type OwnedEndpoint struct {
	URL, Owner, Product string
	TLSDirectory        string
}

// OpenOwned is only for the native controller's product-explicit fresh backend.
// Open's historical fixed-address opt-ins and identities are unchanged.
func OpenOwned(t *testing.T, opts OwnedEndpoint) *Backend {
	t.Helper()
	profile, err := ownedProfile(opts.Product)
	if err != nil {
		t.Fatal(err)
	}
	scheme := "http"
	if opts.TLSDirectory != "" {
		scheme = "https"
	}
	u, err := url.Parse(opts.URL)
	if err != nil || u.Scheme != scheme || u.Hostname() != "127.0.0.1" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		t.Fatal("owned backend requires its selected loopback transport")
	}
	_, port, err := net.SplitHostPort(u.Host)
	n, parseErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || n < 1 || n > 65535 || !regexp.MustCompile(`^weir-finite-[0-9a-f]{32}$`).MatchString(opts.Owner) {
		t.Fatal("invalid finite endpoint/owner")
	}
	transport := &http.Transport{Proxy: nil, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	b := &Backend{URL: opts.URL, Product: opts.Product, Profile: profile, Index: "weir_finite_" + strings.TrimPrefix(opts.Owner, "weir-finite-"), Client: client}
	if opts.TLSDirectory != "" {
		_, trust, material := NativeTLS(t, opts.TLSDirectory)
		transport.TLSClientConfig = trust
		b.Username, b.Password = material.Username, material.Password
		b.CAFile = filepath.Join(opts.TLSDirectory, "ca.pem")
	}
	status, raw := b.Do(t, "GET", "/", "")
	if status != http.StatusOK {
		t.Fatal("owned backend identity status", status)
	}
	verifyVersion(t, raw, b.Profile, opts.Owner)
	t.Cleanup(transport.CloseIdleConnections)
	b.Create(t, b.Index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"n":{"type":"long"}}}}`)
	return b
}

func ownedProfile(product string) (string, error) {
	switch product {
	case "elasticsearch":
		return "elasticsearch-8.19.22", nil
	case "opensearch":
		return "opensearch-3.3.0", nil
	default:
		return "", fmt.Errorf("missing or unsupported owned search product")
	}
}

func verifyVersion(t *testing.T, raw []byte, profile, cluster string) {
	t.Helper()
	var root struct {
		ClusterName string `json:"cluster_name"`
		Version     struct {
			Number, Distribution string
			BuildFlavor          string `json:"build_flavor"`
			BuildHash            string `json:"build_hash"`
		}
	}
	if json.Unmarshal(raw, &root) != nil || root.ClusterName != cluster || root.Version.BuildHash == "" {
		t.Fatal("wrong isolated backend identity")
	}
	v := root.Version
	valid := profile == "elasticsearch-8.19.22" && v.Number == "8.19.22" && v.Distribution == "" && v.BuildFlavor == "default"
	valid = valid || profile == "opensearch-2.19.6" && v.Number == "2.19.6" && v.Distribution == "opensearch"
	valid = valid || profile == "opensearch-3.3.0" && v.Number == "3.3.0" && v.Distribution == "opensearch"
	if !valid {
		t.Fatal("wrong isolated backend version/distribution")
	}
	t.Logf("actual backend profile=%s build=%s", profile, v.BuildHash)
}

func (b *Backend) Create(t *testing.T, index, body string) {
	t.Helper()
	if !strings.HasPrefix(index, b.Index) {
		t.Fatal("refusing non-owned index")
	}
	status, raw := b.Do(t, "PUT", "/"+index, body)
	if status != 200 {
		t.Fatalf("test index setup status=%d body=%s", status, raw)
	}
	t.Cleanup(func() {
		status, _ := b.Do(t, "DELETE", "/"+index, "")
		if status != 200 && status != 404 {
			t.Error("test index cleanup failed", status)
		}
	})
}

func (b *Backend) Do(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, b.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.GetBody = nil
	if b.Username != "" {
		request.SetBasicAuth(b.Username, b.Password)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := b.Client.Do(request)
	if err != nil {
		t.Fatal("independent backend request failed")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, raw
}
