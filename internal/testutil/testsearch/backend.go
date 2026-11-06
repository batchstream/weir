//go:build integration

// Package testsearch owns only unique test indexes on fixed loopback test backends.
package testsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var sequence atomic.Uint64

type Backend struct {
	URL, Product, Index string
	// Profile names the fixture product and version.
	Profile string
	// Container names the scripts/search-local.sh container.
	Container                  string
	Username, Password, CAFile string
	Client                     *http.Client
}

// fixture describes one backend that scripts/search-local.sh runs.
type fixture struct {
	// product is the Search dialect Weir must detect.
	product, profile, container, url string
	// images are the pinned images by GOARCH. They match the images
	// scripts/search-local.sh pulls; the secure fixture runs them unpulled.
	images map[string]string
	// compatibility enables OpenSearch's Elasticsearch 7.10 main response.
	compatibility bool
}

var fixtures = map[string]fixture{
	"elasticsearch": {
		product: "elasticsearch", profile: "elasticsearch-8.19.22", container: "weir-local-elasticsearch", url: "http://127.0.0.1:19200",
		images: map[string]string{
			"amd64": "docker.elastic.co/elasticsearch/elasticsearch@sha256:f1e3d88299cb0db2131fc2ab9060faefccd88fd684d11a49256ae00d7dc3c857",
			"arm64": "docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237",
		},
	},
	"opensearch": {
		product: "opensearch", profile: "opensearch-3.3.0", container: "weir-local-opensearch-3.3", url: "http://127.0.0.1:19202",
		images: map[string]string{
			"amd64": "opensearchproject/opensearch@sha256:59e9e49ac00f19c2af9094e367c8e6586f7d07e6c06465c3f4c43b40934309f2",
			"arm64": "opensearchproject/opensearch@sha256:d96afaf6cbd2a6a3695aeb2f1d48c9a16ad5c8918eb849e5cbf43475f0f8e146",
		},
	},
	"opensearch-compat": {
		product: "opensearch", profile: "opensearch-2.17.1-compat", container: "weir-local-opensearch-2.17-compat", url: "http://127.0.0.1:19203",
		images: map[string]string{
			"amd64": "opensearchproject/opensearch@sha256:ab6db8c14062060cda5f0428364e76ffe2a6f03ad265589b70182c0cecb7f9d7",
			"arm64": "opensearchproject/opensearch@sha256:7d961ff222c267093b7b95fc2e397d31a06a42b6f3c42ee67fc5788417a274bf",
		},
		compatibility: true,
	},
}

// selectedFixture returns the fixture WEIR_SEARCH_PRODUCT names, which
// defaults to elasticsearch.
func selectedFixture(t *testing.T) fixture {
	t.Helper()
	name := os.Getenv("WEIR_SEARCH_PRODUCT")
	if name == "" {
		name = "elasticsearch"
	}
	selected, ok := fixtures[name]
	if !ok {
		t.Fatalf("WEIR_SEARCH_PRODUCT=%q; want elasticsearch, opensearch or opensearch-compat", name)
	}
	return selected
}

func Open(t *testing.T) *Backend {
	t.Helper()
	selected := selectedFixture(t)
	transport := &http.Transport{Proxy: nil, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, DisableCompression: true}
	client := &http.Client{
		Transport:     transport,
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	index := fmt.Sprintf("weir_search_%d_%d", os.Getpid(), sequence.Add(1))
	b := &Backend{
		URL:       selected.url,
		Product:   selected.product,
		Profile:   selected.profile,
		Container: selected.container,
		Index:     index,
		Client:    client,
	}
	status, raw := b.Do(t, "GET", "/", "")
	if status != 200 {
		t.Fatal("wrong isolated backend", status)
	}
	verifyVersion(t, raw, selected.profile, selected.container)
	if selected.compatibility {
		verifyNodeVersion(t, b, compatibilityVersion)
	}
	t.Cleanup(transport.CloseIdleConnections)
	b.Create(t, index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"n":{"type":"long"}}}}`)
	return b
}

// compatibilityVersion is the OpenSearch release behind the compatibility
// profile, whose root reports 7.10.2 instead.
const compatibilityVersion = "2.17.1"

// verifyNodeVersion checks the release the node itself reports, which
// compatibility mode does not rewrite.
func verifyNodeVersion(t *testing.T, b *Backend, want string) {
	t.Helper()
	status, raw := b.Do(t, "GET", "/_nodes/_local?filter_path=nodes.*.version", "")
	var nodes struct {
		Nodes map[string]struct{ Version string }
	}
	if status != 200 || json.Unmarshal(raw, &nodes) != nil || len(nodes.Nodes) != 1 {
		t.Fatal("isolated backend node version unavailable", status)
	}
	for _, node := range nodes.Nodes {
		if node.Version != want {
			t.Fatalf("isolated backend runs OpenSearch %q, want %s", node.Version, want)
		}
	}
}

func verifyVersion(t *testing.T, raw []byte, profile, cluster string) {
	t.Helper()
	var root struct {
		ClusterName string `json:"cluster_name"`
		Tagline     string
		Version     struct {
			Number, Distribution string
			BuildFlavor          *string `json:"build_flavor"`
			BuildHash            string  `json:"build_hash"`
		}
	}
	if json.Unmarshal(raw, &root) != nil || root.ClusterName != cluster || root.Version.BuildHash == "" {
		t.Fatal("wrong isolated backend identity")
	}
	v := root.Version
	valid := profile == "elasticsearch-8.19.22" && v.Number == "8.19.22" && v.Distribution == "" && v.BuildFlavor != nil && *v.BuildFlavor == "default"
	valid = valid || profile == "opensearch-3.3.0" && v.Number == "3.3.0" && v.Distribution == "opensearch"
	// Compatibility mode reports the Elasticsearch 7.10.2 shape; only the
	// tagline still names OpenSearch.
	valid = valid || profile == "opensearch-2.17.1-compat" && v.Number == "7.10.2" && v.Distribution == "" && v.BuildFlavor == nil &&
		root.Tagline == "The OpenSearch Project: https://opensearch.org/"
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
	response, err := b.send(method, path, body)
	if err != nil && (method == http.MethodGet || method == http.MethodDelete) {
		// A proxied exchange in the same test can leave a server-closed
		// connection idle in this pool. Reads and deletes are safe to resend once.
		b.Client.CloseIdleConnections()
		response, err = b.send(method, path, body)
	}
	if err != nil {
		t.Fatalf("independent backend %s %s failed: %v", method, path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, raw
}

// send never lets the transport replay a request on its own.
func (b *Backend) send(method, path, body string) (*http.Response, error) {
	request, err := http.NewRequest(method, b.URL+path, bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
	request.GetBody = nil
	if b.Username != "" {
		request.SetBasicAuth(b.Username, b.Password)
	}
	request.Header.Set("Content-Type", "application/json")
	return b.Client.Do(request)
}
