//go:build integration

package testsearch

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
	"golang.org/x/crypto/bcrypt"
)

const secureOwner = "weir-milestone-11"
const elasticImage = "docker.elastic.co/elasticsearch/elasticsearch@sha256:2f602552550869fb29b6fd5848c5118d3ef3a2e1d5d45802e3ab9088cb2de8e2"
const openSearchImage = "opensearchproject/opensearch@sha256:1f8b88245a6af61e7aa500afe0e87d43401e4b33140bb47230a919428ce3f7cb"

// SecureFixture owns one native TLS database. Admin is used only for bootstrap
// and independent observation. Backend contains the restricted application pair.
// No process-global lookup by index, URL or database exists.
type SecureFixture struct {
	Backend              *Backend
	Admin                *Backend
	Denied               *Backend
	Root                 string
	name, image, product string
	materials            string
	created              bool
}

func OpenSecure(t *testing.T) *SecureFixture {
	t.Helper()
	product := os.Getenv("WEIR_SEARCH_SECURE_INTEGRATION")
	if product == "" {
		t.Skip("native HTTPS/Basic requires WEIR_SEARCH_SECURE_INTEGRATION=elasticsearch|opensearch")
	}
	if product != "elasticsearch" && product != "opensearch" {
		t.Fatal("invalid secure Search product")
	}
	suffix := fmt.Sprintf("%d-%d-%s", os.Getpid(), sequence.Add(1), randomPassword(t)[:12])
	f := &SecureFixture{product: product, name: "weir-m11-" + product + "-" + suffix, image: elasticImage}
	if product == "opensearch" {
		f.image = openSearchImage
	}
	parent := filepath.Join(testutil.Root(t), ".testdata")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal("fixture parent unavailable")
	}
	f.Root = filepath.Join(parent, f.name)
	f.materials = filepath.Join(f.Root, "materials")
	if err := os.Mkdir(f.Root, 0700); err != nil {
		t.Fatal("cannot create exclusive fixture directory")
	}
	f.write(t, "../.weir-owner", secureOwner, 0600)
	t.Cleanup(func() { f.cleanup(t) })
	// The non-root database uid traverses its read-only mount; its host parent
	// remains private. Never adopt a previous process's directory or container.
	if err := os.Mkdir(f.materials, 0755); err != nil {
		t.Fatal("fixture material directory unavailable")
	}
	roots := f.certificates(t)
	adminPass, appPass := randomPassword(t), randomPassword(t)
	f.configuration(t, adminPass, appPass)
	base := "/usr/share/" + product + "/config"
	args := []string{"run", "--pull=never", "-d", "--name", f.name, "--hostname", "weir-node", "--label", "weir.owner=" + secureOwner, "--memory=1536m", "--cpus=2", "-p", "127.0.0.1::9200", "--mount", "type=bind,src=" + f.materials + ",dst=" + base + "/weir,readonly", "--mount", "type=bind,src=" + filepath.Join(f.materials, product+".yml") + ",dst=" + base + "/" + product + ".yml,readonly"}
	if product == "elasticsearch" {
		args = append(args, "-e", "ES_JAVA_OPTS=-Xms512m -Xmx512m")
		for _, file := range []string{"users", "users_roles", "roles.yml"} {
			args = append(args, "--mount", "type=bind,src="+filepath.Join(f.materials, file)+",dst="+base+"/"+file+",readonly")
		}
	} else {
		args = append(args, "-e", "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m", "-e", "DISABLE_INSTALL_DEMO_CONFIG=true")
	}
	args = append(args, f.image)
	f.docker(t, 45*time.Second, args...)
	f.created = true
	port := strings.TrimSpace(f.docker(t, 5*time.Second, "port", f.name, "9200/tcp"))
	if !strings.HasPrefix(port, "127.0.0.1:") {
		t.Fatal("fixture port is not loopback")
	}
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	tlsConfig := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, Protocols: protocols, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, DisableCompression: true, TLSHandshakeTimeout: 2 * time.Second, ResponseHeaderTimeout: 5 * time.Second}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(transport.CloseIdleConnections)
	profile := "elasticsearch-8.17.0"
	if product == "opensearch" {
		profile = "opensearch-2.19.0"
	}
	index := "weir_m11_" + strings.ReplaceAll(suffix, "-", "_")
	f.Admin = &Backend{URL: "https://" + port, Profile: profile, Index: index, Client: client, Username: "weir_admin", Password: adminPass, CAFile: filepath.Join(f.materials, "ca.pem")}
	f.Backend = &Backend{URL: f.Admin.URL, Profile: profile, Index: index, Client: client, Username: "weir_app", Password: appPass, CAFile: f.Admin.CAFile}
	f.Denied = &Backend{URL: f.Admin.URL, Profile: profile, Index: index, Client: client, Username: "weir_reader", Password: appPass, CAFile: f.Admin.CAFile}
	f.waitReady(t, product == "opensearch", 90*time.Second)
	if product == "opensearch" {
		f.docker(t, 45*time.Second, "exec", f.name, "/usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh", "-h", "localhost", "-p", "9200", "-cn", f.name, "-cacert", base+"/weir/ca.pem", "-cert", base+"/weir/admin.pem", "-key", base+"/weir/admin.key", "-cd", base+"/weir/security/")
	}
	f.waitReady(t, false, 20*time.Second)
	status, _ := f.Admin.Do(t, "GET", "/", "")
	if status != 200 {
		t.Fatal("fixture Basic administrator bootstrap failed", status)
	}
	f.Admin.Create(t, index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"n":{"type":"long"}}}}`)
	t.Logf("native %s HTTPS/Basic fixture=%s image=%s CPU=2 memory=1536MiB", profile, f.name, f.image)
	return f
}

// Startup probes are independent reads; they never retry a business command.
func (f *SecureFixture) waitReady(t *testing.T, uninitialized bool, limit time.Duration) {
	t.Helper()
	for end := time.Now().Add(limit); time.Now().Before(end); {
		request, err := http.NewRequest("GET", f.Admin.URL+"/", nil)
		if err != nil {
			t.Fatal("fixture probe construction")
		}
		request.SetBasicAuth(f.Admin.Username, f.Admin.Password)
		response, err := f.Admin.Client.Do(request)
		if err == nil {
			code := response.StatusCode
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
			_ = response.Body.Close()
			if code == 200 || uninitialized && code == 503 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("native HTTPS fixture readiness timed out; diagnostics retained")
}

func randomPassword(t *testing.T) string {
	t.Helper()
	data := make([]byte, 24)
	if _, err := rand.Read(data); err != nil {
		t.Fatal("fixture random source unavailable")
	}
	return hex.EncodeToString(data)
}
func (f *SecureFixture) write(t *testing.T, name, body string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.materials, name), []byte(body), mode); err != nil {
		t.Fatal("fixture file creation failed")
	}
}
func (f *SecureFixture) certificates(t *testing.T) *x509.CertPool {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal("fixture CA key generation")
	}
	subject := pkix.Name{CommonName: "Weir M11 temporary CA"}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: subject, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(12 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal("fixture CA generation")
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal("fixture CA parse")
	}
	block := &pem.Block{Type: "CERTIFICATE", Bytes: der}
	f.write(t, "ca.pem", string(pem.EncodeToMemory(block)), 0644)
	for i, name := range []string{"server", "admin"} {
		leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal("fixture leaf key generation")
		}
		commonName := "weir-node"
		if name == "admin" {
			commonName = "weir-admin"
		}
		subject := pkix.Name{CommonName: commonName}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: subject, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(12 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{"localhost", "weir-node", "search.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
		if err != nil {
			t.Fatal("fixture certificate generation")
		}
		encoded, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			t.Fatal("fixture PKCS8 generation")
		}
		certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: der}
		keyBlock := &pem.Block{Type: "PRIVATE KEY", Bytes: encoded}
		f.write(t, name+".pem", string(pem.EncodeToMemory(certBlock)), 0644)
		f.write(t, name+".key", string(pem.EncodeToMemory(keyBlock)), 0644)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return roots
}
func (f *SecureFixture) configuration(t *testing.T, adminPass, appPass string) {
	t.Helper()
	adminHash, err := bcrypt.GenerateFromPassword([]byte(adminPass), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal("fixture password hashing")
	}
	appHash, err := bcrypt.GenerateFromPassword([]byte(appPass), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal("fixture password hashing")
	}
	common := "cluster.name: " + f.name + "\nnode.name: weir-node\nnetwork.host: 0.0.0.0\ndiscovery.type: single-node\naction.auto_create_index: false\nthread_pool.write.size: 1\nthread_pool.write.queue_size: 1\n"
	if f.product == "elasticsearch" {
		f.write(t, "elasticsearch.yml", common+`xpack.security.enabled: true
xpack.security.autoconfiguration.enabled: false
xpack.security.http.ssl.enabled: true
xpack.security.http.ssl.key: weir/server.key
xpack.security.http.ssl.certificate: weir/server.pem
xpack.security.http.ssl.certificate_authorities: [weir/ca.pem]
xpack.security.transport.ssl.enabled: true
xpack.security.transport.ssl.key: weir/server.key
xpack.security.transport.ssl.certificate: weir/server.pem
xpack.security.transport.ssl.certificate_authorities: [weir/ca.pem]
`, 0644)
		f.write(t, "users", "weir_admin:"+string(adminHash)+"\nweir_app:"+string(appHash)+"\nweir_reader:"+string(appHash)+"\n", 0644)
		f.write(t, "users_roles", "weir_fixture_admin:weir_admin\nweir_application:weir_app\nweir_reader:weir_reader\n", 0644)
		f.write(t, "roles.yml", `weir_fixture_admin:
  cluster: [all]
  indices:
    - names: ['weir_m11_*']
      privileges: [all]
weir_application:
  cluster: [monitor]
  indices:
    - names: ['weir_m11_*']
      privileges: [read, write, view_index_metadata]
weir_reader:
  cluster: [monitor]
  indices:
    - names: ['weir_m11_*']
      privileges: [read, view_index_metadata]
`, 0644)
		return
	}
	f.write(t, "opensearch.yml", common+`plugins.security.ssl.transport.pemcert_filepath: weir/server.pem
plugins.security.ssl.transport.pemkey_filepath: weir/server.key
plugins.security.ssl.transport.pemtrustedcas_filepath: weir/ca.pem
plugins.security.ssl.transport.enforce_hostname_verification: true
plugins.security.ssl.http.enabled: true
plugins.security.ssl.http.pemcert_filepath: weir/server.pem
plugins.security.ssl.http.pemkey_filepath: weir/server.key
plugins.security.ssl.http.pemtrustedcas_filepath: weir/ca.pem
plugins.security.ssl.http.clientauth_mode: OPTIONAL
plugins.security.nodes_dn: ['CN=weir-node']
plugins.security.authcz.admin_dn: ['CN=weir-admin']
plugins.security.allow_default_init_securityindex: false
`, 0644)
	if err := os.Mkdir(filepath.Join(f.materials, "security"), 0755); err != nil {
		t.Fatal("fixture security directory")
	}
	files := map[string]string{
		"config": `config:
  dynamic:
    http:
      anonymous_auth_enabled: false
    authc:
      basic_internal_auth_domain:
        http_enabled: true
        transport_enabled: false
        order: 0
        http_authenticator:
          type: basic
          challenge: true
        authentication_backend:
          type: intern
`,
		"internal_users": "weir_admin:\n  hash: '" + string(adminHash) + "'\nweir_app:\n  hash: '" + string(appHash) + "'\nweir_reader:\n  hash: '" + string(appHash) + "'\n",
		"roles": `weir_fixture_admin:
  cluster_permissions: ['cluster:*', 'indices:data/write/bulk*']
  index_permissions:
    - index_patterns: ['weir_m11_*']
      allowed_actions: ['indices:*']
weir_application:
  cluster_permissions: ['cluster:monitor/main', 'cluster:monitor/state', 'indices:data/write/bulk*']
  index_permissions:
    - index_patterns: ['weir_m11_*']
      allowed_actions: ['indices:admin/get', 'indices:admin/mapping/put', 'indices:data/read/*', 'indices:data/write/*']
weir_reader:
  cluster_permissions: ['cluster:monitor/main', 'cluster:monitor/state']
  index_permissions:
    - index_patterns: ['weir_m11_*']
      allowed_actions: ['indices:admin/get', 'indices:data/read/*']
`,
		"roles_mapping": `weir_fixture_admin:
  users: [weir_admin]
weir_application:
  users: [weir_app]
weir_reader:
  users: [weir_reader]
`,
		"action_groups": "", "tenants": "", "nodes_dn": "", "whitelist": "", "audit": "config:\n  enabled: false\n",
	}
	for name, body := range files {
		kind := strings.ReplaceAll(name, "_", "")
		f.write(t, "security/"+name+".yml", "_meta:\n  type: "+kind+"\n  config_version: 2\n"+body, 0644)
	}
}
func (f *SecureFixture) docker(t *testing.T, limit time.Duration, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if args[0] == "exec" {
		if writeErr := os.WriteFile(filepath.Join(f.Root, "bootstrap.log"), output, 0600); writeErr != nil {
			t.Fatal("cannot retain fixture bootstrap diagnostic")
		}
	}
	if err != nil {
		t.Fatalf("fixture Docker step %s failed (details retained without credentials)", args[0])
	}
	return string(output)
}
func (f *SecureFixture) cleanup(t *testing.T) {
	t.Helper()
	owner, err := os.ReadFile(filepath.Join(f.Root, ".weir-owner"))
	if err != nil || string(owner) != secureOwner {
		t.Error("fixture cleanup owner mismatch")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	metadata, inspectErr := exec.CommandContext(ctx, "docker", "inspect", "-f", `{{index .Config.Labels "weir.owner"}}|{{.Config.Image}}`, f.name).CombinedOutput()
	cancel()
	missing := strings.Contains(string(metadata), "No such object") || strings.Contains(string(metadata), "No such container")
	if inspectErr != nil && (f.created || !missing) {
		t.Error("cannot verify owned container during cleanup")
		return
	}
	if inspectErr == nil {
		if strings.TrimSpace(string(metadata)) != secureOwner+"|"+f.image {
			t.Error("container cleanup owner/image mismatch")
			return
		}
		f.docker(t, 25*time.Second, "stop", "--time", "15", f.name)
		logs := f.docker(t, 5*time.Second, "logs", f.name)
		if f.Admin != nil {
			logs = strings.ReplaceAll(logs, f.Admin.Password, "[redacted]")
			logs = strings.ReplaceAll(logs, f.Backend.Password, "[redacted]")
		}
		if err := os.WriteFile(filepath.Join(f.Root, "database.log"), []byte(logs), 0600); err != nil {
			t.Error("cannot retain fixture log")
		}
		f.docker(t, 10*time.Second, "rm", f.name)
	}
	if err := os.RemoveAll(f.materials); err != nil {
		t.Error("fixture temporary materials cleanup failed")
	}
	t.Log("fixture stopped; owned container/data/materials removed; log:", f.Root)
}
