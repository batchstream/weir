//go:build integration

package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type nativeBurstRecord struct {
	N   int64  `json:"n" bson:"n"`
	Pad string `json:"pad" bson:"pad"`
}

type nativeBurstDataset struct {
	store, root, media string
	mongo              *testmongo.Fixture
	search             *testsearch.Backend
	native             *pb.NativeRequest
	expected           map[string]nativeBurstRecord
}

type nativeBurstResult struct {
	sequence, backend, kind int
	elapsed                 time.Duration
	err                     error
}

func TestCLINativeSmallBurstPreservesAdmission(t *testing.T) {
	if os.Getenv("WEIR_NATIVE_BURST_INTEGRATION") != "1" {
		t.Skip("requires explicit clean Linux CLI Native burst qualification")
	}
	if os.Getenv("WEIR_INTEGRATION") != "1" || os.Getenv("WEIR_SEARCH_INTEGRATION") != "elasticsearch" || os.Getenv("WEIR_MONGO_PROFILE") == "tls" {
		t.Fatal("requires isolated plain MongoDB and Elasticsearch 8.19.22 fixtures")
	}
	binary := os.Getenv("WEIR_NATIVE_BURST_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("WEIR_NATIVE_BURST_BINARY must be an absolute Linux CLI path")
	}
	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest, revision := fmt.Sprintf("%x", sha256.Sum256(raw)), os.Getenv("WEIR_NATIVE_BURST_SOURCE")
	if len(revision) != 40 || digest != os.Getenv("WEIR_NATIVE_BURST_SHA256") {
		t.Fatal("requires an exact source and binary SHA256 receipt")
	}
	engine := strings.TrimSpace(packagedDocker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}/{{.CgroupVersion}} kernel={{.KernelVersion}}"))
	if !strings.HasPrefix(engine, "linux/aarch64/2 ") {
		t.Fatal("requires native Linux arm64 cgroup-v2", engine)
	}
	image := strings.TrimSpace(packagedDocker(t, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Id}}", "busybox:1.37.0"))
	if image != "linux/arm64 sha256:ad4f11f7c85f87c0a6d6c0fe492ad53f9cdcff6e8c88b881360c70eff701c67b" {
		t.Fatal("cached CLI fixture identity changed", image)
	}
	mongo, search := testmongo.Open(t), testsearch.Open(t)
	metadata := strings.Fields(packagedDocker(t, "inspect", "--type", "container", "--format", `{{.Id}} {{index .Config.Labels "weir.owner"}} {{.Config.Image}} {{(index .NetworkSettings.Networks "bridge").IPAddress}} {{.State.Running}}`, "weir-local-elasticsearch"))
	if len(metadata) != 5 || len(metadata[0]) != 64 || metadata[1] != "weir-search-local" || metadata[2] != "docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237" || metadata[4] != "true" {
		t.Fatal("owned pinned Elasticsearch fixture identity mismatch", metadata)
	}
	bridge, err := netip.ParseAddr(metadata[3])
	if err != nil || !bridge.Is4() || !bridge.IsPrivate() {
		t.Fatal("owned Elasticsearch private bridge unavailable")
	}
	datasets := []*nativeBurstDataset{
		{store: "mongo", root: mongo.DB + "/records", media: "application/bson", mongo: mongo, expected: make(map[string]nativeBurstRecord)},
		{store: "search", root: search.Index, media: "application/json", search: search, expected: make(map[string]nativeBurstRecord)},
	}
	for _, dataset := range datasets {
		seedNativeBurstDataset(t, dataset)
	}
	mongoBackend, searchBackend := &Mongo{URI: "mongodb://host.docker.internal:27028/?directConnection=true&serverMonitoringMode=poll"}, &Search{URL: "http://" + bridge.String() + ":9200"}
	mongoLocal, searchLocal := &Local{Backend: BackendConfig{MongoDB: mongoBackend}}, &Local{Backend: BackendConfig{Search: searchBackend}}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{{Name: "mongo", Local: mongoLocal}, {Name: "search", Local: searchLocal}}
	opts := memoryRecoveryOptions{binary: binary, capacityMiB: 1024, automatic: true, config: &cfg}
	process := startMemoryRecoveryCLI(t, opts)
	version := strings.TrimSpace(packagedDocker(t, "exec", process.name, "/weir", "version"))
	var identity map[string]string
	if json.Unmarshal([]byte(version), &identity) != nil || identity["state"] != "clean-commit" || identity["dirty"] != "false" || identity["target"] != "linux/arm64" || identity["revision"] != revision {
		t.Fatal("qualification requires the clean exact Linux source identity", version)
	}
	t.Logf("fixed10x256 mixed1KiB 1GiB/2CPU caller3s cleanup5s retries=0 SHA256=%s version=%s engine=%s image=%s Search_bridge=%s container=%s", digest, version, engine, image, bridge, metadata[0])
	initial := nativeBurstSnapshot(t, process)
	if initial["budget_rejections_total"] != 0 {
		t.Error("fresh CLI already reported Native budget rejection", initial)
	}
	openContext, stop := context.WithTimeout(t.Context(), 3*time.Second)
	openOptions := weirclient.OpenOptions{Seed: process.address, Stores: []string{"mongo", "search"}}
	client, err := weirclient.Open(openContext, openOptions)
	stop()
	if err != nil {
		t.Fatal("real SDK discovery failed", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	failures := 0
	for round := range 10 {
		results := make(chan nativeBurstResult, 256)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var calls sync.WaitGroup
		for index := range 256 {
			job := nativeBurstResult{sequence: round*256 + index, backend: index % 2, kind: index % 3}
			calls.Add(1)
			go func() {
				defer calls.Done()
				call, stopCall := context.WithTimeout(ctx, 3*time.Second)
				started := time.Now()
				job.err = executeNativeBurstCall(call, client, datasets[job.backend], job)
				job.elapsed = time.Since(started)
				stopCall()
				results <- job
			}()
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		received, completed := 0, 0
		latencies := make([]time.Duration, 0, 256)
		for received < 256 {
			select {
			case result := <-results:
				received++
				latencies = append(latencies, result.elapsed)
				if result.elapsed > 3*time.Second {
					t.Error("fixed3s caller budget exceeded", round, result.sequence, result.elapsed)
				}
				if result.err != nil {
					failures++
					t.Logf("burst_failure round=%d seq=%d backend=%s method=%d elapsed=%s origin=%v", round, result.sequence, datasets[result.backend].store, result.kind, result.elapsed, result.err)
				} else {
					completed++
					if result.kind == 1 {
						datasets[result.backend].expected[fmt.Sprintf("write-%d", result.sequence)] = nativeBurstRecord{N: int64(result.sequence + 1000), Pad: strings.Repeat("x", 1<<10)}
					}
				}
			case scheduled := <-ticker.C:
				snapshot := nativeBurstSnapshot(t, process)
				t.Logf("burst_snapshot round=%d sample_lag=%s Native=%v", round, time.Since(scheduled), snapshot)
			case <-ctx.Done():
				cancel()
				calls.Wait()
				ticker.Stop()
				t.Fatal("bounded burst lost results", round, received, ctx.Err())
			}
		}
		calls.Wait()
		cancel()
		ticker.Stop()
		slices.Sort(latencies)
		snapshot := nativeBurstSnapshot(t, process)
		t.Logf("burst round=%d offered=256 received=%d completed=%d p50=%s p99=%s max=%s Native=%v", round, received, completed, latencies[127], latencies[253], latencies[255], snapshot)
		for _, dataset := range datasets {
			verifyNativeBurstOracle(t, dataset)
		}
	}
	if err := client.Close(); err != nil {
		t.Error(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot := nativeBurstSnapshot(t, process)
		idle := true
		for _, name := range []string{"used_bytes", "resident_bytes", "wire_bytes", "wire_buffers", "input_reserved_bytes", "input_used_bytes", "leases", "native_leases", "starting", "settled", "borrowed", "waiting", "terminal", "ingress_bytes", "ingress_reserved_bytes", "ingress_buffers", "ingress_messages", "ingress_waiting", "ingress_connections", "ingress_connection_bytes"} {
			idle = idle && snapshot[name] == 0
		}
		metrics := process.sample(t)
		for _, name := range []string{"weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes"} {
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		if snapshot["budget_rejections_total"] != initial["budget_rejections_total"] {
			t.Error("small valid burst caused Native budget rejection", snapshot)
		}
		if idle {
			t.Logf("final5s Node/Store/RPC/wire ownership0 Q=I=0 Native=%v", snapshot)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixed5s cleanup exhausted", snapshot)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if failures != 0 {
		t.Errorf("received all2560 results with %d business failures", failures)
	}
}

func seedNativeBurstDataset(t *testing.T, dataset *nativeBurstDataset) {
	t.Helper()
	record := nativeBurstRecord{N: 1, Pad: strings.Repeat("x", 1<<10)}
	dataset.expected["hot"] = record
	if dataset.mongo != nil {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		document := bson.D{{Key: "_id", Value: "hot"}, {Key: "n", Value: record.N}, {Key: "pad", Value: record.Pad}}
		if _, err := dataset.mongo.Admin.Database(dataset.mongo.DB).Collection("records").InsertOne(ctx, document); err != nil {
			t.Fatal(err)
		}
		filter := bson.D{{Key: "_id", Value: "hot"}}
		command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}}
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		request := &pb.Document{ContentType: dataset.media, Data: raw}
		dataset.native = &pb.NativeRequest{Resource: dataset.mongo.DB, Request: request}
	} else {
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		code, _ := dataset.search.Do(t, http.MethodPut, "/"+dataset.root+"/_doc/hot", string(raw))
		if code != http.StatusCreated {
			t.Fatal("owned Search seed failed", code)
		}
		request, err := http.NewRequest(http.MethodGet, "http://backend/"+dataset.root+"/_doc/hot", nil)
		if err != nil {
			t.Fatal(err)
		}
		dataset.native, err = weirclient.NewHTTPNativeRequest("", request)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func executeNativeBurstCall(ctx context.Context, client *weirclient.Client, dataset *nativeBurstDataset, job nativeBurstResult) error {
	want := nativeBurstRecord{N: 1, Pad: strings.Repeat("x", 1<<10)}
	var actual nativeBurstRecord
	switch job.kind {
	case 0:
		request := &weirclient.ReadRequest{Resource: dataset.root + "/s:hot"}
		opts := weirclient.ReadOneOptions{StoreName: dataset.store, Request: request}
		result, err := client.ReadOne(ctx, opts)
		if err != nil {
			return err
		}
		if result == nil || result.Failure != nil || result.Document == nil {
			return fmt.Errorf("Read missing document or failed: %v", result)
		}
		if dataset.mongo != nil {
			err = bson.Unmarshal(result.Document.Data, &actual)
		} else {
			err = json.Unmarshal(result.Document.Data, &actual)
		}
		if err != nil {
			return err
		}
	case 1:
		id := fmt.Sprintf("write-%d", job.sequence)
		record := nativeBurstRecord{N: int64(job.sequence + 1000), Pad: want.Pad}
		var raw []byte
		var err error
		if dataset.mongo != nil {
			document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: record.N}, {Key: "pad", Value: record.Pad}}
			raw, err = bson.Marshal(document)
		} else {
			raw, err = json.Marshal(record)
		}
		if err != nil {
			return err
		}
		document := &pb.Document{ContentType: dataset.media, Data: raw}
		request := &weirclient.WriteRequest{Resource: dataset.root + "/s:" + id, Document: document}
		opts := weirclient.WriteOptions{StoreName: dataset.store, Request: request}
		result, err := client.Put(ctx, opts)
		if err != nil {
			return err
		}
		if result == nil || result.Failure != nil || result.Outcome != weirclient.MutationApplied {
			return fmt.Errorf("Put lacked APPLIED evidence: %v", result)
		}
		return nil
	case 2:
		var body []byte
		consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
			if len(body)+len(chunk) > 4<<10 {
				return fmt.Errorf("Native small document response exceeded4KiB")
			}
			body = append(body, chunk...)
			return nil
		}
		opts := weirclient.NativeOptions{StoreName: dataset.store, Request: dataset.native, Consume: consume}
		result, err := client.Native(ctx, opts)
		if err != nil {
			return err
		}
		if result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete {
			return fmt.Errorf("Native completion/failure origin: %v", result)
		}
		if dataset.mongo != nil {
			var response struct {
				OK     float64 `bson:"ok"`
				Cursor struct {
					Batch []nativeBurstRecord `bson:"firstBatch"`
				} `bson:"cursor"`
			}
			if err := bson.Unmarshal(body, &response); err != nil || response.OK != 1 || len(response.Cursor.Batch) != 1 {
				return fmt.Errorf("Native Mongo response invalid: %v", err)
			}
			actual = response.Cursor.Batch[0]
		} else {
			metadata, err := weirclient.ParseHTTPNativeResponse(result.Response)
			if err != nil {
				return fmt.Errorf("Native HTTP metadata invalid: %v", err)
			}
			if metadata.StatusCode != http.StatusOK {
				return fmt.Errorf("Native HTTP status %d, want %d", metadata.StatusCode, http.StatusOK)
			}
			var response struct {
				Found  bool              `json:"found"`
				Source nativeBurstRecord `json:"_source"`
			}
			if err := json.Unmarshal(body, &response); err != nil || !response.Found {
				return fmt.Errorf("Native HTTP document invalid: %v", err)
			}
			actual = response.Source
		}
	}
	if actual != want {
		return fmt.Errorf("Read/Native returned incorrect immutable content")
	}
	return nil
}

func nativeBurstSnapshot(t *testing.T, process *memoryRecoveryCLI) map[string]float64 {
	t.Helper()
	metrics := process.sample(t)
	values := qualificationExecutionMemory(t, metrics, process.capacity)
	latched := qualificationGauge(t, metrics, "weir_memory_latched", nil)
	if latched != 0 {
		t.Error("small burst unexpectedly latched memory pressure")
	}
	for _, store := range []string{"mongo", "search"} {
		labels := map[string]string{"store": store}
		for name, want := range map[string]float64{"weir_store_active_executions_limit": 16, "weir_store_pending_entries_limit": 1024, "weir_store_pending_reserved_bytes_limit": 32 << 20, "weir_store_batch_max_operations": 128, "weir_store_batch_max_bytes": 4 << 20, "weir_store_batch_max_wait_seconds": .02} {
			actual := qualificationGauge(t, metrics, name, labels)
			if actual != want {
				t.Error("default Store policy changed", store, name, actual, want)
			}
		}
		for name, limit := range map[string]float64{"weir_store_active_executions": 16, "weir_store_pending_entries": 1024, "weir_store_pending_reserved_bytes": 32 << 20} {
			actual := qualificationGauge(t, metrics, name, labels)
			if actual > limit {
				t.Error("Store exceeded unchanged default bound", store, name, actual, limit)
			}
		}
	}
	return values
}

func verifyNativeBurstOracle(t *testing.T, dataset *nativeBurstDataset) {
	t.Helper()
	seen := make(map[string]bool)
	if dataset.mongo != nil {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		filter := bson.D{}
		cursor, err := dataset.mongo.Admin.Database(dataset.mongo.DB).Collection("records").Find(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			id, valid := cursor.Current.Lookup("_id").StringValueOK()
			var actual nativeBurstRecord
			err := bson.Unmarshal(cursor.Current, &actual)
			want, exists := dataset.expected[id]
			if err != nil || !valid || !exists || seen[id] || actual != want {
				t.Error("Mongo independent oracle differs", id, err)
			}
			seen[id] = true
		}
		if err := cursor.Err(); err != nil {
			t.Fatal(err)
		}
	} else {
		code, _ := dataset.search.Do(t, http.MethodPost, "/"+dataset.root+"/_refresh", "")
		if code != http.StatusOK {
			t.Fatal("Search independent refresh failed", code)
		}
		code, body := dataset.search.Do(t, http.MethodGet, "/"+dataset.root+"/_search?size=1000&track_total_hits=true", "")
		var response struct {
			Hits struct {
				Total struct {
					Value    int    `json:"value"`
					Relation string `json:"relation"`
				} `json:"total"`
				Hits []struct {
					ID     string            `json:"_id"`
					Source nativeBurstRecord `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if code != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Hits.Total.Relation != "eq" || response.Hits.Total.Value != len(dataset.expected) {
			t.Error("Search independent cardinality differs", code, response.Hits.Total)
		}
		for _, hit := range response.Hits.Hits {
			want, exists := dataset.expected[hit.ID]
			if !exists || seen[hit.ID] || hit.Source != want {
				t.Error("Search independent oracle differs", hit.ID)
			}
			seen[hit.ID] = true
		}
	}
	if len(seen) != len(dataset.expected) {
		t.Error("independent oracle lost acknowledged effects", dataset.store, len(seen), len(dataset.expected))
	}
	t.Logf("independent_oracle store=%s expected=%d seen=%d every_ID_n_pad_checked=true", dataset.store, len(dataset.expected), len(seen))
}
