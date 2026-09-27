//go:build integration

package main

// This opt-in client owns only RPCs and fixture HTTP observations. Kubernetes,
// resource ownership and fault recovery belong to scripts/test-kubernetes.py.
import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/app"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var backendURL string

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "idle", "bounded owned fixture phase")
	id := flag.String("id", "smoke", "unique operation suffix")
	target := flag.String("target", "dns:///weir.m21.svc.cluster.local:7447", "fixture Service or Pod address")
	backend := flag.String("backend", "http://elasticsearch.m21.svc.cluster.local:9200", "owned backend")
	config := flag.String("validate", "", "validate generated static config without constructing resources")
	flag.Parse()
	if os.Getenv("WEIR_KUBE_INTEGRATION") != "1" {
		return errors.New("explicit integration opt-in required")
	}
	if *config != "" {
		f, err := os.Open(*config)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = app.Decode(f)
		return err
	}
	if !regexp.MustCompile(`^[a-z0-9-]{1,32}$`).MatchString(*id) {
		return errors.New("invalid fixture ID")
	}
	backendURL = *backend
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	switch *mode {
	case "idle":
		time.Sleep(15 * time.Minute)
		return nil
	case "setup":
		code, raw, err := admin(ctx, "PUT", "/records", `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)
		if err != nil || code != 200 {
			return fmt.Errorf("setup: %d %s %v", code, raw, err)
		}
		code, raw, err = admin(ctx, "GET", "/", "")
		fmt.Printf("backend identity status=%d %s\n", code, raw)
		return err
	case "inspect":
		return persisted(ctx, *id)
	case "audit":
		return audit(ctx)
	case "db":
		_, tail, tailErr := admin(ctx, "GET", "/records/_stats/refresh?filter_path=_all.total.refresh", "")
		if tailErr != nil || !json.Valid(tail) {
			return fmt.Errorf("refresh stats: %s %v", tail, tailErr)
		}
		fmt.Printf("DB refresh %s\n", tail)
		code, raw, err := admin(ctx, "GET", "/_nodes/stats/http,thread_pool?filter_path=nodes.*.http.current_open,nodes.*.http.total_opened,nodes.*.thread_pool.write", "")
		if err != nil || code != 200 || !json.Valid(raw) {
			return fmt.Errorf("invalid DB statistics: status=%d err=%v", code, err)
		}
		fmt.Printf("DB status=%d %s\n", code, raw)
		return nil
	case "refresh":
		_, _, err := admin(ctx, "POST", "/records/_refresh", "")
		return err
	}
	connection, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0)))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := pb.NewWeirClient(connection)
	switch *mode {
	case "smoke":
		call, stop := context.WithTimeout(ctx, 4*time.Second)
		defer stop()
		return smoke(call, client, *id)
	case "hold":
		return hold(ctx, client, *id)
	case "load":
		return load(ctx, client, *id)
	case "probe":
		return mutation(ctx, client, *id)
	case "active":
		activeCtx, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		return active(activeCtx, connection, client, *id)
	default:
		return errors.New("unknown fixture phase")
	}
}
