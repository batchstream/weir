//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/batchstream/weir/api/weir/v1"
	"io"
	"net/http"
	"time"
)

func admin(ctx context.Context, method, path, body string) (int, []byte, error) {
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	request, err := http.NewRequestWithContext(ctx, method, backendURL+path, bytes.NewBufferString(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.GetBody = nil
	reply, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer reply.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(reply.Body, (64<<10)+1))
	if len(raw) > 64<<10 {
		return reply.StatusCode, nil, fmt.Errorf("fixture HTTP response exceeds 64 KiB")
	}
	return reply.StatusCode, raw, err
}
func put(id string) *pb.MutateRequest {
	doc := &pb.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"n":1,"op":%q}`, id))}
	action := &pb.MutateRequest_Put{Put: doc}
	request := &pb.MutateRequest{Resource: "weir://records/records/s:" + id, Action: action}
	return request
}
func persisted(ctx context.Context, id string) error {
	code, raw, err := admin(ctx, "GET", "/records/_doc/"+id, "")
	var result struct {
		Version int `json:"_version"`
		Source  struct {
			N int `json:"n"`
		} `json:"_source"`
	}
	if err != nil || code != 200 || json.Unmarshal(raw, &result) != nil || result.Version != 1 || result.Source.N != 1 {
		return fmt.Errorf("persistence %s: status=%d body=%s err=%v", id, code, raw, err)
	}
	fmt.Printf("persisted id=%s version=1 n=1\n", id)
	return nil
}

func audit(ctx context.Context) error {
	if _, _, err := admin(ctx, "POST", "/records/_refresh", ""); err != nil {
		return err
	}
	code, raw, err := admin(ctx, "GET", "/records/_search?size=400&version=true", "")
	if err != nil || code != 200 {
		return fmt.Errorf("audit %d %s %v", code, raw, err)
	}
	var result struct {
		TimedOut bool `json:"timed_out"`
		Shards   struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
		Hits struct {
			Total struct {
				Value    int    `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			Hits []struct {
				ID      string `json:"_id"`
				Version int    `json:"_version"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if json.Unmarshal(raw, &result) != nil || result.TimedOut || result.Shards.Failed != 0 || result.Hits.Total.Relation != "eq" || result.Hits.Total.Value != len(result.Hits.Hits) {
		return fmt.Errorf("incomplete backend audit %s", raw)
	}
	for _, hit := range result.Hits.Hits {
		if hit.Version != 1 {
			return fmt.Errorf("replay id=%s version=%d", hit.ID, hit.Version)
		}
		fmt.Printf("FINAL id=%s version=%d\n", hit.ID, hit.Version)
	}
	fmt.Printf("AUDIT records=%d all version1\n", len(result.Hits.Hits))
	return nil
}
