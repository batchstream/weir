package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct {
	HTTP        *http.Client
	Transport   *http.Transport
	Backend     string
	Connections []*grpc.ClientConn
	RPC         []pb.WeirClient
}

func newClient(backend, target string) (*Client, error) {
	dialer := &net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{Proxy: nil, DialContext: dialer.DialContext, DisableCompression: true, MaxConnsPerHost: 62, MaxIdleConns: 62, MaxIdleConnsPerHost: 62, IdleConnTimeout: 10 * time.Second, ResponseHeaderTimeout: time.Second, MaxResponseHeaderBytes: 16384}
	hc := &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return errors.New("redirect refused") }}
	c := &Client{HTTP: hc, Transport: transport, Backend: backend}
	if target != "" {
		for i := 0; i < 4; i++ {
			conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(16384), grpc.MaxCallSendMsgSize(16384)))
			if err != nil {
				c.Close()
				return nil, err
			}
			c.Connections = append(c.Connections, conn)
			c.RPC = append(c.RPC, pb.NewWeirClient(conn))
		}
	}
	return c, nil
}
func (c *Client) Close() {
	for _, conn := range c.Connections {
		conn.Close()
	}
	c.Transport.CloseIdleConnections()
}
func (c *Client) request(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.Backend+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.GetBody = nil
	// Non-rewindable bodies disable net/http transparent replay, including GET.
	if len(body) == 0 {
		req.Body = io.NopCloser(bytes.NewReader(nil))
		req.ContentLength = -1
	}
	req.Header.Set("Content-Type", "application/json")
	reply, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer reply.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(reply.Body, (256<<10)+1))
	if len(raw) > 256<<10 {
		return reply.StatusCode, nil, errors.New("response limit")
	}
	return reply.StatusCode, raw, err
}
func failure(class string, write bool) Result {
	r := Result{Class: class}
	if write {
		r.Outcome = unknown
	}
	return r
}
func (c *Client) Call(ctx context.Context, op Operation) Result {
	if ctx.Err() != nil {
		r := Result{Class: "deadline_before_call", Outcome: notStarted}
		return r
	}
	if len(c.RPC) == 0 {
		return c.direct(ctx, op)
	}
	client := c.RPC[op.Number%len(c.RPC)]
	resource := "weir://records/records/s:" + op.ID
	if !op.Write {
		req := &pb.ReadRequest{Resource: resource, ReadMediaType: "application/json"}
		resp, err := client.Read(ctx, req)
		if err != nil {
			return failure("transport_"+status.Code(err).String(), false)
		}
		if f := resp.GetFailure(); f != nil {
			return failure("protocol_"+f.Code.String(), false)
		}
		if resp.GetDocument() == nil || resp.GetDocument().MediaType != "application/json" || !validPayload(resp.GetDocument().Data, op.ID) {
			return failure("payload_or_response", false)
		}
		r := Result{Class: "ok"}
		return r
	}
	doc := &pb.Document{MediaType: "application/json", Data: payload(op.ID)}
	action := &pb.MutateRequest_Put{Put: doc}
	req := &pb.MutateRequest{Resource: resource, Action: action}
	resp, err := client.Mutate(ctx, req)
	if err != nil {
		return failure("transport_"+status.Code(err).String(), true)
	}
	r := Result{Outcome: byte(resp.GetOutcome()), Class: "ok"}
	// Public enum and ledger encodings are deliberately different.
	switch resp.GetOutcome() {
	case pb.MutationOutcome_APPLIED:
		r.Outcome = applied
	case pb.MutationOutcome_UNKNOWN:
		r.Outcome = unknown
	case pb.MutationOutcome_NOT_APPLIED:
		r.Outcome = notApplied
	case pb.MutationOutcome_NOT_STARTED:
		r.Outcome = notStarted
	default:
		return failure("invalid_outcome", true)
	}
	if f := resp.GetFailure(); f != nil {
		r.Class = "protocol_" + f.Code.String()
	} else if r.Outcome != applied {
		r.Class = "invalid_outcome"
	}
	return r
}

type Record struct {
	Index   string          `json:"_index"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Found   bool            `json:"found"`
	Source  json.RawMessage `json:"_source"`
	Error   json.RawMessage `json:"error"`
}

func (c *Client) direct(ctx context.Context, op Operation) Result {
	if !op.Write {
		code, raw, err := c.request(ctx, "GET", "/records/_doc/"+op.ID+"?realtime=true", nil)
		if err != nil {
			return failure("transport_http", false)
		}
		var rec Record
		if code != 200 || json.Unmarshal(raw, &rec) != nil || rec.Index != "records" || rec.ID != op.ID || !rec.Found || rec.Version != 1 || !validPayload(rec.Source, op.ID) {
			return failure("payload_or_response", false)
		}
		r := Result{Class: "ok"}
		return r
	}
	body := []byte(fmt.Sprintf("{\"index\":{\"_index\":\"records\",\"_id\":%q}}\n%s\n", op.ID, payload(op.ID)))
	code, raw, err := c.request(ctx, "POST", "/_bulk?refresh=false&wait_for_active_shards=1", body)
	if err != nil {
		return failure("transport_http", true)
	}
	return parseBulk(code, raw, op.ID)
}
func parseBulk(code int, raw []byte, id string) Result {
	var response struct {
		Errors *bool  `json:"errors"`
		Took   *int64 `json:"took"`
		Items  []map[string]struct {
			Index   string                                  `json:"_index"`
			ID      string                                  `json:"_id"`
			Version int                                     `json:"_version"`
			Status  int                                     `json:"status"`
			Result  string                                  `json:"result"`
			Seq     *int64                                  `json:"_seq_no"`
			Term    int                                     `json:"_primary_term"`
			Shards  struct{ Total, Successful, Failed int } `json:"_shards"`
			Error   json.RawMessage                         `json:"error"`
		} `json:"items"`
	}
	if code != 200 || json.Unmarshal(raw, &response) != nil || response.Errors == nil || response.Took == nil || *response.Took < 0 || len(response.Items) != 1 || len(response.Items[0]) != 1 {
		return failure("invalid_bulk", true)
	}
	item, ok := response.Items[0]["index"]
	if !ok || item.Index != "records" || item.ID != id {
		return failure("correlation", true)
	}
	if *response.Errors || len(item.Error) > 0 {
		return failure("backend_rejection_unknown", true)
	}
	if item.Status != 201 || item.Result != "created" || item.Version != 1 || item.Seq == nil || *item.Seq < 0 || item.Term < 1 || item.Shards.Total != 1 || item.Shards.Successful != 1 || item.Shards.Failed != 0 {
		return failure("invalid_ack", true)
	}
	r := Result{Class: "ok", Outcome: applied}
	return r
}
