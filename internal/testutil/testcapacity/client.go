package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	MutationsStarted atomic.Uint64
	HTTP             *http.Client
	Transport        *http.Transport
	Backend          string
	MongoBackend     bool
	Connections      []*grpc.ClientConn
	RPC              []pb.StoreServiceClient
	Mongo            *mongo.Client
}

func newClient(backend, target string, pool int) (*Client, error) {
	if pool < 1 || pool > 62 {
		return nil, errors.New("HTTP connection pool bound")
	}
	dialer := &net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		DisableCompression:     true,
		MaxConnsPerHost:        pool,
		MaxIdleConns:           pool,
		MaxIdleConnsPerHost:    pool,
		IdleConnTimeout:        10 * time.Second,
		ResponseHeaderTimeout:  10 * time.Second,
		MaxResponseHeaderBytes: 16384,
	}
	hc := &http.Client{
		Transport:     transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return errors.New("redirect refused") },
	}
	c := &Client{HTTP: hc, Transport: transport, Backend: backend, MongoBackend: strings.HasPrefix(backend, "mongodb://")}
	if c.MongoBackend && target == "" {
		var err error
		c.Mongo, err = newMongoClient(backend, pool)
		if err != nil {
			c.Close()
			return nil, err
		}
	}
	if target != "" {
		for i := 0; i < 4; i++ {
			conn, err := weirclient.Dial(target)
			if err != nil {
				c.Close()
				return nil, err
			}
			c.Connections = append(c.Connections, conn)
			c.RPC = append(c.RPC, pb.NewStoreServiceClient(conn))
		}
	}
	return c, nil
}

func (c *Client) Close() {
	for _, conn := range c.Connections {
		conn.Close()
	}
	c.Transport.CloseIdleConnections()
	if c.Mongo != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		c.Mongo.Disconnect(ctx)
	}
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

// Only fixed diagnostic text is exposed; arbitrary status messages may contain
// a resource name or payload and are represented by a bounded fingerprint.
func failureMessage(err error) string {
	message := status.Convert(err).Message()
	switch {
	case strings.Contains(message, "RST_STREAM with error code: INTERNAL_ERROR"):
		return "http2_internal_reset"
	case errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded:
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled:
		return "canceled"
	}
	diagnostics := []struct{ text, tag string }{
		{text: "invalid execution emission", tag: "invalid_execution_emission"},
		{text: "uncorrelated execution emission", tag: "uncorrelated_execution_emission"},
		{text: "invalid downstream response envelope", tag: "invalid_downstream_response"},
		{text: "unknown or completed downstream request ID", tag: "invalid_downstream_request_id"},
		{text: "missing HTTP/2 delivery lifetime", tag: "missing_delivery_lifetime"},
		{text: "missing ingress context", tag: "missing_ingress_context"},
		{text: "request ID unavailable", tag: "request_id_unavailable"},
	}
	for _, diagnostic := range diagnostics {
		if strings.Contains(message, diagnostic.text) {
			return diagnostic.tag
		}
	}
	digest := sha256.Sum256([]byte(message))
	return "sha256:" + hex.EncodeToString(digest[:8])
}

func (c *Client) Call(ctx context.Context, op Operation) Result {
	if op.Write {
		c.MutationsStarted.Add(1)
	}
	if ctx.Err() != nil {
		r := Result{Class: "deadline_before_call", Outcome: notStarted}
		return r
	}
	if len(c.RPC) == 0 {
		return c.direct(ctx, op)
	}
	client := c.RPC[op.Number%len(c.RPC)]
	resource := "records/s:" + op.ID
	media := "application/json"
	data := payload(op.ID)
	if c.MongoBackend {
		resource = "weir_load/records/s:" + op.ID
		media = "application/bson"
		data = mongoPayload(op.ID)
	}
	if !op.Write {
		req := &pb.ReadRequest{Resource: resource, ReadMediaType: media}
		variant := &pb.Call_Read{Read: req}
		call := &pb.Call{Version: 1, Operation: variant}
		opts := weirclient.RecordOptions{StoreName: "records", Call: call}
		result, err := weirclient.Record(ctx, client, opts)
		resp := result.GetRead()
		if err != nil {
			r := failure("transport_"+status.Code(err).String(), false)
			r.Message = failureMessage(err)
			return r
		}
		if f := resp.GetFailure(); f != nil {
			return failure("protocol_"+f.Code.String(), false)
		}
		if resp.GetDocument() == nil || resp.GetDocument().MediaType != media || !bytes.Equal(resp.GetDocument().Data, data) {
			return failure("payload_or_response", false)
		}
		r := Result{Class: "ok"}
		return r
	}
	doc := &pb.Document{MediaType: media, Data: data}
	action := &pb.MutateRequest_Put{Put: doc}
	req := &pb.MutateRequest{Resource: resource, Action: action}
	variant := &pb.Call_Mutate{Mutate: req}
	call := &pb.Call{Version: 1, Operation: variant}
	opts := weirclient.RecordOptions{StoreName: "records", Call: call}
	result, err := weirclient.Record(ctx, client, opts)
	resp := result.GetMutation()
	r := Result{Outcome: unknown, Class: "ok"}
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
		r.Class = "invalid_outcome"
	}
	// Record preserves a validated result even when its end frame or trailers
	// are lost. RPC failure is distinct from irreversible mutation evidence.
	if err != nil {
		r.Class = "transport_" + status.Code(err).String()
		r.Message = failureMessage(err)
		return r
	}
	if r.Class != "ok" {
		return r
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
	if c.Mongo != nil {
		return c.directMongo(ctx, op)
	}
	if !op.Write {
		code, raw, err := c.request(ctx, "GET", "/records/_doc/"+op.ID+"?realtime=true", nil)
		if err != nil {
			return failure("transport_http", false)
		}
		if code == 429 || code == 503 {
			return failure(fmt.Sprintf("backend_http_%d", code), false)
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
	if code == 429 || code == 503 {
		var envelope struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if json.Unmarshal(raw, &envelope) == nil && knownRejection(code, envelope.Error.Type) {
			r := Result{Class: "backend_rejected_not_applied", Outcome: notApplied}
			return r
		}
		return failure(fmt.Sprintf("backend_http_%d", code), true)
	}
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
	if code != 200 ||
		json.Unmarshal(raw, &response) != nil ||
		response.Errors == nil ||
		response.Took == nil ||
		*response.Took < 0 ||
		len(response.Items) != 1 ||
		len(response.Items[0]) != 1 {
		return failure("invalid_bulk", true)
	}
	item, ok := response.Items[0]["index"]
	if !ok || item.Index != "records" || item.ID != id {
		return failure("correlation", true)
	}
	if *response.Errors || len(item.Error) > 0 {
		var rejection struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(item.Error, &rejection) == nil && knownRejection(item.Status, rejection.Type) {
			r := Result{Class: "backend_rejected_not_applied", Outcome: notApplied}
			return r
		}
		return failure("backend_rejection_unknown", true)
	}
	if item.Status != 201 ||
		item.Result != "created" ||
		item.Version != 1 ||
		item.Seq == nil ||
		*item.Seq < 0 ||
		item.Term < 1 ||
		item.Shards.Total != 1 ||
		item.Shards.Successful != 1 ||
		item.Shards.Failed != 0 {
		return failure("invalid_ack", true)
	}
	r := Result{Class: "ok", Outcome: applied}
	return r
}

func knownRejection(code int, kind string) bool {
	return code == 429 && kind == "es_rejected_execution_exception" || code == 503 && kind == "unavailable_shards_exception"
}
