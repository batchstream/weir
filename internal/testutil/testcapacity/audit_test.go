package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPartialSetupRecordsEverySingleAttempt(t *testing.T) {
	count := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" {
			fmt.Fprint(w, "{}")
			return
		}
		count++
		if count == 17 {
			w.WriteHeader(503)
			return
		}
		data, err := io.ReadAll(io.LimitReader(r.Body, 4096))
		if err != nil {
			t.Error(err)
			return
		}
		line := bytes.SplitN(data, []byte("\n"), 2)[0]
		var action map[string]map[string]string
		if err := json.Unmarshal(line, &action); err != nil {
			t.Error(err)
			return
		}
		fmt.Fprintf(
			w,
			`{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":%q,"_version":1,"status":201,"result":"created","_seq_no":0,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`,
			action["index"]["_id"],
		)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := newClient(server.URL, "", 62)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	err = client.setup(context.Background(), encoder)
	if err == nil || count != 17 || client.MutationsStarted.Load() != 17 {
		t.Fatal(err, count, client.MutationsStarted.Load())
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != 17 || !bytes.Contains(lines[16], []byte(`"started":17`)) {
		t.Fatal(output.String())
	}
}
