package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Every trial recreates only records in the exclusively owned ES container.
// All operations, including setup and audit, are single attempts.
func (c *Client) setup(ctx context.Context, encoder *json.Encoder) error {
	code, _, err := c.request(ctx, "DELETE", "/records", nil)
	if err != nil || (code != 200 && code != 404) {
		return fmt.Errorf("reset status=%d: %w", code, err)
	}
	mapping := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"1s","translog.durability":"request"},"mappings":{"enabled":false}}`)
	code, raw, err := c.request(ctx, "PUT", "/records", mapping)
	if err != nil || code != 200 {
		return fmt.Errorf("create status=%d %s: %w", code, raw, err)
	}
	for i := 0; i < corpusSize; i++ {
		op := Operation{ID: fmt.Sprintf("read-%04d", i), Write: true}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		progress := map[string]any{"type": "setup_progress", "started": i + 1}
		if err := encoder.Encode(progress); err != nil {
			return err
		}
		c.MutationsStarted.Add(1)
		r := c.direct(ctx, op)
		if r.Class != "ok" {
			return fmt.Errorf("seed %d: %s", i, r.Class)
		}
	}
	code, _, err = c.request(ctx, "POST", "/records/_refresh", nil)
	if err != nil || code != 200 {
		return fmt.Errorf("seed refresh %d: %w", code, err)
	}
	return nil
}

type Audit struct {
	Planned      int `json:"planned_writes"`
	Found        int `json:"found_version1"`
	Applied      int `json:"applied"`
	UnknownFound int `json:"unknown_found"`
	Absent       int `json:"absent"`
	Pages        int `json:"pages"`
}

func (c *Client) audit(ctx context.Context, t *Trial) (Audit, error) {
	a := Audit{Planned: len(t.Ledger)}
	for start := 0; start < len(t.Ledger); start += 100 {
		end := min(start+100, len(t.Ledger))
		ids := make([]string, 0, end-start)
		for n := start; n < end; n++ {
			ids = append(ids, fmt.Sprintf("%s-%06d", t.Options.Prefix, n))
		}
		body := struct {
			IDs []string `json:"ids"`
		}{ids}
		raw, err := json.Marshal(body)
		if err != nil {
			return a, err
		}
		page, cancel := context.WithTimeout(ctx, 2*time.Second)
		code, raw, err := c.request(page, "POST", "/records/_mget?realtime=true", raw)
		cancel()
		var response struct {
			Docs []Record `json:"docs"`
		}
		if err != nil || code != 200 || json.Unmarshal(raw, &response) != nil || len(response.Docs) != len(ids) {
			return a, fmt.Errorf("incomplete audit page status=%d: %w", code, err)
		}
		a.Pages++
		for i, rec := range response.Docs {
			outcome := t.Ledger[start+i]
			if rec.Index != "records" || rec.ID != ids[i] || len(rec.Error) != 0 {
				return a, fmt.Errorf("audit correlation %s", ids[i])
			}
			if rec.Found {
				a.Found++
				if rec.Version != 1 || !validPayload(rec.Source, rec.ID) {
					return a, fmt.Errorf("replay/payload %s version=%d", rec.ID, rec.Version)
				}
				if outcome != applied && outcome != unknown {
					return a, fmt.Errorf("false non-application %s outcome=%d", rec.ID, outcome)
				}
				if outcome == unknown {
					a.UnknownFound++
				}
			} else {
				a.Absent++
				if outcome == applied {
					return a, fmt.Errorf("false APPLIED %s", rec.ID)
				}
			}
			if outcome == applied {
				a.Applied++
			}
		}
	}
	return a, nil
}
