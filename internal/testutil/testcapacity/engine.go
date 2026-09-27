package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	dropped byte = iota
	applied
	unknown
	notApplied
	notStarted
)

type Operation struct {
	ID      string
	Write   bool
	Number  int
	Planned time.Time
}
type Result struct {
	Outcome byte
	Class   string
}
type Metrics struct {
	Planned   uint64            `json:"planned"`
	Started   uint64            `json:"started"`
	Completed uint64            `json:"completed"`
	Success   uint64            `json:"success"`
	Drop      uint64            `json:"client_drop"`
	Late      uint64            `json:"client_late"`
	Unknown   uint64            `json:"unknown"`
	Failures  map[string]uint64 `json:"failures"`
	Arrival   Histogram         `json:"arrival"`
	Dispatch  Histogram         `json:"dispatch"`
	Lag       Histogram         `json:"lag"`
}
type Window struct {
	All  Metrics `json:"all"`
	Read Metrics `json:"read"`
	Put  Metrics `json:"put"`
}
type TrialOptions struct {
	Rate        int
	WarmSeconds int
	Seconds     int
	Prefix      string
	Workers     int
}
type Trial struct {
	Options TrialOptions `json:"options"`
	Start   time.Time    `json:"start"`
	End     time.Time    `json:"end"`
	Warm    Window       `json:"warm"`
	Measure Window       `json:"measure"`
	Windows []Window     `json:"ten_second_windows"`
	Ledger  []byte       `json:"-"`
	Planned int          `json:"planned"`
}

func (m *Metrics) finish(r Result, op Operation, dispatch, end time.Time) {
	m.Started++
	m.Completed++
	m.Arrival.Add(end.Sub(op.Planned))
	m.Dispatch.Add(end.Sub(dispatch))
	m.Lag.Add(dispatch.Sub(op.Planned))
	if r.Class == "ok" {
		m.Success++
	} else {
		if m.Failures == nil {
			m.Failures = map[string]uint64{}
		}
		m.Failures[r.Class]++
	}
	if r.Outcome == unknown {
		m.Unknown++
	}
}
func (w *Window) plan(write bool, late bool, drop bool) {
	ms := []*Metrics{&w.All, &w.Read}
	if write {
		ms[1] = &w.Put
	}
	for _, m := range ms {
		m.Planned++
		if drop {
			m.Drop++
		}
		if late {
			m.Late++
		}
	}
}
func (w *Window) finish(r Result, op Operation, dispatch, end time.Time) {
	w.All.finish(r, op, dispatch, end)
	if op.Write {
		w.Put.finish(r, op, dispatch, end)
	} else {
		w.Read.finish(r, op, dispatch, end)
	}
}
func runTrial(ctx context.Context, client *Client, opts TrialOptions) (*Trial, error) {
	if opts.Rate < 1 || opts.Rate > 6400 || opts.Seconds < 1 || opts.Seconds > 120 || opts.WarmSeconds < 0 || opts.WarmSeconds > 20 || opts.Workers < 1 || opts.Workers > 64 || opts.Rate*(opts.WarmSeconds+opts.Seconds)%10 != 0 || opts.Rate*(opts.WarmSeconds+opts.Seconds)/10 > 100000 {
		return nil, errors.New("trial bounds")
	}
	total := opts.Rate * (opts.WarmSeconds + opts.Seconds)
	t := &Trial{Options: opts, Ledger: make([]byte, total/10), Planned: total, Windows: make([]Window, (opts.Seconds+9)/10)}
	jobs := make(chan Operation)
	var mu sync.Mutex
	var wg sync.WaitGroup
	window := func(op Operation) *Window {
		if op.Number < opts.Rate*opts.WarmSeconds {
			return &t.Warm
		}
		return &t.Measure
	}
	for worker := 0; worker < opts.Workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := range jobs {
				deadline := op.Planned.Add(time.Second)
				call, cancel := context.WithDeadline(ctx, deadline)
				dispatch := time.Now()
				result := client.Call(call, op)
				end := time.Now()
				cancel()
				mu.Lock()
				window(op).finish(result, op, dispatch, end)
				if op.Number >= opts.Rate*opts.WarmSeconds {
					t.Windows[(op.Number/opts.Rate-opts.WarmSeconds)/10].finish(result, op, dispatch, end)
				}
				if op.Write {
					t.Ledger[op.Number/10] = result.Outcome
				}
				mu.Unlock()
			}
		}()
	}
	t.Start = time.Now().Add(20 * time.Millisecond)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	for n := 0; n < total; n++ {
		scheduled := t.Start.Add(time.Duration(int64(n) * int64(time.Second) / int64(opts.Rate)))
		if wait := time.Until(scheduled); wait > 0 {
			timer.Reset(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
		}
		write := n%10 == 9
		id := readID(n)
		if write {
			id = fmt.Sprintf("%s-%06d", opts.Prefix, n/10)
		}
		op := Operation{ID: id, Write: write, Number: n, Planned: scheduled}
		late := time.Since(scheduled) > 5*time.Millisecond
		drop := true
		mu.Lock()
		if !late && ctx.Err() == nil {
			select {
			case jobs <- op:
				drop = false
			default:
			}
		}
		window(op).plan(write, late, drop)
		if n >= opts.Rate*opts.WarmSeconds {
			t.Windows[(n/opts.Rate-opts.WarmSeconds)/10].plan(write, late, drop)
		}
		mu.Unlock()
	}
	timer.Stop()
	close(jobs)
	wg.Wait()
	t.End = time.Now()
	for _, m := range []*Metrics{&t.Warm.All, &t.Measure.All} {
		if m.Planned != m.Completed+m.Drop || m.Completed != m.Started {
			return t, errors.New("count identity")
		}
	}
	return t, ctx.Err()
}
