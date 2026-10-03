package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	arrivalExpiry = 20 * time.Millisecond
	catchupLimit  = 8
	callLifetime  = time.Second
)

const (
	dropped byte = iota
	applied
	unknown
	notApplied
	notStarted
)

type Operation struct {
	ID       string
	Write    bool
	Number   int
	Planned  time.Time
	Decision time.Time
}

type Result struct {
	Outcome byte
	Class   string
	Message string
}

type Metrics struct {
	Planned         uint64            `json:"planned"`
	Started         uint64            `json:"started"`
	Completed       uint64            `json:"completed"`
	Success         uint64            `json:"success"`
	Drop            uint64            `json:"client_drop"`
	Late            uint64            `json:"client_late"`
	Unknown         uint64            `json:"unknown"`
	Due             uint64            `json:"due"`
	CancelledFuture uint64            `json:"cancelled_future"`
	WorkerExpired   uint64            `json:"worker_expired"`
	DropReasons     map[string]uint64 `json:"drop_reasons"`
	Wake            Histogram         `json:"wake"`
	Decision        Histogram         `json:"decision"`
	Construct       Histogram         `json:"construct"`
	Handoff         Histogram         `json:"handoff"`
	WorkerStart     Histogram         `json:"worker_start"`
	Failures        map[string]uint64 `json:"failures"`
	ErrorMessages   map[string]uint64 `json:"error_messages,omitempty"`
	SuccessArrival  Histogram         `json:"success_arrival"`
	SuccessDispatch Histogram         `json:"success_dispatch"`
	Arrival         Histogram         `json:"arrival"`
	Dispatch        Histogram         `json:"dispatch"`
	Lag             Histogram         `json:"lag"`
}

type Window struct {
	All  Metrics `json:"all"`
	Read Metrics `json:"read"`
	Put  Metrics `json:"put"`
}

type TrialOptions struct {
	Rate            int
	WarmSeconds     int
	Seconds         int
	Prefix          string
	Workers         int
	WriteEvery      int
	ArrivalExpiryMS int
	MaxCatchup      int
	ClientQueue     int
	TimingOnly      bool
	LegacyExpiry    bool
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
		m.SuccessArrival.Add(end.Sub(op.Planned))
		m.SuccessDispatch.Add(end.Sub(dispatch))
	} else {
		if m.Failures == nil {
			m.Failures = map[string]uint64{}
		}
		m.Failures[r.Class]++
		if r.Message != "" {
			if m.ErrorMessages == nil {
				m.ErrorMessages = map[string]uint64{}
			}
			message := r.Message
			// Keep at most 16 distinct diagnostics plus one overflow counter.
			if m.ErrorMessages[message] == 0 && len(m.ErrorMessages) >= 16 {
				message = "other"
			}
			m.ErrorMessages[message]++
		}
	}
	if r.Outcome == unknown {
		m.Unknown++
	}
}

type Decision struct {
	Operation   Operation
	Wake        time.Time
	Constructed time.Time
	Reason      string
	Future      bool
}

func (w *Window) plan(d Decision) {
	ms := []*Metrics{&w.All, &w.Read}
	if d.Operation.Write {
		ms[1] = &w.Put
	}
	for _, m := range ms {
		m.Planned++
		if d.Future {
			m.CancelledFuture++
		} else {
			m.Due++
			m.Wake.Add(d.Wake.Sub(d.Operation.Planned))
			m.Construct.Add(d.Constructed.Sub(d.Wake))
			m.Decision.Add(d.Operation.Decision.Sub(d.Operation.Planned))
		}
		if d.Reason != "" {
			m.Drop++
			if m.DropReasons == nil {
				m.DropReasons = map[string]uint64{}
			}
			m.DropReasons[d.Reason]++
			if d.Reason == "expired" {
				m.Late++
			}
		}
	}
}

func (w *Window) worker(op Operation, start time.Time, expired bool) {
	ms := []*Metrics{&w.All, &w.Read}
	if op.Write {
		ms[1] = &w.Put
	}
	for _, m := range ms {
		m.Handoff.Add(start.Sub(op.Decision))
		m.WorkerStart.Add(start.Sub(op.Planned))
		if expired {
			m.Drop++
			m.WorkerExpired++
			if m.DropReasons == nil {
				m.DropReasons = map[string]uint64{}
			}
			m.DropReasons["worker_deadline_or_cancel"]++
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
	if opts.WriteEvery == 0 {
		opts.WriteEvery = 10
	}
	if opts.ArrivalExpiryMS < 0 || opts.ArrivalExpiryMS > 100 || opts.MaxCatchup < 0 || opts.MaxCatchup > 512 || opts.ClientQueue < 0 || opts.ClientQueue > 512 {
		return nil, errors.New("pacing bounds")
	}
	if opts.Rate < 1 ||
		opts.Rate > 6400 ||
		opts.Seconds < 1 ||
		opts.Seconds > 120 ||
		opts.WarmSeconds < 0 ||
		opts.WarmSeconds > 20 ||
		opts.Workers < 1 ||
		opts.Workers > 64 ||
		(opts.WriteEvery != 1 && opts.WriteEvery != 10) ||
		opts.Rate*(opts.WarmSeconds+opts.Seconds)%opts.WriteEvery != 0 ||
		opts.Rate*(opts.WarmSeconds+opts.Seconds)/opts.WriteEvery > 300000 ||
		(opts.LegacyExpiry && !opts.TimingOnly) {
		return nil, errors.New("trial bounds")
	}
	total := opts.Rate * (opts.WarmSeconds + opts.Seconds)
	t := &Trial{
		Options: opts,
		Ledger:  make([]byte, total/opts.WriteEvery),
		Planned: total,
		Windows: make([]Window, (opts.Seconds+9)/10),
	}
	jobs := make(chan Operation, opts.ClientQueue)
	var mu sync.Mutex
	var wg sync.WaitGroup
	windows := func(op Operation) []*Window {
		if op.Number < opts.Rate*opts.WarmSeconds {
			return []*Window{&t.Warm}
		}
		return []*Window{&t.Measure, &t.Windows[(op.Number/opts.Rate-opts.WarmSeconds)/10]}
	}
	for worker := 0; worker < opts.Workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := range jobs {
				start := time.Now()
				call, cancel := context.WithDeadline(ctx, op.Planned.Add(callLifetime))
				dispatch := time.Now()
				expired := call.Err() != nil || !dispatch.Before(op.Planned.Add(callLifetime))
				result := Result{Class: "ok", Outcome: applied}
				if !expired && !opts.TimingOnly {
					result = client.Execute(call, op)
				}
				end := time.Now()
				cancel()
				mu.Lock()
				for _, w := range windows(op) {
					w.worker(op, start, expired)
					if !expired {
						w.finish(result, op, dispatch, end)
					}
				}
				if op.Write && !expired {
					t.Ledger[op.Number/opts.WriteEvery] = result.Outcome
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
	catchup := 0
	expiry := arrivalExpiry
	if opts.ArrivalExpiryMS != 0 {
		expiry = time.Duration(opts.ArrivalExpiryMS) * time.Millisecond
	}
	maxCatchup := catchupLimit
	if opts.MaxCatchup != 0 {
		maxCatchup = opts.MaxCatchup
	}
	if opts.LegacyExpiry {
		expiry = 5 * time.Millisecond
	}
	for n := 0; n < total; n++ {
		scheduled := t.Start.Add(time.Duration(int64(n) * int64(time.Second) / int64(opts.Rate)))
		waited := false
		if wait := time.Until(scheduled); wait > 0 && ctx.Err() == nil {
			timer.Reset(wait)
			waited = true
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
		// Wake is the first observation after the timer select (or overdue loop).
		wake := time.Now()
		future := ctx.Err() != nil && wake.Before(scheduled)
		write := n%opts.WriteEvery == opts.WriteEvery-1
		id := readID(n)
		if write {
			id = fmt.Sprintf("%s-%06d", opts.Prefix, n/opts.WriteEvery)
		}
		constructed := time.Now()
		op := Operation{ID: id, Write: write, Number: n, Planned: scheduled}
		mu.Lock()
		op.Decision = time.Now()
		d := Decision{Operation: op, Wake: wake, Constructed: constructed, Future: future}
		if waited {
			catchup = 0
		}
		switch {
		case ctx.Err() != nil:
			d.Reason = "cancelled"
		case op.Decision.Sub(scheduled) > expiry:
			d.Reason = "expired"
		case !opts.LegacyExpiry && catchup >= maxCatchup:
			d.Reason = "catchup_bound"
		default:
			catchup++
			select {
			case jobs <- op:
			default:
				d.Reason = "no_worker"
				if opts.ClientQueue != 0 {
					d.Reason = "client_queue_full"
				}
			}
		}
		for _, w := range windows(op) {
			w.plan(d)
		}
		mu.Unlock()
	}
	timer.Stop()
	close(jobs)
	wg.Wait()
	t.End = time.Now()
	for _, m := range []*Metrics{&t.Warm.All, &t.Measure.All} {
		if m.Planned != m.Completed+m.Drop || m.Completed != m.Started || m.Planned != m.Due+m.CancelledFuture {
			return t, errors.New("count identity")
		}
	}
	return t, ctx.Err()
}
