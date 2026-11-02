package store

import (
	"math"
	"math/rand/v2"
	"slices"
	"time"
)

// A Store adapts how many record batches that are not full it runs at once.
// Each replica of a group sends a batch whenever an execution slot is free, so
// without this a shared backend would receive more and smaller requests as
// replicas are added or as it slows down, and slow down further. The limit
// compares each execution's time with the fastest recent execution of a batch
// of the same group and size. When the backend queues while this Store
// contends for it, executions take longer than that baseline and the limit
// halves, so records wait and share larger batches; while the limit holds
// batches back and executions stay near the baseline, it grows again. Replicas sharing a backend see the same queueing
// and back off together, so their total follows what the backend can take
// rather than how many replicas there are. Below one, the limit runs one batch
// at a time and leaves the backend idle in between, so a replica's callers
// share one batch even when one execution per replica is more than the backend
// can take. Full batches already carry as many records as one execution can
// and may use every slot up to max_in_flight.
//
// A replica that starts while the backend queues sees only slow executions and
// would take them for its baseline. So every replica that kept record batches
// running, or whose limit held one back, pauses at the same wall-clock
// instants, letting the backend empty, and then sends one probe batch at a
// random moment; the probe's time renews the baseline.
const (
	// queueingRatio is the median execution time over its baseline, across
	// one window, that halves the limit. Backends vary below it without
	// queueing: an Elasticsearch execution waits for a shared translog fsync,
	// and a CPU-throttled replica stalls its own executions. A busy backend
	// also queues a little before it is saturated, and backing off there
	// gives up throughput.
	queueingRatio = 4.0
	// limitGrowth is how much a window that held a batch back raises the limit,
	// in batches above one and as a share of the limit below.
	limitGrowth = 0.5
	// lowestLimit is the floor when min_in_flight is zero: one batch running
	// a quarter of the time. Lower floors make callers wait longer between
	// batches without making them larger.
	lowestLimit = 1.0 / 4
	// limitWindow is the shortest decision window. A window also lasts at
	// least the typical execution time, so a change shows before the next.
	limitWindow = 10 * time.Millisecond
	// baselineAge is how long the fastest execution of a generation is kept,
	// or the probe interval when that is longer. A baseline expires after one
	// to two ages, so a backend that became slower for good regains its limit.
	baselineAge = 15 * time.Second
	// sizeClasses holds batch sizes 1, 2–3, 4–7 and so on, in records or in
	// KiB when that is larger; larger sizes share the last class.
	sizeClasses = 16
	// longestIdle bounds the idle time between batches below one, which
	// follows the typical execution time that one stalled execution raises.
	longestIdle = time.Second
	// probeInterval separates probes; they start at multiples of it on the
	// wall clock, so the replicas of a group pause together. It doubles until
	// a pause takes at most probeShare of it. Replicas whose executions differ
	// may choose neighboring intervals, and the longer one's instants are
	// still instants of the shorter one.
	probeInterval = 10 * time.Second
	probeShare    = 0.025
	// A probe pauses twice the typical execution time, within these bounds,
	// and then sends its batch within one typical execution time. A backend
	// that does not empty within the longest pause still runs the probe faster
	// than its queue did, and the lower limit that follows shortens the next
	// pause's wait.
	probeDrainMin, probeDrainMax   = 20 * time.Millisecond, time.Second
	probeWindowMin, probeWindowMax = 5 * time.Millisecond, 250 * time.Millisecond
)

type executionLimit struct {
	min, max float64
	value    float64
	// fastest holds each batch group's shortest executions by size class in
	// the current (0) and previous generation.
	fastest    map[groupKey]*[2][sizeClasses]time.Duration
	generation time.Time
	// typical is a moving average of execution time.
	typical time.Duration
	// The current window. A window contended for the backend when the limit
	// held a batch back or batches ran together.
	windowStart        time.Time
	ratios             []float64
	heldBack, together bool
	// cutAt is when the limit last halved; executions that started earlier
	// show the old limit and do not count.
	cutAt time.Time
	// ratio is the last window's median execution time over its baseline.
	ratio float64
	// lastHeld is when the limit last held a batch back.
	lastHeld time.Time
	// lastFinished is when the last observed batch that was not full finished.
	lastFinished time.Time
	// idle is set while the Store runs and queues no record batch, and
	// busySince is when it last stopped being idle.
	idle      bool
	busySince time.Time
	probe     probe
	// interval is the shortest probe interval and every the current one,
	// whose pause takes at most share of it.
	interval, every time.Duration
	share           float64
}

// probe is the schedule of the current probe interval.
type probe struct {
	start time.Time
	// active is set when the limit held a batch back in the previous interval.
	active bool
	// Batches wait until sendAt, one probe batch may then start, and the
	// rest wait until end.
	sendAt, end time.Time
	sent        bool
}

// gate is what a probe allows now.
type gate int

const (
	gateOpen gate = iota
	gateClosed
	gateProbe
)

// finishedBatch is one record batch the adapter executed.
type finishedBatch struct {
	started, finished time.Time
	key               groupKey
	size, bytes       int
	full, probe       bool
}

func newExecutionLimit(minimum, maximum int, now time.Time) executionLimit {
	floor := float64(minimum)
	if minimum == 0 {
		floor = lowestLimit
	}
	limit := executionLimit{min: floor, max: float64(maximum), value: float64(maximum), fastest: make(map[groupKey]*[2][sizeClasses]time.Duration), generation: now, windowStart: now, idle: true, interval: probeInterval, every: probeInterval, share: probeShare}
	return limit
}

// admits reports whether a batch that is not full may start while running
// such batches execute. Otherwise it returns when one may, or the zero time
// when that waits for an execution to finish. Below one these batches leave
// the backend idle in proportion: at a half, as long as one typically takes.
func (l *executionLimit) admits(running int, now time.Time) (bool, time.Time) {
	var finish time.Time
	if l.value >= 1 {
		return float64(running) < math.Floor(l.value), finish
	}
	if running > 0 {
		return false, finish
	}
	idle := min(time.Duration(float64(l.typical)*(1/l.value-1)), longestIdle)
	next := l.lastFinished.Add(idle)
	if !now.Before(next) {
		return true, finish
	}
	return false, next
}

// holdBack records that the limit kept a ready batch waiting.
func (l *executionLimit) holdBack(now time.Time) {
	l.heldBack = true
	l.lastHeld = now
}

// overlap records that a record batch started while another ran.
func (l *executionLimit) overlap() {
	l.together = true
}

// busy records that a record batch started.
func (l *executionLimit) busy(now time.Time) {
	if l.idle {
		l.idle, l.busySince = false, now
	}
}

// rest records that the Store runs and queues no record batch. A probe's
// pause empties a Store without making it idle.
func (l *executionLimit) rest(now time.Time) {
	p := &l.probe
	if p.active && !now.Before(p.start) && now.Before(p.end) {
		return
	}
	l.idle = true
}

// current is the limit in running batches: whole numbers from one, and below
// one the share of time one batch runs.
func (l *executionLimit) current() float64 {
	if l.value >= 1 {
		return math.Floor(l.value)
	}
	return l.value
}

// gate reports whether batches may start now and, while they wait, until when.
func (l *executionLimit) gate(now time.Time) (gate, time.Time) {
	drain := min(max(2*l.typical, probeDrainMin), probeDrainMax)
	window := min(max(l.typical, probeWindowMin), probeWindowMax)
	every := l.interval
	for float64(drain+window) > l.share*float64(every) {
		every *= 2
	}
	start := now.Truncate(every)
	p := &l.probe
	l.every = every
	if !p.start.Equal(start) {
		previous := start.Add(-every)
		held := !l.lastHeld.IsZero() && !l.lastHeld.Before(previous)
		busy := !l.idle && !l.busySince.After(previous)
		active := held || busy
		*p = probe{start: start, active: active, sendAt: start.Add(drain + rand.N(window)), end: start.Add(drain + window)}
	}
	var open time.Time
	if !p.active || !now.Before(p.end) {
		return gateOpen, open
	}
	if p.sent || now.Before(p.sendAt) {
		until := p.end
		if !p.sent {
			until = p.sendAt
		}
		return gateClosed, until
	}
	return gateProbe, p.end
}

// sendProbe records that the probe batch started.
func (l *executionLimit) sendProbe() {
	l.probe.sent = true
}

// sizeClass is the class of a batch of size records and bytes.
func sizeClass(size, bytes int) int {
	size = max(size, (bytes+1023)/1024)
	class := 0
	for size > 1 && class < sizeClasses-1 {
		size >>= 1
		class++
	}
	return class
}

// baseline is the shortest recent execution of a batch of this group and size
// class or larger, since a larger batch is never faster on an idle backend.
func (l *executionLimit) baseline(key groupKey, class int) time.Duration {
	var fastest time.Duration
	generations := l.fastest[key]
	if generations == nil {
		return fastest
	}
	for c := class; c < sizeClasses; c++ {
		for g := range generations {
			if d := generations[g][c]; d > 0 && (fastest == 0 || d < fastest) {
				fastest = d
			}
		}
	}
	return fastest
}

func (l *executionLimit) observe(e finishedBatch) {
	elapsed := max(e.finished.Sub(e.started), time.Microsecond)
	if e.finished.Sub(l.generation) >= max(baselineAge, l.every) {
		// Groups seen in neither generation are forgotten.
		for key, generations := range l.fastest {
			if generations[0] == [sizeClasses]time.Duration{} {
				delete(l.fastest, key)
				continue
			}
			generations[1], generations[0] = generations[0], [sizeClasses]time.Duration{}
		}
		l.generation = e.finished
	}
	generations := l.fastest[e.key]
	if generations == nil {
		generations = new([2][sizeClasses]time.Duration)
		l.fastest[e.key] = generations
	}
	class := sizeClass(e.size, e.bytes)
	if fastest := generations[0][class]; fastest == 0 || elapsed < fastest {
		generations[0][class] = elapsed
	}
	if !e.full && e.finished.After(l.lastFinished) {
		l.lastFinished = e.finished
	}
	if e.probe {
		return
	}
	if l.typical == 0 {
		l.typical = elapsed
	} else {
		l.typical += (elapsed - l.typical) / 8
	}
	if e.started.Before(l.cutAt) {
		return
	}
	l.ratios = append(l.ratios, float64(elapsed)/float64(l.baseline(e.key, class)))
	if e.finished.Sub(l.windowStart) < max(limitWindow, l.typical) {
		return
	}
	// The lower median, so one stalled execution of two does not decide.
	slices.Sort(l.ratios)
	l.ratio = l.ratios[(len(l.ratios)-1)/2]
	// A Store that runs one batch at a time without holding any back does not
	// contend for the backend, and executions slower than the fastest of
	// their group then show different work rather than queueing.
	switch {
	case l.ratio > queueingRatio && (l.heldBack || l.together):
		l.value = max(l.min, l.value/2)
		l.cutAt = e.finished
	case l.heldBack:
		l.value = min(l.max, l.value+limitGrowth*min(l.value, 1))
	}
	l.windowStart, l.ratios, l.heldBack, l.together = e.finished, l.ratios[:0], false, false
}
