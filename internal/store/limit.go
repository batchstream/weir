package store

import (
	"math"
	"math/rand/v2"
	"slices"
	"time"
)

// A Store adapts how many record batches that are not full it runs at once,
// for mutations and for reads apart. Each replica of a group sends a batch whenever an execution slot is free, so
// without this a shared backend would receive more and smaller requests as
// replicas are added or as it slows down, and slow down further. The limit
// compares each execution's time with the fastest recent execution of a batch
// of the same group and size. When the backend queues while this Store
// contends for it, executions take longer than that baseline and the limit
// halves, so records wait and share larger batches; while executions stay
// near the baseline, it grows again. Replicas sharing a backend see the same queueing
// and back off together, so their total follows what the backend can take
// rather than how many replicas there are. Below one, the limit runs one batch
// at a time and leaves the backend idle in between, so a replica's callers
// share one batch even when one execution per replica is more than the backend
// can take. Full batches already carry as many records as one execution can
// and may use every slot up to max_in_flight.
//
// A replica that starts while the backend queues sees only slow executions and
// would take them for its baseline. So every replica that kept record batches
// running, or whose limit held one back, pauses all its record batches at the
// same wall-clock instants, letting the backend empty, and then sends one probe
// batch of each operation at a random moment; the probe's time renews the
// baseline. A limit starts at two batches and grows only after its Store's
// second probe, at an instant of the interval its executions choose, which
// replicas in front of the same backend share. So replicas added in front of a
// queueing backend do not flood it before a probe shows them the queue, and
// those in front of a backend with room soon use it.
const (
	// queueingRatio is the median execution time over its baseline, across
	// one window, that halves the limit. Backends vary below it without
	// queueing: an Elasticsearch execution waits for a shared translog fsync,
	// and a CPU-throttled replica stalls its own executions. A busy backend
	// also queues a little before it is saturated, and backing off there
	// gives up throughput.
	queueingRatio = 4.0
	// limitGrowth is how much a window without queueing raises the limit, in
	// batches above one and as a share of the limit below.
	limitGrowth = 0.5
	// lowestLimit is the floor when min_in_flight is zero: one batch running
	// a quarter of the time. Lower floors make callers wait longer between
	// batches without making them larger.
	lowestLimit = 1.0 / 4
	// startingLimit is where a limit starts: few batches, so that replicas
	// added in front of a queueing backend do not flood it, and more than one,
	// so that one slow execution does not hold the others back.
	startingLimit = 2.0
	// limitWindow is the shortest decision window. A window also lasts at
	// least the typical execution time, so a change shows before the next,
	// and holds windowExecutions, so a few slower records, such as the reads
	// of a large document, do not decide it.
	limitWindow      = 10 * time.Millisecond
	windowExecutions = 8
	// baselineAge is how long the fastest execution of a generation is kept,
	// or the probe interval when that is longer. A baseline rises at most
	// twofold per age, so one probe into a backend that did not empty cannot
	// hide its queue, and a backend that became slower for good regains its
	// limit within a few ages.
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
	// A probe pauses twice the longer typical execution time of mutations and
	// reads, within these bounds, and then sends their batches within one such
	// time. A backend that does not empty within the longest pause still runs
	// the probe faster than its queue did, and the lower limit that follows
	// shortens the next pause's wait.
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
	// lastFinished is when the last observed batch that was not full finished,
	// and lastStarted when the last batch the limit counts started.
	lastFinished, lastStarted time.Time
	// idle is set while the Store runs and queues no batch of the limit's
	// operation, and busySince is when it last stopped being idle.
	idle      bool
	busySince time.Time
	// lastSeen is when the last observed execution finished.
	lastSeen time.Time
	// settled is set once the execution of a probe at an instant every
	// replica's schedule includes was observed; the limit grows only after it.
	// starting is set until the limit first halves after that, and until then
	// a window without queueing doubles it.
	settled, starting bool
	// every is the Store's current probe interval.
	every time.Duration
}

// probes is the probe schedule of a Store's limits. Its record batches pause
// together, so that a pause empties the backend of mutations and reads alike,
// and each operation then sends one probe batch.
type probes struct {
	limits *[2]executionLimit
	// interval is the shortest probe interval and every the current one,
	// whose pause takes at most share of it.
	interval, every time.Duration
	share           float64
	start           time.Time
	// active is set when a limit held a batch back, or its operation kept
	// batches running, through the previous interval. paused is set once an
	// active pause ended.
	active, paused bool
	// Batches of each operation wait until its sendAt, one probe batch of it
	// may then start, and the rest wait until end.
	sendAt [2]time.Time
	end    time.Time
	sent   [2]bool
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
	// settles marks a probe after the Store's first pause.
	settles bool
}

func newExecutionLimit(minimum, maximum int, now time.Time) executionLimit {
	floor := float64(minimum)
	if minimum == 0 {
		floor = lowestLimit
	}
	// A replica that starts cannot tell how much its backend already queues,
	// so it runs few batches until a probe shows that.
	start := min(float64(maximum), max(floor, startingLimit))
	limit := executionLimit{min: floor, max: float64(maximum), value: start, fastest: make(map[groupKey]*[2][sizeClasses]time.Duration), generation: now, windowStart: now, idle: true, every: probeInterval, starting: true}
	return limit
}

func newProbes(limits *[2]executionLimit) probes {
	schedule := probes{limits: limits, interval: probeInterval, every: probeInterval, share: probeShare}
	return schedule
}

// admits reports whether a batch that is not full may start while running
// such batches execute. Otherwise it returns when one may, or the zero time
// when that waits for an execution to finish. Below one these batches leave
// the backend idle in proportion: at a half, as long as one typically takes.
// Until the limit settles, and while it is at least one, another batch may
// also start once queueingRatio typical execution times, and at most a second,
// have passed since the last one started, so that slow executions do not hold
// the others back merely because the Store has yet to learn its backend. Each
// further batch above the limit waits that much longer, so that executions of
// several seconds do not add up to the flood the low start avoids.
func (l *executionLimit) admits(running int, now time.Time) (bool, time.Time) {
	var next time.Time
	switch {
	case l.value >= 1 && float64(running) < math.Floor(l.value):
		return true, next
	case l.value < 1 && running == 0:
		idle := min(time.Duration(float64(l.typical)*(1/l.value-1)), longestIdle)
		next = l.lastFinished.Add(idle)
	case l.settled || l.value < 1:
		return false, next
	default:
		overrun := longestIdle
		if l.typical > 0 {
			overrun = min(overrun, time.Duration(queueingRatio*float64(l.typical)))
		}
		next = l.lastStarted.Add(overrun * time.Duration(running-int(l.value)+1))
	}
	if !now.Before(next) {
		var admitted time.Time
		return true, admitted
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

// busy records that a record batch started; counted marks one the limit
// counts.
func (l *executionLimit) busy(now time.Time, counted bool) {
	if l.idle {
		l.idle, l.busySince = false, now
	}
	if counted {
		l.lastStarted = now
	}
}

// recent reports whether an execution of the limit's operation finished
// since a time, so that its typical execution time still applies.
func (l *executionLimit) recent(since time.Time) bool {
	return !l.lastSeen.IsZero() && !l.lastSeen.Before(since)
}

// loaded reports whether the limit held a batch back since a time, or its
// operation kept batches running since then. A limit that has not settled yet
// may have learned its baseline from a queue, so it counts as loaded while it
// runs batches, however recently it started.
func (l *executionLimit) loaded(since time.Time) bool {
	held := !l.lastHeld.IsZero() && !l.lastHeld.Before(since)
	busy := !l.idle && l.recent(since) && (!l.busySince.After(since) || !l.settled)
	return held || busy
}

// current is the limit in running batches: whole numbers from one, and below
// one the share of time one batch runs.
func (l *executionLimit) current() float64 {
	if l.value >= 1 {
		return math.Floor(l.value)
	}
	return l.value
}

// gate reports whether a batch of an operation may start now and, while it
// waits, until when.
func (p *probes) gate(op operation, now time.Time) (gate, time.Time) {
	if p.active && !now.Before(p.end) {
		p.paused = true
	}
	// An operation that stopped running keeps its typical execution time, so
	// only those that ran in the current interval size the pause.
	drain, window := probeDrainMin, probeWindowMin
	for i := range p.limits {
		if !p.limits[i].recent(now.Add(-p.every)) {
			continue
		}
		typical := p.limits[i].typical
		drain = max(drain, min(2*typical, probeDrainMax))
		window = max(window, min(typical, probeWindowMax))
	}
	// A Store that has not paused yet pauses at the next instant of the
	// shortest interval, so replicas that start together soon probe together.
	share := p.share
	if !p.paused {
		share = 1
	}
	every := p.interval
	for float64(drain+window) > share*float64(every) {
		every *= 2
	}
	p.every = every
	for i := range p.limits {
		p.limits[i].every = every
	}
	start := now.Truncate(every)
	if !p.start.Equal(start) {
		previous := start.Add(-every)
		p.start, p.end, p.active = start, start.Add(drain+window), false
		for i := range p.limits {
			p.active = p.active || p.limits[i].loaded(previous)
			p.sendAt[i], p.sent[i] = start.Add(drain+rand.N(window)), false
		}
	}
	var open time.Time
	if !p.pausing(now) {
		return gateOpen, open
	}
	if p.sent[op] {
		return gateClosed, p.end
	}
	if now.Before(p.sendAt[op]) {
		return gateClosed, p.sendAt[op]
	}
	return gateProbe, p.end
}

// pausing reports whether a probe pause holds the Store's record batches.
func (p *probes) pausing(now time.Time) bool {
	return p.active && !now.Before(p.start) && now.Before(p.end)
}

// send records that the probe batch of an operation started.
func (p *probes) send(op operation) {
	p.sent[op] = true
}

// rest records that the Store runs and queues no batch of an operation. A
// pause empties a Store without making it idle.
func (p *probes) rest(op operation, now time.Time) {
	if p.pausing(now) {
		return
	}
	p.limits[op].idle = true
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
			var kept [sizeClasses]time.Duration
			for class, fastest := range generations[0] {
				if fastest > 0 {
					kept[class] = min(fastest, 2*l.baseline(key, class))
				}
			}
			generations[1], generations[0] = kept, [sizeClasses]time.Duration{}
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
	if e.finished.After(l.lastSeen) {
		l.lastSeen = e.finished
	}
	if e.probe {
		l.settled = l.settled || e.settles
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
	if len(l.ratios) < windowExecutions || e.finished.Sub(l.windowStart) < max(limitWindow, l.typical) {
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
		l.starting = l.starting && !l.settled
	case l.ratio <= queueingRatio && l.settled:
		growth := limitGrowth * min(l.value, 1)
		if l.starting {
			growth = l.value
		}
		l.value = min(l.max, l.value+growth)
	}
	l.windowStart, l.ratios, l.heldBack, l.together = e.finished, l.ratios[:0], false, false
}
