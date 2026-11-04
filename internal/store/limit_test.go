package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// Batch groups of the executions below.
var (
	mutations = groupKey{batch: "records"}
	reads     = groupKey{batch: "records", read: true}
)

// executions describes back-to-back record batches of one group and size.
// Unless they are alone, each overlaps another batch of the Store.
type executions struct {
	count   int
	size    int
	bytes   int
	elapsed time.Duration
	key     groupKey
	// held marks that the limit held a ready batch back before each one.
	held, alone bool
}

// unit is a tenth of the shortest window, which the executions below are
// measured in.
const unit = limitWindow / 10

// times is ratio times an execution time.
func times(elapsed time.Duration, ratio float64) time.Duration {
	return time.Duration(ratio * float64(elapsed))
}

// Executions at 5 units are idle, and these run under and over queueingRatio
// times as long.
var (
	varied = times(5*unit, queueingRatio-0.2)
	queued = times(5*unit, queueingRatio+1)
)

type limitFeed struct {
	limit executionLimit
	now   time.Time
}

// newLimitFeed returns a limit that settled at max_in_flight, as one in front
// of a backend that did not queue.
func newLimitFeed(minimum, maximum int) *limitFeed {
	now := time.Unix(1_000_000, 0)
	limit := newExecutionLimit(minimum, maximum, now)
	limit.value, limit.settled = limit.max, true
	feed := &limitFeed{limit: limit, now: now}
	return feed
}

func (f *limitFeed) run(batches executions) {
	for range batches.count {
		if batches.held {
			f.limit.lastHeld = f.now
		}
		if !batches.alone {
			f.limit.together = true
		}
		done := finishedBatch{started: f.now, finished: f.now.Add(batches.elapsed), key: batches.key, size: batches.size, bytes: batches.bytes}
		f.limit.observe(done)
		f.now = done.finished
	}
}

func TestLimitHalvesWhileExecutionsQueue(t *testing.T) {
	feed := newLimitFeed(1, 16)
	idle := executions{count: windowExecutions, size: 8, elapsed: 5 * unit}
	feed.run(idle)
	if got := feed.limit.current(); got != 16 {
		t.Fatal("executions at their baseline lowered the limit to", got)
	}
	// Executions vary below queueingRatio without queueing.
	below := executions{count: windowExecutions, size: 8, elapsed: varied}
	feed.run(below)
	if got := feed.limit.current(); got != 16 {
		t.Fatal("executions under queueingRatio times their baseline lowered the limit to", got)
	}
	over := executions{count: windowExecutions, size: 8, elapsed: queued}
	feed.run(over)
	if got := feed.limit.current(); got != 8 {
		t.Fatal("executions over queueingRatio times their baseline left the limit at", got)
	}
	// Executions that started before the cut show the old limit.
	before := finishedBatch{started: feed.now.Add(-unit), finished: feed.now.Add(100 * unit), key: mutations, size: 8}
	feed.limit.observe(before)
	if got := feed.limit.current(); got != 8 {
		t.Fatal("an execution started before the cut lowered the limit to", got)
	}
	feed.now = before.finished
	over.count = 10 * windowExecutions
	feed.run(over)
	if got := feed.limit.current(); got != 1 {
		t.Fatal("the limit fell to", got, "instead of its minimum")
	}
}

// A cut that noise caused, or a backend that recovered, must not keep the
// limit low: each window without queueing adds half a batch.
func TestLimitGrowsBackWhileExecutionsStayNearTheirBaseline(t *testing.T) {
	feed := newLimitFeed(1, 16)
	idle := executions{count: windowExecutions, size: 8, elapsed: 5 * unit}
	feed.run(idle)
	over := executions{count: 6 * windowExecutions, size: 8, elapsed: queued}
	feed.run(over)
	if got := feed.limit.current(); got != 1 {
		t.Fatal("queueing left the limit at", got)
	}
	steady := executions{count: 10 * windowExecutions, size: 8, elapsed: 5 * unit}
	feed.run(steady)
	if got := feed.limit.current(); got != 6 {
		t.Fatal("ten windows without queueing grew the limit to", got)
	}
	steady.count = 100 * windowExecutions
	feed.run(steady)
	if got := feed.limit.current(); got != 16 {
		t.Fatal("the limit grew to", got, "instead of max_in_flight")
	}
}

// A window needs several executions, so a few slower records among faster
// ones, such as reads of a large document, do not lower the limit.
func TestAFewSlowerExecutionsDoNotDecideAWindow(t *testing.T) {
	feed := newLimitFeed(0, 16)
	fast := executions{count: 4, size: 1, elapsed: unit, key: reads}
	slow := executions{count: 1, size: 1, elapsed: times(unit, 20), key: reads}
	for range 200 {
		feed.run(fast)
		feed.run(slow)
	}
	if got := feed.limit.current(); got != 16 {
		t.Fatal("one slower read in five lowered the limit to", got)
	}
}

// Halving the limit makes batches larger, and larger batches take longer even
// on an idle backend. That must not read as queueing, or the limit would keep
// halving.
func TestLargerBatchesAreNotMistakenForQueueing(t *testing.T) {
	feed := newLimitFeed(1, 16)
	idle := executions{count: windowExecutions, size: 8, elapsed: 5 * unit}
	feed.run(idle)
	over := executions{count: windowExecutions, size: 8, elapsed: queued}
	feed.run(over)
	if got := feed.limit.current(); got != 8 {
		t.Fatal("queueing left the limit at", got)
	}
	larger := executions{count: 20 * windowExecutions, size: 64, elapsed: 12 * unit, held: true}
	feed.run(larger)
	if got := feed.limit.current(); got != 16 {
		t.Fatal("larger batches at their own baseline held the limit at", got)
	}
	// A larger batch is never faster, so it bounds the baseline of smaller ones.
	fast := newLimitFeed(1, 16)
	quick := executions{count: 2, size: 64, elapsed: 2 * unit}
	fast.run(quick)
	small := executions{count: 10, size: 8, elapsed: times(2*unit, queueingRatio+1)}
	fast.run(small)
	if got := fast.limit.current(); got == 16 {
		t.Fatal("small batches far slower than a larger one did not lower the limit")
	}
}

func TestEachBatchGroupKeepsItsOwnBaseline(t *testing.T) {
	programs := groupKey{batch: "db.collection/lua"}
	for _, slower := range []groupKey{mutations, programs} {
		feed := newLimitFeed(1, 16)
		fast := executions{count: 1, size: 8, elapsed: unit, key: reads}
		if slower == programs {
			fast.key = mutations
		}
		slow := executions{count: 1, size: 8, elapsed: 5 * unit, key: slower}
		for range 50 {
			feed.run(fast)
			feed.run(slow)
		}
		if got := feed.limit.current(); got != 16 {
			t.Fatal(slower, "batches slower than", fast.key, "lowered the limit to", got)
		}
	}
}

// Records carry very different amounts of data, so a batch of large documents
// is compared with batches of as much data.
func TestLargerDocumentsAreNotMistakenForQueueing(t *testing.T) {
	feed := newLimitFeed(1, 16)
	small := executions{count: 1, size: 8, bytes: 8 << 10, elapsed: unit, key: mutations}
	large := executions{count: 1, size: 8, bytes: 8 << 20, elapsed: 5 * unit, key: mutations}
	for range 50 {
		feed.run(small)
		feed.run(large)
	}
	if got := feed.limit.current(); got != 16 {
		t.Fatal("batches of larger documents lowered the limit to", got)
	}
}

// One caller sending one request at a time does not contend for the backend,
// so its slower requests are different work rather than queueing.
func TestExecutionsThatRunAloneDoNotLowerTheLimit(t *testing.T) {
	feed := newLimitFeed(0, 16)
	fast := executions{count: 1, size: 1, elapsed: unit, key: mutations, alone: true}
	slow := executions{count: 3, size: 1, elapsed: times(unit, queueingRatio+2), key: mutations, alone: true}
	for range 50 {
		feed.run(fast)
		feed.run(slow)
	}
	if got := feed.limit.current(); got != 16 {
		t.Fatal("executions that ran alone lowered the limit to", got)
	}
	slow.alone, slow.count = false, windowExecutions
	feed.run(slow)
	if got := feed.limit.current(); got != 8 {
		t.Fatal("the same executions running together left the limit at", got)
	}
}

// A backend that became slower for good must not keep the limit at its
// minimum: the old baseline expires.
func TestBaselineOfAFasterBackendExpires(t *testing.T) {
	feed := newLimitFeed(1, 16)
	faster := executions{count: windowExecutions, size: 8, elapsed: 5 * unit}
	feed.run(faster)
	slower := executions{count: 1, size: 8, elapsed: queued, held: true}
	start := feed.now
	for feed.now.Sub(start) < baselineAge {
		feed.run(slower)
	}
	if got := feed.limit.current(); got != 1 {
		t.Fatal("a slower backend left the limit at", got, "while its old baseline lasted")
	}
	// Growing from one to 16 takes 30 windows.
	regrow := time.Duration(30*windowExecutions) * slower.elapsed
	for feed.now.Sub(start) < 2*baselineAge+regrow {
		feed.run(slower)
	}
	if got := feed.limit.current(); got != 16 {
		t.Fatal("after the old baseline expired the limit only reached", got)
	}
}

// A probe that ran into a backend that did not empty would otherwise become
// the next generation's baseline and hide the queue from the limit.
func TestABaselineRisesAtMostTwofoldPerGeneration(t *testing.T) {
	feed := newLimitFeed(1, 16)
	fast := executions{count: windowExecutions, size: 8, elapsed: 5 * unit, key: mutations}
	feed.run(fast)
	feed.now = feed.now.Add(baselineAge)
	inflated := finishedBatch{started: feed.now, finished: feed.now.Add(40 * unit), key: mutations, size: 8, probe: true}
	feed.limit.observe(inflated)
	feed.now = inflated.finished
	congested := executions{count: 2 * windowExecutions, size: 8, elapsed: 50 * unit, key: mutations, held: true}
	feed.run(congested)
	feed.now = feed.now.Add(baselineAge)
	before := feed.limit.current()
	feed.run(congested)
	if got := feed.limit.baseline(mutations, sizeClass(8, 0)); got != 10*unit {
		t.Fatal("the baseline rose from", 5*unit, "to", got, "in one generation")
	}
	if got := feed.limit.current(); got >= before {
		t.Fatal("a queue ten times the old baseline let the limit go from", before, "to", got)
	}
}

func TestOneStalledExecutionDoesNotDecideAWindow(t *testing.T) {
	feed := newLimitFeed(1, 16)
	quick := executions{count: 19, size: 8, elapsed: unit}
	feed.run(quick)
	stalled := executions{count: 1, size: 8, elapsed: 100 * unit}
	feed.run(stalled)
	if got := feed.limit.current(); got != 16 {
		t.Fatal("one stalled execution lowered the limit to", got)
	}
}

// One execution per replica can be more than a shared backend takes. The limit
// then runs one batch at a time and leaves the backend idle in between, and
// the callers that would have sent a second batch join the next one.
func TestLimitFallsBelowOneBatch(t *testing.T) {
	feed := newLimitFeed(0, 16)
	idle := executions{count: windowExecutions, size: 8, elapsed: 5 * unit}
	feed.run(idle)
	queued := executions{count: 8 * windowExecutions, size: 8, elapsed: 40 * unit}
	feed.run(queued)
	if got := feed.limit.current(); got != lowestLimit {
		t.Fatal("queueing at one batch left the limit at", got)
	}
	if admitted, _ := feed.limit.admits(1, feed.now); admitted {
		t.Fatal("a limit below one admitted a second running batch")
	}
	// A quarter runs one batch and stays idle three times as long.
	admitted, retry := feed.limit.admits(0, feed.now)
	idleFor := min(3*feed.limit.typical, longestIdle)
	if admitted || !retry.Equal(feed.limit.lastFinished.Add(idleFor)) {
		t.Fatal("a limit of a quarter admitted", admitted, "with retry after", retry.Sub(feed.now))
	}
	if admitted, _ := feed.limit.admits(0, retry); !admitted {
		t.Fatal("the limit did not admit a batch after its idle time")
	}
	// Below one it grows by half its value per window without queueing.
	held := executions{count: 1, size: 8, elapsed: 5 * unit, held: true}
	for range 100 {
		feed.run(held)
		if feed.limit.current() != lowestLimit {
			break
		}
	}
	if got := feed.limit.current(); got != 1.5*lowestLimit {
		t.Fatal("a window without queueing grew the limit to", got)
	}
}

// A replica that starts while the backend queues takes slow executions for its
// baseline; the probe's execution on an emptied backend corrects it.
func TestAProbeRenewsAnInflatedBaseline(t *testing.T) {
	feed := newLimitFeed(1, 16)
	congested := executions{count: 2 * windowExecutions, size: 8, elapsed: 8 * unit, key: mutations, held: true}
	feed.run(congested)
	if got := feed.limit.current(); got != 16 {
		t.Fatal("executions at a congested baseline lowered the limit to", got)
	}
	typical := feed.limit.typical
	fastest := times(congested.elapsed, 1/(queueingRatio+1))
	probe := finishedBatch{started: feed.now, finished: feed.now.Add(fastest), key: mutations, size: 8, probe: true}
	feed.limit.observe(probe)
	feed.now = probe.finished
	if got := feed.limit.baseline(mutations, sizeClass(8, 0)); got != fastest {
		t.Fatal("the probe left the baseline at", got)
	}
	if feed.limit.typical != typical {
		t.Fatal("the probe moved the typical execution time to", feed.limit.typical)
	}
	congested.count = 4 * windowExecutions
	feed.run(congested)
	if got := feed.limit.current(); got != 1 {
		t.Fatal("congested executions after the probe left the limit at", got)
	}
}

// A replica that starts cannot tell how much its backend already queues, since
// its first executions set its baseline either way. It runs two batches at a
// time, halving below that while executions slow down, and grows only once it
// settled.
func TestALimitStartsAtTwoBatchesAndGrowsOnceItSettled(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	feed := &limitFeed{limit: newExecutionLimit(0, 16, now), now: now}
	if got := feed.limit.current(); got != startingLimit {
		t.Fatal("a limit started at", got)
	}
	steady := executions{count: 10 * windowExecutions, size: 8, elapsed: 5 * unit, held: true}
	feed.run(steady)
	if got := feed.limit.current(); got != startingLimit {
		t.Fatal("executions at their baseline grew a limit that did not settle to", got)
	}
	over := executions{count: windowExecutions, size: 8, elapsed: queued, held: true}
	for range 2 {
		feed.run(over)
	}
	if got := feed.limit.current(); got != 0.5 {
		t.Fatal("queueing left a limit that did not settle at", got)
	}
	feed.now = feed.limit.firstSeen.Add(probeInterval + probeInterval/4)
	steady.count = windowExecutions
	for _, want := range []float64{0.75, 1, 1, 2} {
		feed.run(steady)
		if got := feed.limit.current(); got != want {
			t.Fatal("a settled limit grew to", got, "instead of", want)
		}
	}
}

// A limit settles a quarter interval past one whole probe interval after its
// first execution, so at any interval a pause lies behind it, and even a burst
// that reached no pause does not stay held back for good.
func TestALimitSettlesAfterItsProbeInterval(t *testing.T) {
	for _, every := range []time.Duration{probeInterval, 8 * probeInterval} {
		now := time.Unix(1_000_000, 0)
		feed := &limitFeed{limit: newExecutionLimit(0, 16, now), now: now}
		feed.limit.every = every
		steady := executions{count: windowExecutions, elapsed: 5 * time.Millisecond, key: mutations, size: 8}
		feed.run(steady)
		first := feed.limit.firstSeen
		settles := every + every/4
		feed.now = first.Add(settles - time.Second)
		feed.run(steady)
		if feed.limit.settled {
			t.Fatal("a limit settled before", settles, "at an interval of", every)
		}
		feed.now = first.Add(settles)
		feed.run(steady)
		if !feed.limit.settled {
			t.Fatal("a limit did not settle after", settles, "at an interval of", every)
		}
	}
}

// probeStore is one replica's limits and their probe schedule.
type probeStore struct {
	limits [2]executionLimit
	probes probes
}

// newProbeStore starts a replica's limits at a time, with an interval that
// the pause's share of it may still lengthen.
func newProbeStore(now time.Time, interval time.Duration) *probeStore {
	s := &probeStore{}
	for op := range s.limits {
		s.limits[op] = newExecutionLimit(1, 16, now)
	}
	s.probes = newProbes(&s.limits)
	s.probes.interval = interval
	return s
}

// ran records that executions of an operation, typically this long, ran
// until a time.
func (s *probeStore) ran(op operation, typical time.Duration, until time.Time) {
	s.limits[op].typical = typical
	s.limits[op].lastSeen = until
}

// probed walks an interval as the runtime does, waking when a pause ends, and
// returns when each operation sent its probe batches.
func (s *probeStore) probed(t *testing.T, start time.Time) [2][]time.Time {
	t.Helper()
	var sent [2][]time.Time
	for op := range s.limits {
		for at := start; at.Before(start.Add(s.limits[op].every)); {
			gate, until := s.probes.gate(operation(op), at)
			switch gate {
			case gateClosed:
				if !until.After(at) {
					t.Fatal("operation", op, "paused until", until.Sub(start), "at", at.Sub(start))
				}
				at = until
			case gateProbe:
				s.probes.send(operation(op))
				sent[op] = append(sent[op], at)
			default:
				at = at.Add(time.Millisecond)
			}
		}
	}
	return sent
}

func TestProbesPauseEveryReplicaTogether(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	interval := 4 * time.Second
	replicas := make([]*probeStore, 3)
	for i := range replicas {
		replicas[i] = newProbeStore(start, interval)
		replicas[i].ran(operationMutate, time.Duration(i+1)*10*time.Millisecond, start)
		replicas[i].limits[operationMutate].lastHeld = start.Add(-interval / 2)
	}
	// Every replica that held a batch back pauses at the start of the interval.
	for i, replica := range replicas {
		if gate, _ := replica.probes.gate(operationMutate, start); gate != gateClosed {
			t.Fatal("replica", i, "did not pause at the start of an interval")
		}
	}
	// Each sends one probe batch of each operation, after a drain of twice its
	// typical execution time and within one typical execution time after that.
	for i, replica := range replicas {
		typical := replica.limits[operationMutate].typical
		for op, sent := range replica.probed(t, start) {
			if len(sent) != 1 {
				t.Fatal("replica", i, "sent", len(sent), "probe batches of operation", op)
			}
			if sent[0].Before(start.Add(2*typical)) || !sent[0].Before(start.Add(3*typical)) {
				t.Fatal("replica", i, "probed at", sent[0].Sub(start))
			}
		}
	}
	// A replica that held nothing back in the previous interval does not pause.
	next := start.Add(interval)
	idle := newProbeStore(start, interval)
	idle.ran(operationMutate, 10*time.Millisecond, start)
	idle.limits[operationMutate].lastHeld = start.Add(-interval / 2)
	if gate, _ := idle.probes.gate(operationMutate, next); gate != gateOpen {
		t.Fatal("a replica that held nothing back for an interval paused")
	}
	// One whose executions outlast the longest pause still pauses, within it.
	slow := newProbeStore(start, interval)
	slow.ran(operationMutate, 2*probeDrainMax, start)
	slow.limits[operationMutate].lastHeld = start.Add(-interval / 2)
	gate, until := slow.probes.gate(operationMutate, start)
	if gate != gateClosed || until.After(start.Add(probeDrainMax+probeWindowMax)) {
		t.Fatal("a replica with executions longer than the longest pause paused until", until.Sub(start))
	}
}

// Fast reads must not refill the backend while slow mutations drain, or the
// mutations' probe would run into the queue it was meant to avoid.
func TestAPauseEmptiesTheBackendOfEveryOperation(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	s := newProbeStore(start, 16*time.Second)
	s.ran(operationMutate, 80*time.Millisecond, start)
	s.limits[operationMutate].lastHeld = start.Add(-time.Second / 2)
	s.ran(operationRead, 5*time.Millisecond, start)
	drain := 2 * s.limits[operationMutate].typical
	for op := range s.limits {
		if gate, _ := s.probes.gate(operation(op), start.Add(drain-time.Millisecond)); gate != gateClosed {
			t.Fatal("operation", op, "resumed before the slowest operation drained")
		}
	}
	for op, sent := range s.probed(t, start) {
		if len(sent) != 1 || sent[0].Before(start.Add(drain)) {
			t.Fatal("operation", op, "probed at", sent)
		}
	}
}

// An operation that stopped keeps its last typical execution time, which must
// not lengthen the pauses of the one still running.
func TestAnOperationThatStoppedDoesNotLengthenPauses(t *testing.T) {
	start := time.Unix(1_000_000, 0).Truncate(8 * probeInterval)
	s := newProbeStore(start.Add(-20*probeInterval), probeInterval)
	s.ran(operationRead, 800*time.Millisecond, start.Add(-10*probeInterval))
	s.ran(operationMutate, 5*time.Millisecond, start)
	s.limits[operationMutate].lastHeld = start.Add(-probeInterval / 2)
	gate, until := s.probes.gate(operationMutate, start)
	if gate != gateClosed || until.After(start.Add(probeDrainMin+probeWindowMin)) || s.limits[operationMutate].every != probeInterval {
		t.Fatal("reads that stopped long ago paused mutations until", until.Sub(start), "every", s.limits[operationMutate].every)
	}
}

// A replica that started while its backend queued pauses at the first instant
// it is busy for, without waiting a whole interval.
func TestAnUnsettledReplicaPausesAtOnce(t *testing.T) {
	start := time.Unix(1_000_000, 0).Truncate(probeInterval)
	s := newProbeStore(start.Add(-time.Second), probeInterval)
	s.limits[operationMutate].busy(start.Add(-time.Second), true)
	s.ran(operationMutate, 10*time.Millisecond, start.Add(-time.Millisecond))
	if gate, _ := s.probes.gate(operationMutate, start); gate != gateClosed {
		t.Fatal("an unsettled replica did not pause at its first instant")
	}
}

func TestAStoreThatNeverPausedPausesWithinTheShortestInterval(t *testing.T) {
	longest := 8 * probeInterval
	start := time.Unix(1_000_000, 0).Truncate(longest).Add(probeInterval)
	s := newProbeStore(start.Add(-time.Second), probeInterval)
	l := &s.limits[operationMutate]
	l.busy(start.Add(-time.Second), true)
	s.ran(operationMutate, 600*time.Millisecond, start.Add(-time.Millisecond))
	if gate, _ := s.probes.gate(operationMutate, start); gate != gateClosed || s.probes.paused {
		t.Fatal("a Store with slow executions that never paused did not pause at the next instant of the shortest interval")
	}
	s.ran(operationMutate, 600*time.Millisecond, start.Add(probeDrainMax+probeWindowMax))
	next := start.Add(probeInterval)
	if gate, _ := s.probes.gate(operationMutate, next); gate != gateOpen || l.every != longest || !s.probes.paused {
		t.Fatal("after its first pause a slow Store paused at", next.Sub(start), "probing every", l.every)
	}
	second := start.Truncate(longest).Add(longest)
	s.ran(operationMutate, 600*time.Millisecond, second.Add(-time.Millisecond))
	if gate, _ := s.probes.gate(operationMutate, second); gate != gateClosed {
		t.Fatal("a Store that never settled did not pause at an instant of its interval")
	}
}

// A baseline lasts until the next probe can renew it.
func TestBaselinesOutlastTheProbeInterval(t *testing.T) {
	start := time.Unix(1_000_000, 0).Truncate(8 * probeInterval)
	s := newProbeStore(start, probeInterval)
	l := &s.limits[operationMutate]
	s.ran(operationMutate, 300*time.Millisecond, start)
	s.probes.gate(operationMutate, start)
	fast := finishedBatch{started: start, finished: start.Add(10 * time.Millisecond), key: mutations, size: 8}
	l.observe(fast)
	for _, at := range []time.Duration{20 * time.Second, 36 * time.Second} {
		slow := finishedBatch{started: start.Add(at), finished: start.Add(at + 300*time.Millisecond), key: mutations, size: 8}
		l.observe(slow)
	}
	if got := l.baseline(mutations, sizeClass(8, 0)); got != 10*time.Millisecond {
		t.Fatal("a probe interval of", l.every, "kept a baseline of", got)
	}
}

// A replica whose limit holds nothing back still loads the backend while it
// keeps batches running, so it pauses with the others.
func TestReplicasThatStayBusyPause(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	busy := newProbeStore(start.Add(-3*probeInterval), probeInterval)
	busy.limits[operationRead].busy(start.Add(-3*probeInterval), true)
	busy.limits[operationRead].settled = true
	busy.ran(operationRead, 10*time.Millisecond, start.Add(-time.Millisecond))
	if gate, _ := busy.probes.gate(operationMutate, start); gate != gateClosed {
		t.Fatal("a replica busy with reads through the previous interval did not pause its mutations")
	}
	// Emptying during its own pause does not make it idle.
	busy.ran(operationRead, 10*time.Millisecond, start.Add(5*time.Millisecond))
	busy.probes.rest(operationRead, start.Add(5*time.Millisecond))
	if gate, _ := busy.probes.gate(operationRead, start.Add(probeInterval)); gate != gateClosed {
		t.Fatal("a replica that emptied only during a pause stopped pausing")
	}
	rested := newProbeStore(start.Add(-3*probeInterval), probeInterval)
	rested.limits[operationRead].busy(start.Add(-3*probeInterval), true)
	rested.limits[operationRead].settled = true
	rested.probes.rest(operationRead, start.Add(-probeInterval/2))
	rested.limits[operationRead].busy(start.Add(-probeInterval/4), true)
	rested.ran(operationRead, 10*time.Millisecond, start.Add(-time.Millisecond))
	if gate, _ := rested.probes.gate(operationRead, start); gate != gateOpen {
		t.Fatal("a replica that was idle in the previous interval paused")
	}
}

// A pause of twice a long execution would take a large share of every
// interval, so slower Stores probe less often, at instants faster ones share.
func TestSlowExecutionsProbeLessOften(t *testing.T) {
	start := time.Unix(1_000_000, 0).Truncate(8 * probeInterval)
	slow := newProbeStore(start.Add(-20*probeInterval), probeInterval)
	slow.probes.paused = true
	l := &slow.limits[operationMutate]
	l.busy(start.Add(-20*probeInterval), true)
	slow.ran(operationMutate, 300*time.Millisecond, start.Add(4*probeInterval))
	for _, at := range []time.Time{start.Add(probeInterval), start.Add(2 * probeInterval), start.Add(3 * probeInterval)} {
		if gate, _ := slow.probes.gate(operationRead, at); gate != gateOpen {
			t.Fatal("a slow replica paused", at.Sub(start), "after an instant of its interval")
		}
	}
	if gate, _ := slow.probes.gate(operationRead, start.Add(4*probeInterval)); gate != gateClosed {
		t.Fatal("a slow replica did not pause at an instant of its interval")
	}
	if l.every != 4*probeInterval || slow.limits[operationRead].every != l.every {
		t.Fatal("a pause of", 2*l.typical, "probes every", l.every)
	}
}

func TestAStoreIsIdleOnlyWithNothingRunningOrQueued(t *testing.T) {
	release := make(chan struct{})
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 1)}
	adapter.records = blockUntil(release)
	runtime := open(t, adapter, limits(nil))
	idle := func(op operation) bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.limit[op].busySince.IsZero()
	}
	ticket := submit(t, runtime, mutationPlan("a"), nil)
	<-adapter.started
	if idle(operationMutate) {
		t.Fatal("a Store running a mutation batch is idle for mutations")
	}
	if !idle(operationRead) {
		t.Fatal("a Store running only mutations is busy for reads")
	}
	close(release)
	wait(t, ticket)
	if !idle(operationMutate) {
		t.Fatal("a Store with nothing running or queued is busy")
	}
}

// settle sets a runtime's adaptive limits as if its probes had found a backend
// that did not queue.
func settle(runtime *Runtime) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	for op := range runtime.limit {
		l := &runtime.limit[op]
		l.value, l.settled = l.max, true
	}
}

// A Store that just started runs two batches of each operation that are not
// full at a time; full batches still use every slot.
func TestAStoreStartsWithTwoBatchesOfEachOperation(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	defer close(release)
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchOperations = 2 }))
	for i := range 3 {
		plan := mutationPlan(fmt.Sprint("m", i))
		plan.BatchKey = fmt.Sprint("collection", i)
		submit(t, runtime, plan, nil)
	}
	for range 2 {
		started(t, adapter)
	}
	first := time.Now()
	notStarted(t, adapter)
	submit(t, runtime, readPlan("read"), nil)
	if batch := started(t, adapter); batch[0] != "read" {
		t.Fatal("a read waited for the first mutation batch", batch)
	}
	for _, key := range []string{"a", "b"} {
		submit(t, runtime, mutationPlan(key), nil)
	}
	if batch := started(t, adapter); len(batch) != 2 {
		t.Fatal("a full batch waited for the first mutation batch", batch)
	}
	// Before any execution finished, those that run on hold the next back
	// for a second.
	batch := started(t, adapter)
	if waited := time.Since(first); batch[0] != "m2" || waited < longestIdle-200*time.Millisecond {
		t.Fatal(batch, "started", waited, "after the first mutation batches")
	}
}

// Before a limit settles, and while it is at least one, an execution far slower
// than executions typically take holds the next batch back for at most a second,
// and each further batch above the limit for that much longer.
func TestAnUnsettledLimitDoesNotWaitForAnOverrunningBatch(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	l := newExecutionLimit(0, 16, now)
	l.value = 1
	l.busy(now, true)
	if admitted, retry := l.admits(1, now); admitted || !retry.Equal(now.Add(longestIdle)) {
		t.Fatal("before any execution finished, a second batch could start after", retry.Sub(now))
	}
	l.typical = 10 * time.Millisecond
	overrun := times(l.typical, queueingRatio)
	if admitted, retry := l.admits(1, now.Add(overrun-time.Millisecond)); admitted || !retry.Equal(now.Add(overrun)) {
		t.Fatal("a second batch could start", retry.Sub(now), "after the first instead of", overrun)
	}
	if admitted, _ := l.admits(1, now.Add(overrun)); !admitted {
		t.Fatal("an overrunning batch held the next back")
	}
	// Executions that took long do not make the next wait longer.
	l.typical = 10 * time.Second
	if _, retry := l.admits(1, now); !retry.Equal(now.Add(longestIdle)) {
		t.Fatal("after slow executions a second batch could start", retry.Sub(now), "after the first")
	}
	if _, retry := l.admits(3, now); !retry.Equal(now.Add(3 * longestIdle)) {
		t.Fatal("two batches above the limit let another start", retry.Sub(now), "after the last")
	}
	// Below one the backend queued, and batches wait their turn.
	l.value = 0.5
	if admitted, retry := l.admits(1, now.Add(time.Hour)); admitted || !retry.IsZero() {
		t.Fatal("a limit below one admitted a second batch")
	}
	l.value, l.settled = 1, true
	if admitted, retry := l.admits(1, now.Add(time.Hour)); admitted || !retry.IsZero() {
		t.Fatal("a settled limit of one admitted a second batch")
	}
}

// A Store under load pauses before its limit settles, and only then grows past
// where it started.
func TestAStoreUnderLoadPausesBeforeItSettlesAndGrows(t *testing.T) {
	adapter := &fakeAdapter{records: func(_ context.Context, plans []*execution.Plan) []*pb.Event {
		time.Sleep(time.Millisecond)
		return applied(plans)
	}}
	runtime := open(t, adapter, DefaultLimits())
	runtime.mu.Lock()
	runtime.probes.interval = time.Second
	runtime.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	var writers sync.WaitGroup
	defer writers.Wait()
	defer cancel()
	var sequence atomic.Int64
	for range 64 {
		writers.Go(func() {
			for ctx.Err() == nil {
				// Callers that finish together do not all send again at once.
				time.Sleep(rand.N(2 * time.Millisecond))
				plan := mutationPlan(fmt.Sprint("r", sequence.Add(1)))
				ticket, failure := runtime.Submit(ctx, plan, nil)
				if failure != nil {
					return
				}
				_, _ = ticket.Wait(ctx)
			}
		})
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		settled, value, paused := runtime.limit[operationMutate].settled, runtime.limit[operationMutate].value, runtime.probes.paused
		runtime.mu.Unlock()
		if settled && !paused {
			t.Fatal("a limit settled before its Store paused")
		}
		if !settled && value > startingLimit {
			t.Fatal("a limit grew to", value, "before it settled")
		}
		if settled && value > startingLimit {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("a Store under load did not settle and grow")
}

// lowerLimit sets a runtime's adaptive limits as if its backend had queued.
func lowerLimit(runtime *Runtime, value float64) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	for op := range runtime.limit {
		runtime.limit[op].value = value
	}
}

func TestFullBatchesUseEverySlot(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight, l.BatchOperations = 4, 2 }))
	lowerLimit(runtime, 1)
	submit(t, runtime, mutationPlan("a"), nil)
	if got := started(t, adapter); len(got) != 1 {
		t.Fatal("first batch", got)
	}
	// Each lone record waits for the limit until a second one fills its batch.
	for i := range 3 {
		submit(t, runtime, mutationPlan(fmt.Sprint("held", i)), nil)
		notStarted(t, adapter)
		submit(t, runtime, mutationPlan(fmt.Sprint("filled", i)), nil)
		if got := started(t, adapter); len(got) != 2 {
			t.Fatal("a full batch did not start beside the limit", got)
		}
	}
	submit(t, runtime, mutationPlan("over"), nil)
	submit(t, runtime, mutationPlan("max"), nil)
	notStarted(t, adapter)
	if stats := runtime.Stats(); stats.Active != 4 || stats.AdaptiveLimit[operationMutate] != 1 || stats.Queued != 2 {
		t.Fatal("full batches did not stop at max_in_flight", stats)
	}
	close(release)
	if got := started(t, adapter); len(got) != 2 {
		t.Fatal("the batch over max_in_flight", got)
	}
}

func TestDrainUsesEverySlot(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 4 }))
	lowerLimit(runtime, 1)
	for i := range 4 {
		plan := mutationPlan(fmt.Sprint("r", i))
		plan.BatchKey = fmt.Sprint("collection", i)
		submit(t, runtime, plan, nil)
	}
	started(t, adapter)
	notStarted(t, adapter)
	runtime.BeginDrain()
	for range 3 {
		started(t, adapter)
	}
	close(release)
}

func TestALimitBelowOneSpacesBatchesOut(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime := open(t, adapter, limits(nil))
	runtime.mu.Lock()
	limit := &runtime.limit[operationMutate]
	limit.value = 0.5
	limit.typical = 200 * time.Millisecond
	limit.lastFinished = time.Now()
	runtime.mu.Unlock()
	submitted := time.Now()
	ticket := submit(t, runtime, mutationPlan("a"), nil)
	started(t, adapter)
	// At a half the backend stays idle as long as an execution typically takes.
	if waited := time.Since(submitted); waited < 150*time.Millisecond {
		t.Fatal("a batch started", waited, "after the last execution finished")
	}
	wait(t, ticket)
}

// One stalled execution raises the typical execution time, but a limit below
// one keeps the backend idle for at most longestIdle after it.
func TestIdleTimeBelowOneIsBounded(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	l := newExecutionLimit(0, 16, now)
	l.value = lowestLimit
	l.typical = 20 * time.Second
	l.lastFinished = now
	if _, retry := l.admits(0, now); retry.After(now.Add(longestIdle)) {
		t.Fatal("a stalled execution left the backend idle for", retry.Sub(now))
	}
}

// Full batches do not wait for the limit, and batches that are not full wait
// only for each other, so ingest does not starve other work.
func TestFullBatchesDoNotHoldOtherBatchesBack(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	defer close(release)
	// Mutations wait for companions until their batch is full.
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight, l.BatchOperations, l.BatchWait = 8, 2, time.Hour }))
	runtime.mu.Lock()
	limit := &runtime.limit[operationMutate]
	limit.value = lowestLimit
	limit.typical = 10 * time.Second
	runtime.mu.Unlock()
	for i := range 4 {
		submit(t, runtime, mutationPlan(fmt.Sprint("r", i)), nil)
	}
	for range 2 {
		if batch := started(t, adapter); len(batch) != 2 {
			t.Fatal("a full batch started with", len(batch), "records")
		}
	}
	// A deadline inside the batch window sends a lone mutation at once.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	other := mutationPlan("other")
	other.BatchKey = "other"
	submitWithin(t, ctx, runtime, other)
	if batch := started(t, adapter); batch[0] != "other" {
		t.Fatal("a mutation did not start beside running full batches")
	}
}

// A write waiting for a Search refresh does not use the backend meanwhile,
// so it does not take the limit's slot from other work.
func TestBatchesThatWaitDoNotHoldOtherBatchesBack(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	defer close(release)
	runtime := open(t, adapter, limits(nil))
	lowerLimit(runtime, 1)
	visible := mutationPlan("visible")
	visible.BatchKey, visible.Waits = "records|visible", true
	submit(t, runtime, visible, nil)
	started(t, adapter)
	submit(t, runtime, mutationPlan("other"), nil)
	if batch := started(t, adapter); batch[0] != "other" {
		t.Fatal("a mutation waited for a write waiting on a refresh")
	}
}

// Mutations that queue behind a slow journal lower their own limit, not the
// limit of reads the backend still serves at once.
func TestSlowMutationsDoNotHoldReadsBack(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	defer close(release)
	runtime := open(t, adapter, limits(nil))
	settle(runtime)
	runtime.mu.Lock()
	runtime.limit[operationMutate].value = 1
	runtime.mu.Unlock()
	submit(t, runtime, mutationPlan("slow"), nil)
	started(t, adapter)
	held := mutationPlan("held")
	held.BatchKey = "other"
	submit(t, runtime, held, nil)
	notStarted(t, adapter)
	for i := range 2 {
		read := readPlan(fmt.Sprint("read", i))
		read.BatchKey = fmt.Sprint("collection", i)
		submit(t, runtime, read, nil)
		if batch := started(t, adapter); batch[0] != read.Key {
			t.Fatal("a read waited for the limit of mutations", batch)
		}
	}
	if stats := runtime.Stats(); stats.AdaptiveLimit[operationRead] != 16 || stats.Queued != 1 {
		t.Fatal("reads shared the limit of mutations", stats)
	}
}

// The limits of mutations and reads learn only from their own executions.
func TestEachOperationLearnsItsOwnLimit(t *testing.T) {
	adapter := &fakeAdapter{}
	adapter.records = func(_ context.Context, plans []*execution.Plan) []*pb.Event {
		return applied(plans)
	}
	runtime := open(t, adapter, limits(nil))
	wait(t, submit(t, runtime, readPlan("read"), nil))
	runtime.mu.Lock()
	mutations := runtime.limit[operationMutate].typical
	reads := runtime.limit[operationRead].typical
	runtime.mu.Unlock()
	if mutations != 0 || reads == 0 {
		t.Fatal("a read taught the limit of mutations", mutations, reads)
	}
}

func TestEqualInFlightBoundsFixTheLimit(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	runtime := open(t, adapter, limits(func(l *Limits) { l.MinInFlight, l.MaxInFlight = 3, 3 }))
	if runtime.adaptive {
		t.Fatal("equal bounds left the limit adaptive")
	}
	lowerLimit(runtime, 1)
	for i := range 3 {
		plan := mutationPlan(fmt.Sprint("r", i))
		plan.BatchKey = fmt.Sprint("collection", i)
		submit(t, runtime, plan, nil)
		started(t, adapter)
	}
	fixed := [2]float64{3, 3}
	var none [2]float64
	if stats := runtime.Stats(); stats.AdaptiveLimit != fixed || stats.LatencyRatio != none {
		t.Fatal("fixed limit reported", stats)
	}
	close(release)
}

func TestAbandonedExecutionsDoNotTeachTheLimit(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	adapter.records = blockUntil(nil)
	runtime := open(t, adapter, limits(nil))
	ctx, cancel := context.WithCancel(t.Context())
	ticket := submitWithin(t, ctx, runtime, mutationPlan("a"))
	started(t, adapter)
	cancel()
	if _, err := ticket.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	typical := runtime.limit[operationMutate].typical
	runtime.mu.Unlock()
	if typical != 0 {
		t.Fatal("an execution its callers abandoned was observed", typical)
	}
}

func TestRefusedExecutionsDoNotTeachTheLimit(t *testing.T) {
	var refuse atomic.Bool
	refuse.Store(true)
	adapter := &fakeAdapter{}
	adapter.records = func(_ context.Context, plans []*execution.Plan) []*pb.Event {
		if !refuse.Load() {
			return applied(plans)
		}
		events := make([]*pb.Event, len(plans))
		for i, plan := range plans {
			failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "connection refused")
			events[i] = execution.FailedEvent(plan.Command, pb.MutationOutcome_NOT_STARTED, failure)
		}
		return events
	}
	runtime := open(t, adapter, limits(nil))
	typical := func() time.Duration {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.limit[operationMutate].typical
	}
	refused := submit(t, runtime, mutationPlan("a"), nil)
	wait(t, refused)
	if got := typical(); got != 0 {
		t.Fatal("an execution its backend refused was observed", got)
	}
	refuse.Store(false)
	answered := submit(t, runtime, mutationPlan("b"), nil)
	wait(t, answered)
	if typical() == 0 {
		t.Fatal("an execution its backend answered was not observed")
	}
}

func TestExecutionsThatWaitDoNotTeachTheLimit(t *testing.T) {
	adapter := &fakeAdapter{}
	adapter.records = func(_ context.Context, plans []*execution.Plan) []*pb.Event {
		return applied(plans)
	}
	runtime := open(t, adapter, limits(nil))
	typical := func() time.Duration {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.limit[operationMutate].typical
	}
	visible := mutationPlan("a")
	visible.Waits = true
	waited := submit(t, runtime, visible, nil)
	wait(t, waited)
	if got := typical(); got != 0 {
		t.Fatal("an execution that waited for a refresh was observed", got)
	}
	answered := submit(t, runtime, mutationPlan("b"), nil)
	wait(t, answered)
	if typical() == 0 {
		t.Fatal("an execution that did not wait was not observed")
	}
}

// sharedBackend runs executions on two workers. Each costs a fixed overhead
// plus a small cost per record, as a request costs a database CPU.
type sharedBackend struct {
	workers   chan struct{}
	overhead  time.Duration
	perRecord time.Duration
	records   atomic.Int64
}

func (b *sharedBackend) execute(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	select {
	case b.workers <- struct{}{}:
	case <-ctx.Done():
		events := make([]*pb.Event, len(plans))
		for i, plan := range plans {
			events[i] = execution.FailedEvent(plan.Command, pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(ctx))
		}
		return events
	}
	time.Sleep(b.overhead + time.Duration(len(plans))*b.perRecord)
	<-b.workers
	b.records.Add(int64(len(plans)))
	return applied(plans)
}

type sharingRun struct {
	replicas, writers int
	limits            Limits
}

// throughput runs writers that each wait for one record at a time, spread over
// replicas sharing one backend, and returns the records per second it applied
// once the limits settled and converged, which can take over ten seconds.
func throughput(t *testing.T, run sharingRun) float64 {
	t.Helper()
	backend := &sharedBackend{workers: make(chan struct{}, 2), overhead: 2 * time.Millisecond, perRecord: 20 * time.Microsecond}
	runtimes := make([]*Runtime, run.replicas)
	for i := range runtimes {
		adapter := &fakeAdapter{records: backend.execute}
		runtimes[i] = open(t, adapter, run.limits)
		runtimes[i].mu.Lock()
		runtimes[i].probes.interval = time.Second
		runtimes[i].mu.Unlock()
	}
	ctx, cancel := context.WithCancel(t.Context())
	var writers sync.WaitGroup
	var sequence atomic.Int64
	for i := range run.writers {
		runtime := runtimes[i%run.replicas]
		writers.Go(func() {
			for ctx.Err() == nil {
				// Callers that finish together do not all send again at once.
				time.Sleep(rand.N(2 * time.Millisecond))
				plan := mutationPlan(fmt.Sprint("r", sequence.Add(1)))
				ticket, failure := runtime.Submit(ctx, plan, nil)
				if failure != nil {
					return
				}
				_, _ = ticket.Wait(ctx)
			}
		})
	}
	time.Sleep(15 * time.Second)
	before := backend.records.Load()
	measure := 10 * time.Second
	time.Sleep(measure)
	applied := backend.records.Load() - before
	cancel()
	writers.Wait()
	return float64(applied) / measure.Seconds()
}

// Replicas added in front of a backend that is the bottleneck must not lower
// what it applies. With fixed slots each replica keeps sending small batches,
// and the backend spends its time on their overhead.
func TestReplicasSharingASlowBackendKeepTheirThroughput(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measures throughput for over a minute")
	}
	adaptive := DefaultLimits()
	fixed := DefaultLimits()
	fixed.MinInFlight = fixed.MaxInFlight
	few := sharingRun{replicas: 2, writers: 768, limits: adaptive}
	many := few
	many.replicas = 6
	fixedMany := many
	fixedMany.limits = fixed
	fewRate := throughput(t, few)
	manyRate := throughput(t, many)
	fixedRate := throughput(t, fixedMany)
	t.Logf("records/s: 2 replicas %.0f, 6 replicas %.0f, 6 replicas with fixed slots %.0f", fewRate, manyRate, fixedRate)
	if manyRate < 0.7*fewRate {
		t.Fatalf("six replicas applied %.0f records/s against %.0f for two", manyRate, fewRate)
	}
	if manyRate < 3*fixedRate {
		t.Fatalf("adaptive slots applied %.0f records/s against %.0f for fixed slots", manyRate, fixedRate)
	}
}
