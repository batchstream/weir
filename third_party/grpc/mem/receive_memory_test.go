package mem

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
)

type receiveMemoryTestPool struct {
	puts atomic.Int64
}

func (*receiveMemoryTestPool) Get(size int) *[]byte {
	data := make([]byte, size)
	return &data
}

func (pool *receiveMemoryTestPool) Put(data *[]byte) {
	pool.puts.Add(1)
	*data = nil
}

func TestReceiveBufferTracksFinalView(t *testing.T) {
	tests := []struct {
		name       string
		length     int
		capacity   int
		sliceInput bool
		subview    bool
		wantPuts   int64
	}{
		{name: "small-slice", length: 17, capacity: 64, sliceInput: true},
		{name: "small-unpooled", length: 17, capacity: 64},
		{name: "pooled", length: 2049, capacity: 8192, wantPuts: 1},
		{name: "short-pooled-allocation", length: 17, capacity: 8192, wantPuts: 1},
		{name: "short-view-of-pooled-allocation", length: 2049, capacity: 8192, subview: true, wantPuts: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := &receiveMemoryTestPool{}
			raw := make([]byte, test.length, test.capacity)
			for index := range raw {
				raw[index] = byte(index)
			}
			var original Buffer
			if test.sliceInput {
				original = SliceBuffer(raw)
			} else {
				original = NewBuffer(&raw, pool)
			}
			if test.subview {
				view := original.Slice(3, 20)
				original.Free()
				original = view
			}
			originalData := original.ReadOnlyData()
			var released atomic.Int64
			var earlyCredit atomic.Bool
			tracked := TrackReceiveBuffer(original, func() {
				if pool.puts.Load() != test.wantPuts {
					earlyCredit.Store(true)
				}
				released.Add(1)
			})
			if !bytes.Equal(originalData, tracked.ReadOnlyData()) || &originalData[0] != &tracked.ReadOnlyData()[0] {
				t.Fatal("tracking copied or changed the original allocation")
			}
			original.Free()
			tracked.Ref()
			whole := tracked.Slice(0, tracked.Len())
			partial := tracked.Slice(1, tracked.Len()-1)
			empty := tracked.Slice(2, 2)
			empty.Free()
			left, right := SplitUnsafe(tracked, tracked.Len()/2)
			left.Free()
			tracked.Free()
			whole.Free()
			partial.Free()
			if released.Load() != 0 || pool.puts.Load() != 0 {
				t.Fatal("receive credit or backing allocation released while the split tail is live", released.Load(), pool.puts.Load())
			}
			if right.Len() == 0 {
				t.Fatal("split tail did not retain input bytes")
			}
			right.Free()
			if released.Load() != 1 || pool.puts.Load() != test.wantPuts || earlyCredit.Load() {
				t.Fatal("final view did not release the original allocation before exactly one credit", released.Load(), pool.puts.Load(), earlyCredit.Load())
			}
		})
	}
}

func TestReceiveBufferConcurrentViewsRetainLastOwner(t *testing.T) {
	pool := &receiveMemoryTestPool{}
	raw := make([]byte, 4096)
	original := NewBuffer(&raw, pool)
	var released atomic.Int64
	var earlyCredit atomic.Bool
	tracked := TrackReceiveBuffer(original, func() {
		if pool.puts.Load() != 1 {
			earlyCredit.Store(true)
		}
		released.Add(1)
	})
	original.Free()
	held := tracked.Slice(0, 1)
	views := make([]Buffer, 64)
	for index := range views {
		views[index] = tracked.Slice(index+1, index+2)
	}
	tracked.Free()
	var workers sync.WaitGroup
	for _, view := range views {
		workers.Add(1)
		go func() {
			defer workers.Done()
			view.Free()
		}()
	}
	workers.Wait()
	if released.Load() != 0 || pool.puts.Load() != 0 || held.Len() != 1 {
		t.Fatal("concurrent view release retired a live final owner", released.Load(), pool.puts.Load())
	}
	held.Free()
	if released.Load() != 1 || pool.puts.Load() != 1 || earlyCredit.Load() {
		t.Fatal("concurrent views did not preserve one final release", released.Load(), pool.puts.Load(), earlyCredit.Load())
	}
}

func TestReceiveBufferReadRetainsUntilConsumed(t *testing.T) {
	pool := &receiveMemoryTestPool{}
	raw := make([]byte, 17, 8192)
	copy(raw, "short receive body")
	original := NewBuffer(&raw, pool)
	var released atomic.Int64
	tracked := TrackReceiveBuffer(original, func() { released.Add(1) })
	original.Free()
	first := make([]byte, 4)
	n, remaining := ReadUnsafe(first, tracked)
	if n != 4 || string(first) != "shor" || remaining == nil || remaining.Len() != 13 || released.Load() != 0 || pool.puts.Load() != 0 {
		t.Fatal("partial read changed data or released live input", n, remaining, released.Load(), pool.puts.Load())
	}
	tail := make([]byte, 13)
	n, remaining = ReadUnsafe(tail, remaining)
	if n != 13 || string(tail) != "t receive bod" || remaining != nil || released.Load() != 1 || pool.puts.Load() != 1 {
		t.Fatal("consuming final input did not retire its real backing allocation", n, string(tail), remaining, released.Load(), pool.puts.Load())
	}
}
