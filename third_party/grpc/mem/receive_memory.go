package mem

import "context"

// ReceiveMemory binds native server receive ownership to an application budget.
// NewStream and Buffer must never block the connection reader. ReserveMessage
// runs in the RPC reader and may wait for capacity before expanding its window.
type ReceiveMemory interface {
	NewConnection() (func(), error)
	NewStream(context.Context, int) (ReceiveStream, error)
}

type ReceiveStream interface {
	SetContext(context.Context)
	ReserveMessage(int, bool) error
	Buffer(int) (func(), error)
	BeforeDecode(BufferSlice, any) error
	Decoded(any)
	ReleaseMessage()
	Close()
	Retire()
}

// TrackReceiveBuffer preserves the original allocation and reports its final
// reference, including split views and buffers below the pooling threshold.
func TrackReceiveBuffer(data Buffer, release func()) Buffer {
	data.Ref()
	owner := &receiveBufferOwner{data: data, release: release}
	raw := data.ReadOnlyData()
	b := newBuffer()
	b.origData = &raw
	b.data = raw
	b.pool = owner
	b.rootBuf = b
	b.refs.Store(1)
	return b
}

type receiveBufferOwner struct {
	data    Buffer
	release func()
}

func (*receiveBufferOwner) Get(int) *[]byte { panic("receive owner is not an allocation pool") }
func (owner *receiveBufferOwner) Put(*[]byte) {
	owner.data.Free()
	owner.data = nil
	owner.release()
	owner.release = nil
}
