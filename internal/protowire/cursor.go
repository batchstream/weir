package protowire

import "google.golang.org/grpc/mem"

// frameCursor borrows the codec input only during synchronous validation. It
// neither copies payload bytes nor changes their transport-owned references.
type frameCursor struct {
	buffers   mem.BufferSlice
	offset    int
	remaining int
}

func (cursor *frameCursor) readByte() (byte, bool) {
	for len(cursor.buffers) != 0 {
		data := cursor.buffers[0].ReadOnlyData()
		if cursor.offset == len(data) {
			cursor.buffers = cursor.buffers[1:]
			cursor.offset = 0
			continue
		}
		value := data[cursor.offset]
		cursor.offset++
		cursor.remaining--
		return value, true
	}
	return 0, false
}

func (cursor *frameCursor) discard(length int) bool {
	if length > cursor.remaining {
		return false
	}
	cursor.remaining -= length
	for length > 0 {
		available := len(cursor.buffers[0].ReadOnlyData()) - cursor.offset
		if length < available {
			cursor.offset += length
			return true
		}
		length -= available
		cursor.buffers = cursor.buffers[1:]
		cursor.offset = 0
	}
	return true
}
