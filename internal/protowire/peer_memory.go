package protowire

import (
	"unsafe"

	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc/mem"
)

// PeerDecodeBytes counts actual repeated objects and string slots before native
// protobuf allocation. String/unknown bytes fit the wire length; Go slice
// growth can retain fewer than twice the logical pointer/string slot count.
func PeerDecodeBytes(data mem.BufferSlice) (int, error) {
	reader := frameCursor{buffers: data, remaining: data.Len()}
	announcement := peerpb.NodeAnnouncement{}
	bytes := data.Len() + 4096
	for reader.remaining > 0 {
		tag, _, err := frameVarint(&reader, uint64(reader.remaining))
		if err != nil || tag != 10 {
			return 0, invalidExecuteFraming()
		}
		length, _, err := frameVarint(&reader, uint64(reader.remaining))
		if err != nil || length > uint64(reader.remaining) {
			return 0, invalidExecuteFraming()
		}
		bytes += int(unsafe.Sizeof(announcement)) + 2*int(unsafe.Sizeof((*peerpb.NodeAnnouncement)(nil))) + 16
		end := reader.remaining - int(length)
		for reader.remaining > end {
			tag, _, err := frameVarint(&reader, uint64(reader.remaining-end))
			if err != nil {
				return 0, err
			}
			field := tag >> 3
			switch field {
			case 1, 3, 4, 5, 6:
				if tag&7 != 2 {
					return 0, invalidExecuteFraming()
				}
				length, _, err := frameVarint(&reader, uint64(reader.remaining-end))
				if err != nil || length > uint64(reader.remaining-end) {
					return 0, invalidExecuteFraming()
				}
				if field == 5 || field == 6 {
					bytes += 2 * int(unsafe.Sizeof(""))
				}
				if !reader.discard(int(length)) {
					return 0, invalidExecuteFraming()
				}
			case 2, 7, 8, 9:
				if tag&7 != 0 {
					return 0, invalidExecuteFraming()
				}
				if _, _, err := frameVarint(&reader, uint64(reader.remaining-end)); err != nil {
					return 0, err
				}
			default:
				// Directory validation already rejects unknown announcement fields. Do it
				// before allocating a potentially huge unknown-field representation.
				return 0, invalidExecuteFraming()
			}
		}
	}
	return bytes, nil
}
