package protowire

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// ValidateExecuteFrame bounds nested record counts before protobuf allocates
// repeated messages. Singular routing and command fields cannot be repeated to
// bypass the frame's one-kind decode bound.
func ValidateExecuteFrame(data mem.BufferSlice) error {
	if data.Len() > protocol.MaxExecuteRequestBytes {
		return status.Error(codes.ResourceExhausted, "execution frame exceeds input byte budget")
	}
	source := data.Reader()
	defer source.Close()
	reader := bufio.NewReader(source)
	seen := uint64(0)
	for {
		tag, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			return nil
		}
		field := tag >> 3
		if err != nil || field < 1 || field > 3 || seen&(1<<field) != 0 {
			return invalidExecuteFraming()
		}
		seen |= 1 << field
		if field == 2 {
			if tag&7 != 0 {
				return invalidExecuteFraming()
			}
			if _, err := binary.ReadUvarint(reader); err != nil {
				return invalidExecuteFraming()
			}
			continue
		}
		length, err := messageLength(reader, tag, protocol.MaxExecuteRequestBytes)
		if err != nil {
			return err
		}
		if field == 3 {
			limited := &io.LimitedReader{R: reader, N: int64(length)}
			if err := validateCommandFrame(bufio.NewReader(limited), int(length)); err != nil {
				return err
			}
			if limited.N != 0 {
				return invalidExecuteFraming()
			}
		} else if _, err := reader.Discard(int(length)); err != nil {
			return invalidExecuteFraming()
		}
	}
}

func validateCommandFrame(reader *bufio.Reader, bytes int) error {
	tag, err := binary.ReadUvarint(reader)
	if err != nil || tag>>3 < 1 || tag>>3 > 4 {
		return invalidExecuteFraming()
	}
	length, err := messageLength(reader, tag, protocol.MaxCommandBytes)
	if err != nil {
		return err
	}
	if tag>>3 <= 2 {
		if bytes > protocol.MaxRecordFrameBytes {
			return status.Error(codes.ResourceExhausted, "record frame exceeds input byte budget")
		}
		limited := &io.LimitedReader{R: reader, N: int64(length)}
		if err := validateRecordFrame(bufio.NewReader(limited)); err != nil {
			return err
		}
		if limited.N != 0 {
			return invalidExecuteFraming()
		}
	} else if _, err := reader.Discard(int(length)); err != nil {
		return invalidExecuteFraming()
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		return invalidExecuteFraming()
	}
	return nil
}

func validateRecordFrame(reader *bufio.Reader) error {
	count := 0
	for {
		tag, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			return nil
		}
		if err != nil || tag>>3 != 1 {
			return invalidExecuteFraming()
		}
		length, err := messageLength(reader, tag, protocol.MaxRecordFrameBytes)
		if err != nil {
			return err
		}
		count++
		if count > protocol.MaxRecordFrameItems {
			return status.Error(codes.ResourceExhausted, "record metadata exceeds decode budget")
		}
		if _, err := reader.Discard(int(length)); err != nil {
			return invalidExecuteFraming()
		}
	}
}

func messageLength(reader *bufio.Reader, tag uint64, maximum int) (uint64, error) {
	if tag&7 != 2 {
		return 0, invalidExecuteFraming()
	}
	length, err := binary.ReadUvarint(reader)
	if err != nil || length > uint64(maximum) {
		return 0, invalidExecuteFraming()
	}
	return length, nil
}

func invalidExecuteFraming() error {
	return status.Error(codes.InvalidArgument, "invalid execution protobuf framing")
}
