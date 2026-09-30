package luaengine

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/batchstream/weir/internal/luaworker"
)

func Serve(in io.Reader, out io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(in, luaworker.MaxRequestBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > luaworker.MaxRequestBytes {
		answer := luaworker.Response{Error: "worker request exceeds limit"}
		return writeResponse(out, answer)
	}
	var incoming luaworker.Request
	if err := luaworker.DecodeEnvelope(raw, &incoming); err != nil {
		answer := luaworker.Response{Error: "invalid worker request"}
		return writeResponse(out, answer)
	}
	if incoming.TimeoutMillis < 1 || incoming.TimeoutMillis > luaworker.MaxTimeout.Milliseconds() {
		answer := luaworker.Response{Error: "invalid worker timeout"}
		return writeResponse(out, answer)
	}
	if err := luaworker.ValidateProgram(incoming.Program); err != nil {
		answer := luaworker.Response{Error: "invalid worker program"}
		return writeResponse(out, answer)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(incoming.TimeoutMillis)*time.Millisecond)
	defer cancel()
	result, err := Evaluate(ctx, incoming.Program)
	if err != nil {
		answer := luaworker.Response{Error: "Lua evaluation failed"}
		return writeResponse(out, answer)
	}
	if err := luaworker.ValidateResult(result); err != nil {
		answer := luaworker.Response{Error: "invalid Lua result"}
		return writeResponse(out, answer)
	}
	answer := luaworker.Response{Result: result}
	return writeResponse(out, answer)
}

func writeResponse(out io.Writer, answer luaworker.Response) error {
	data, err := json.Marshal(answer)
	if err != nil || len(data)+1 > luaworker.MaxResponseBytes {
		answer = luaworker.Response{Error: "worker response exceeds limit"}
		data, err = json.Marshal(answer)
		if err != nil {
			return err
		}
	}
	data = append(data, '\n')
	_, err = out.Write(data)
	return err
}
