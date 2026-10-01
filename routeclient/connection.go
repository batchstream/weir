package routeclient

import (
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial creates a reusable connection to Weir's deployment-isolated plaintext
// listener. Static windows and bounded codec buffers match the peer transport
// budget. Close the returned connection after all finite batches finish.
func Dial(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535),
		grpc.WithReadBufferSize(16<<10), grpc.WithWriteBufferSize(16<<10), grpc.WithMaxHeaderListSize(16<<10),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallSendMsgSize(protocol.MaxFrame), grpc.MaxCallRecvMsgSize(protocol.MaxResponse)),
	)
}
