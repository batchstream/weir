#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# Use the project-local compiler; never depend on a mutable system installation.
test "$(.tools/protoc/bin/protoc --version)" = 'libprotoc 33.4'
if [ ! -x .tools/bin/protoc-gen-go ] || [ "$(.tools/bin/protoc-gen-go --version)" != 'protoc-gen-go v1.36.11' ]; then
 GOBIN="$PWD/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
fi
if [ ! -x .tools/bin/protoc-gen-go-grpc ] || [ "$(.tools/bin/protoc-gen-go-grpc --version)" != 'protoc-gen-go-grpc 1.5.1' ]; then
 GOBIN="$PWD/.tools/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
fi
PATH="$PWD/.tools/bin:$PATH" .tools/protoc/bin/protoc \
 --go_out=. --go_opt=module=github.com/batchstream/weir \
 --go-grpc_out=. --go-grpc_opt=module=github.com/batchstream/weir \
 internal/api/peer/v1/peer.proto

# Keep the named-struct-literal convention reproducible in generated Go code.
awk '
 $0 == "\treturn &peerDiscoveryServiceClient{cc}" {
  print "\tclient := &peerDiscoveryServiceClient{cc}"; print "\treturn client"; count++; next
 }
 { print }
 END { if (count != 1) exit 1 }
' internal/api/peer/v1/peer_grpc.pb.go > .tools/peer_grpc.pb.go
mv .tools/peer_grpc.pb.go internal/api/peer/v1/peer_grpc.pb.go

go_file=internal/api/peer/v1/peer.pb.go
awk '
  $0 == "\ttype x struct{}" { print; print "\tpackageMarker := x{}"; next }
  { if (sub(/reflect.TypeOf\(x\{\}\)/,"reflect.TypeOf(packageMarker)")) count++; print }
  END { if (count != 1) exit 1 }
' "$go_file" > .tools/generated.pb.go
mv .tools/generated.pb.go "$go_file"
gofmt -w internal/api/peer/v1/peer.pb.go internal/api/peer/v1/peer_grpc.pb.go
