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
PATH="$PWD/.tools/bin:$PATH" .tools/protoc/bin/protoc --go_out=. --go_opt=module=github.com/batchstream/weir --go-grpc_out=. --go-grpc_opt=module=github.com/batchstream/weir api/weir/v1/weir.proto
# Keep the repository's named-struct-literal convention reproducible, including generated code.
awk '
 $0 == "\treturn &weirClient{cc}" { print "\tclient := &weirClient{cc}"; print "\treturn client"; count++; next }
 index($0,"\treturn srv.(WeirServer).Bulk(&grpc.GenericServerStream") == 1 {
  print "\tserverStream := &grpc.GenericServerStream[BulkRequestFrame, BulkResponseFrame]{ServerStream: stream}"
  print "\treturn srv.(WeirServer).Bulk(serverStream)"; count++; next
 }
 index($0,"\treturn srv.(WeirServer).Scan(m, &grpc.GenericServerStream") == 1 {
  print "\tserverStream := &grpc.GenericServerStream[ScanRequest, ScanResponseFrame]{ServerStream: stream}"
  print "\treturn srv.(WeirServer).Scan(m, serverStream)"; count++; next
 }
 { print }
 END { if (count != 3) exit 1 }
' api/weir/v1/weir_grpc.pb.go > .tools/weir_grpc.pb.go
mv .tools/weir_grpc.pb.go api/weir/v1/weir_grpc.pb.go
awk '
 $0 == "\ttype x struct{}" { print; print "\tpackageMarker := x{}"; next }
 { if (sub(/reflect.TypeOf\(x\{\}\)/,"reflect.TypeOf(packageMarker)")) count++; print }
 END { if (count != 1) exit 1 }
' api/weir/v1/weir.pb.go > .tools/weir.pb.go
mv .tools/weir.pb.go api/weir/v1/weir.pb.go
gofmt -w api/weir/v1/*.go
