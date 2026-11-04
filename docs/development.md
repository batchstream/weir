# Development

How to test Weir at every level and how releases are published.

## Tests

```sh
go test ./...
go test -race ./...
go vet ./...
go vet -tags=integration ./...
```

Default tests need no backends.

## Integration tests

Integration tests use the `integration` build tag, Docker and these local
fixtures:

- `scripts/mongod.sh` downloads MongoDB 8.0.32 for Ubuntu 24.04 amd64 or macOS
  arm64 into the ignored `.tools/`, checks its SHA-256 and prints its path.
- `scripts/mongo-local.sh start` runs a single-member replica set from that
  `mongod` on `127.0.0.1:27028`. `WEIR_MONGOD` selects another binary and
  `WEIR_MONGO_BIND` adds comma-separated literal, non-wildcard IP bind
  addresses. A running fixture is reused only when its bind matches.
- `scripts/search-local.sh start elasticsearch|opensearch normal|overload` runs
  Elasticsearch 8.19.22 on `127.0.0.1:19200` or OpenSearch 3.3.0 on
  `127.0.0.1:19202` in Docker. Use `normal` capacity for ordinary runs; the
  default `overload` profile shrinks the write thread pool for the Search
  capacity-rejection tests. `start` waits until the cluster is green and the
  write thread pool has stayed idle for 5 seconds, so startup system-index
  writes do not compete with tests. Each fixture container runs a 1 GiB heap
  and needs about 2 GiB of Docker memory. Stop the fixture before changing
  profiles.

The TLS and replica-set tests start their own `mongod` processes from the same
binary. The HTTPS Search tests start their own 1.5 GiB container from the image
`scripts/search-local.sh` pulled, so the whole suite needs about 3.5 GiB of
Docker memory. `WEIR_SEARCH_PRODUCT` selects `elasticsearch` (the default) or
`opensearch`.
Run packages one at a time, because MongoDB fault injection uses a server-global
failpoint:

```sh
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch normal
go test -p=1 -tags=integration -timeout=15m -skip '^(TestSearchRealBackendCongestion|TestSearchExpressionRealCapacity)$' ./...
scripts/search-local.sh stop elasticsearch
scripts/search-local.sh start elasticsearch overload
go test -tags=integration -run '^(TestSearchRealBackendCongestion|TestSearchExpressionRealCapacity)$' ./internal/backend/search
```

## Load and fault tests

These tests cross-compile the Linux CLI and run it in a CPU- and memory-limited
`busybox:1.37.0` container, reaching the MongoDB and Search fixtures above
through `host.docker.internal`. On Linux Docker Engine, also bind mongod to the
bridge gateway, as CI does:
`WEIR_MONGO_BIND=$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')`.
Each has its own opt-in:

| Opt-in | Test |
| --- | --- |
| `WEIR_LOAD_INTEGRATION=1` | `TestProcessLoad`: fixed 200 RPC/s SDK traffic for a 30s warmup plus 600s (`WEIR_LOAD_MEASUREMENT` shortens it), with slow consumers and a burst; p99, resource-bound and recovery gates and an independent oracle |
| `WEIR_FAULT_INTEGRATION=1` | `TestProcessFaults`: lost acknowledgements, cancellation, slow IO, unavailability and SIGTERM through owned proxies, with no replay |

```sh
docker pull busybox:1.37.0
WEIR_LOAD_INTEGRATION=1 go test -tags=integration -count=1 -timeout=20m -run '^TestProcessLoad$' -v ./internal/app
```

Run the load and fault tests separately so injected faults do not distort load
measurements.

## Other repositories

The [weir-tests](https://github.com/batchstream/weir-tests) repository contains
matched direct/Weir workloads and blackbox integration. Keep generated logs and
profiles under ignored `.testdata/`.

The public schema belongs to
[weir-protocol](https://github.com/batchstream/weir-protocol).

## Releases

Every push to `main` publishes the image `ghcr.io/batchstream/weir:<commit SHA>`
for Linux amd64 and arm64. Publishing a GitHub release `vMAJOR.MINOR.PATCH` also
publishes `ghcr.io/batchstream/weir:vMAJOR.MINOR.PATCH` and attaches Linux and
macOS amd64/arm64 archives with their `SHA256SUMS`. `weir version` reports the
source commit as `revision`. Build the image locally with:

```sh
docker buildx build --build-arg REVISION=$(git rev-parse HEAD) --load -t weir .
```
