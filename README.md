# Weir

[![CI](https://github.com/batchstream/weir/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/batchstream/weir/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Weir is a server that combines concurrent database requests into bounded batches.

## Overview

| Item | Description |
| --- | --- |
| Backends | MongoDB, Elasticsearch, OpenSearch |
| Operations | Typed reads and mutations, Lua transforms, native backend commands |
| Client | [Go SDK](https://github.com/batchstream/weir-go) |
| Deployment | Binary, container image or [Helm chart](https://github.com/batchstream/weir-charts) |

### How it works

1. A client connects to one Weir address and names a Store in each request.
2. Weir queues the request in that Store.
3. Weir sends queued requests to the database in one batch, such as a MongoDB
   `bulkWrite` or a Search `_bulk` call.
4. Each caller receives the result of its own record.

### Terms

| Term | Meaning |
| --- | --- |
| Store | A named backend connection in `weir.yaml`. |
| Search | Elasticsearch or OpenSearch. |
| Resource | The path of one record in a Store. MongoDB: `database/collection/s:key`. Search: `index/s:key`. |
| Mutation | A create, put, replace, delete or atomic transform of one record. |
| Native command | A backend's own command, sent without batching. Use it for queries, scans and counts, and to create collections and indices. |
| Replica | One Weir server process. All replicas behind an address host the same Stores and share nothing. Scale by adding replicas. |

### When to use it

Use Weir when many processes write single records at the same time.

Throughput through Weir compared with direct writes, for single 1 KiB upserts
to a 2-vCPU database on EKS:

| Concurrent writers | MongoDB | Elasticsearch |
| --: | --: | --: |
| 64 | 1.2× | 0.98× |
| 256 | 2.4× | 1.1× |
| 1,024 | 2.6× | 2.9× |
| 2,048 | 2.8× | 3.9× |

- At 2,048 writers, direct writes held about 2,100 database connections. One
  Weir Store held about 20.
- With few writers, throughput stays close to direct writes and median latency
  can rise by about 2 ms.

See [Performance](docs/performance.md) for the full results.

## Requirements

| Item | Requirement |
| --- | --- |
| Go | 1.27.3, to build from source |
| Operating system | Linux or macOS, amd64 or arm64 |
| MongoDB | A nonsharded replica set with the client `bulkWrite` command (MongoDB 8.0 or later). Tested with 8.0.32. Lua transforms also require transactions. |
| Search | Tested with Elasticsearch 8.19.22 and OpenSearch 3.3.0 |
| Collections and indices | Must exist before typed reads and mutations. Native commands can create them. |
| Network | The application listener uses plaintext gRPC. Run it on an isolated network. Backend authentication stays local. |

## Quick start

This runs MongoDB in Docker, starts Weir and sends a write and a read. It also
requires Docker and `curl`.

**Step 1.** Start MongoDB as a one-member replica set and create a collection.

```sh
docker run -d --name weir-mongo -p 127.0.0.1:27017:27017 mongo:8.0 --replSet rs0
sleep 5
docker exec weir-mongo mongosh --quiet --eval '
  rs.initiate();
  while (!db.hello().isWritablePrimary) sleep(100);
  db.getSiblingDB("weir_example").createCollection("records");
'
```

**Step 2.** Build Weir.

```sh
git clone https://github.com/batchstream/weir.git
cd weir
go build -o bin/weir ./cmd/weir
```

**Step 3.** Create the configuration file `weir.yaml`.

```sh
cat > weir.yaml <<'YAML'
listeners:
  application: 127.0.0.1:7447
  diagnostics: 127.0.0.1:7449
stores:
  - name: mongo
    mongodb:
      uri: mongodb://127.0.0.1:27017/?directConnection=true
YAML
```

**Step 4.** Validate the file and start the server.

```sh
bin/weir check --config weir.yaml
bin/weir serve --config weir.yaml
```

Expected output:

```text
configuration valid
Weir listening on 127.0.0.1:7447
Diagnostics listening on 127.0.0.1:7449
```

**Step 5.** In a second terminal, check readiness and run the SDK example.

```sh
curl http://127.0.0.1:7449/readyz
git clone https://github.com/batchstream/weir-go.git
cd weir-go
go run ./examples/basic -count 1
```

Expected output of `curl` and the example:

```text
ready
index=0 record=29 bytes missing=false
completed 1 reads with indexed stream responses
```

**Step 6.** Stop the server with `Ctrl-C`, then remove MongoDB.

```sh
docker rm -f weir-mongo
```

This setup uses MongoDB direct mode, which has no failover. For production, see
[Configuration](docs/configuration.md#mongodb) and
[weir-charts](https://github.com/batchstream/weir-charts). To write your own
client, see [weir-go](https://github.com/batchstream/weir-go).

## Documentation

| Guide | Contents |
| --- | --- |
| [Configuration](docs/configuration.md) | Loading, listeners, Stores and authentication |
| [Reference configuration](config/weir.reference.yaml) | Every supported field |
| [Payloads](docs/payloads.md) | Resources, documents, mutation outcomes and Native formats |
| [Lua](docs/lua.md) | Lua transforms |
| [Batching](docs/batching.md) | Queue, batch, wait and concurrency tuning |
| [Architecture](docs/architecture.md) | Ownership, execution, memory and shutdown |
| [Operations](docs/operations.md) | Health, metrics, alerts, memory pressure and failure handling |
| [Protocols](docs/protocols.md) | Public contract and module boundaries |
| [Performance](docs/performance.md) | Benchmark setup and results |
| [Development](docs/development.md) | Integration, load and fault tests, and releases |

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
go vet -tags=integration ./...
```

Default tests need no backends. See [Development](docs/development.md) for
integration, load and fault tests.

- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security reports: [SECURITY.md](SECURITY.md)

## Related repositories

| Repository | Purpose |
| --- | --- |
| **[weir](https://github.com/batchstream/weir)** | Server (this repository) |
| [weir-go](https://github.com/batchstream/weir-go) | Go client SDK |
| [weir-protocol](https://github.com/batchstream/weir-protocol) | Public gRPC schema and validation |
| [weir-charts](https://github.com/batchstream/weir-charts) | Helm chart |
| [weir-tests](https://github.com/batchstream/weir-tests) | System tests and benchmarks |

## License

MIT. See [LICENSE](LICENSE).
