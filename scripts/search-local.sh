#!/bin/sh
# Only manages labeled, disposable local-test containers. Never changes host sysctls.
set -eu
cd "$(dirname "$0")/.."
action=${1:-start}
product=${2:-elasticsearch}
capacity=${3:-overload}
case "$capacity" in
 normal) set -- ;;
 overload) set -- -e thread_pool.write.size=1 -e thread_pool.write.queue_size=1 ;;
 *) echo 'capacity must be normal or overload' >&2; exit 1;;
esac
case "$(uname -m)" in
 arm64|aarch64) platform=linux/arm64;;
 x86_64|amd64) platform=linux/amd64;;
 *) echo 'unsupported host architecture' >&2; exit 1;;
esac
case "$product" in
 elasticsearch)
  name=weir-local-elasticsearch
  port=19200
  version=8.19.22
  if [ "$platform" = linux/arm64 ]; then
   image=docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237
  else
   image=docker.elastic.co/elasticsearch/elasticsearch@sha256:f1e3d88299cb0db2131fc2ab9060faefccd88fd684d11a49256ae00d7dc3c857
  fi
  ;;
 opensearch)
  name=weir-local-opensearch-3.3
  port=19202
  version=3.3.0
  if [ "$platform" = linux/arm64 ]; then
   image=opensearchproject/opensearch@sha256:d96afaf6cbd2a6a3695aeb2f1d48c9a16ad5c8918eb849e5cbf43475f0f8e146
  else
   image=opensearchproject/opensearch@sha256:59e9e49ac00f19c2af9094e367c8e6586f7d07e6c06465c3f4c43b40934309f2
  fi
  ;;
 *) echo 'product must be elasticsearch or opensearch' >&2; exit 1;;
esac
if docker container inspect "$name" >/dev/null 2>&1; then
 test "$(docker inspect -f '{{index .Config.Labels "weir.owner"}}' "$name")" = weir-search-local
 test "$(docker inspect -f '{{.Config.Image}}' "$name")" = "$image"
 exists=true
else
 exists=false
fi
case "$action" in
 start)
  if "$exists"; then
   echo 'fixture already exists; stop it before starting a fresh empty fixture' >&2
   exit 1
  fi
  # The load test keeps 16 slow 8 MiB Native reads open next to steady traffic.
  # A 1 GiB heap holds them, and accounting-based parent breaking avoids 429s
  # that depend only on when the JVM last collected garbage.
  if [ "$product" = elasticsearch ]; then
   docker run --pull=missing --platform="$platform" -d --name "$name" --label weir.owner=weir-search-local --memory=2g --cpus=2 \
    -p "127.0.0.1:$port:9200" -e "cluster.name=$name" -e discovery.type=single-node \
    -e xpack.security.enabled=false -e action.auto_create_index=false -e indices.breaker.total.use_real_memory=false \
    "$@" \
    -e 'ES_JAVA_OPTS=-Xms1g -Xmx1g' "$image"
  else
   # Query Insights exports and the ML Commons sync job write system indexes
   # in the background; with the overload write pool they would compete with
   # test writes at arbitrary times.
   docker run --pull=missing --platform="$platform" -d --name "$name" --label weir.owner=weir-search-local --memory=2g --cpus=2 \
    -p "127.0.0.1:$port:9200" -e "cluster.name=$name" -e discovery.type=single-node \
    -e DISABLE_SECURITY_PLUGIN=true -e DISABLE_INSTALL_DEMO_CONFIG=true -e action.auto_create_index=false \
    -e indices.breaker.total.use_real_memory=false \
    -e search.insights.top_queries.latency.enabled=false -e search.insights.top_queries.cpu.enabled=false \
    -e search.insights.top_queries.memory.enabled=false -e plugins.ml_commons.sync_up_job_interval_in_seconds=0 \
    "$@" \
    -e 'OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g' "$image"
  fi
  # Overload tests use small native queues; positive batch smoke uses normal capacity.
  ready=false
  for attempt in $(seq 1 90); do
   if curl -fsS --max-time 2 "http://127.0.0.1:$port/" > /dev/null 2>&1; then
    curl -fsS --max-time 2 "http://127.0.0.1:$port/" | python3 -c 'import json,sys; x=json.load(sys.stdin); v=x["version"]; assert x["cluster_name"]==sys.argv[1] and v["number"]==sys.argv[2]; assert (v.get("distribution")=="opensearch") if sys.argv[3].startswith("opensearch") else (v.get("distribution", "")=="" and v.get("build_flavor")=="default"); print(json.dumps(x,sort_keys=True))' "$name" "$version" "$product"
    ready=true
    break
   fi
   sleep 1
  done
  if ! "$ready"; then
   echo 'backend failed to become ready; inspect its container logs (do not change host sysctls)' >&2
   exit 1
  fi
  # Startup plugins (for example OpenSearch Security Analytics) bulk-load system
  # indexes right after HTTP is up. Wait until the write pool has been idle with
  # an unchanged completed count for 5 consecutive seconds.
  curl -fsS --max-time 35 "http://127.0.0.1:$port/_cluster/health?wait_for_status=green&wait_for_no_initializing_shards=true&wait_for_events=languid&timeout=30s" > /dev/null
  previous=
  quiet=0
  for attempt in $(seq 1 60); do
   current=$(curl -fsS --max-time 2 "http://127.0.0.1:$port/_nodes/_local/stats/thread_pool" | python3 -c 'import json,sys; w=next(iter(json.load(sys.stdin)["nodes"].values()))["thread_pool"]["write"]; print("%d/%d/%d" % (w["active"], w["queue"], w["completed"]))')
   case "$current" in
    0/0/*) if [ "$current" = "$previous" ]; then quiet=$((quiet + 1)); else quiet=0; fi ;;
    *) quiet=0 ;;
   esac
   if [ "$quiet" -ge 5 ]; then
    exit 0
   fi
   previous=$current
   sleep 1
  done
  echo 'backend write pool did not become idle; inspect its container logs' >&2
  exit 1
  ;;
 stop)
  "$exists" || exit 0
  docker stop --timeout 15 "$name"
  id=$(docker inspect -f '{{.Id}}' "$name")
  mkdir -p .testdata
  docker logs "$name" > ".testdata/$name-$id.log" 2>&1
  docker rm "$name"
  ;;
 *) echo 'usage: scripts/search-local.sh start|stop elasticsearch|opensearch [normal|overload]' >&2;exit 1;;
esac
