#!/bin/sh
# Only manages labeled, disposable milestone containers. Never changes host sysctls.
set -eu
cd "$(dirname "$0")/.."
action=${1:-start}
product=${2:-elasticsearch}
case "$product" in
 elasticsearch)
  name=weir-m2-elasticsearch
  port=19200
  image=docker.elastic.co/elasticsearch/elasticsearch@sha256:2f602552550869fb29b6fd5848c5118d3ef3a2e1d5d45802e3ab9088cb2de8e2
  ;;
 opensearch)
  name=weir-m2-opensearch
  port=19201
  image=opensearchproject/opensearch@sha256:1f8b88245a6af61e7aa500afe0e87d43401e4b33140bb47230a919428ce3f7cb
  ;;
 *) echo 'product must be elasticsearch or opensearch' >&2; exit 1;;
esac
if docker container inspect "$name" >/dev/null 2>&1; then
 test "$(docker inspect -f '{{index .Config.Labels "weir.owner"}}' "$name")" = weir-milestone-2
 test "$(docker inspect -f '{{.Config.Image}}' "$name")" = "$image"
 exists=true
else
 exists=false
fi
case "$action" in
 start)
  if "$exists"; then
   docker start "$name" >/dev/null
  elif [ "$product" = elasticsearch ]; then
   docker run -d --name "$name" --label weir.owner=weir-milestone-2 --memory=1536m --cpus=2 \
    -p "127.0.0.1:$port:9200" -e "cluster.name=$name" -e discovery.type=single-node \
    -e xpack.security.enabled=false -e action.auto_create_index=false \
    -e thread_pool.write.size=1 -e thread_pool.write.queue_size=1 \
    -e 'ES_JAVA_OPTS=-Xms512m -Xmx512m' "$image"
  else
   docker run -d --name "$name" --label weir.owner=weir-milestone-2 --memory=1536m --cpus=2 \
    -p "127.0.0.1:$port:9200" -e "cluster.name=$name" -e discovery.type=single-node \
    -e DISABLE_SECURITY_PLUGIN=true -e DISABLE_INSTALL_DEMO_CONFIG=true -e action.auto_create_index=false \
    -e thread_pool.write.size=1 -e thread_pool.write.queue_size=1 \
    -e 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' "$image"
  fi
  # Small native write queues make real overload rejection reproducible, not production sizing.
  for attempt in $(seq 1 90); do
   if curl -fsS --max-time 2 "http://127.0.0.1:$port/" > /dev/null 2>&1; then
    curl -fsS --max-time 2 "http://127.0.0.1:$port/"
    exit 0
   fi
   sleep 1
  done
  echo 'backend failed to become ready; inspect its container logs (do not change host sysctls)' >&2
  exit 1
  ;;
 stop)
  "$exists" || exit 0
  docker stop --timeout 15 "$name"
  ;;
 *) echo 'usage: scripts/search-local.sh start|stop elasticsearch|opensearch' >&2;exit 1;;
esac
