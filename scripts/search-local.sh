#!/bin/sh
# Only manages labeled, disposable milestone containers. Never changes host sysctls.
set -eu
cd "$(dirname "$0")/.."
action=${1:-start}
product=${2:-elasticsearch}
case "$product" in
 elasticsearch)
  name=weir-m17-elasticsearch
  port=19200
  version=8.19.22
  image=docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237
  ;;
 opensearch)
  name=weir-m17-opensearch
  port=19201
  version=2.19.6
  image=opensearchproject/opensearch@sha256:89a402aa9132286200b8d12aa37fd5b14daa65851193d009355900c0d1d9d59c
  ;;
 *) echo 'product must be elasticsearch or opensearch' >&2; exit 1;;
esac
if docker container inspect "$name" >/dev/null 2>&1; then
 test "$(docker inspect -f '{{index .Config.Labels "weir.owner"}}' "$name")" = weir-milestone-17
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
  elif [ "$product" = elasticsearch ]; then
   docker run --pull=never --platform=linux/arm64 -d --name "$name" --label weir.owner=weir-milestone-17 --memory=1536m --cpus=2 \
    -p "127.0.0.1:$port:9200" -e "cluster.name=$name" -e discovery.type=single-node \
    -e xpack.security.enabled=false -e action.auto_create_index=false \
    -e thread_pool.write.size=1 -e thread_pool.write.queue_size=1 \
    -e 'ES_JAVA_OPTS=-Xms512m -Xmx512m' "$image"
  else
   docker run --pull=never --platform=linux/arm64 -d --name "$name" --label weir.owner=weir-milestone-17 --memory=1536m --cpus=2 \
    -p "127.0.0.1:$port:9200" -e "cluster.name=$name" -e discovery.type=single-node \
    -e DISABLE_SECURITY_PLUGIN=true -e DISABLE_INSTALL_DEMO_CONFIG=true -e action.auto_create_index=false \
    -e thread_pool.write.size=1 -e thread_pool.write.queue_size=1 \
    -e 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' "$image"
  fi
  # Small native write queues make real overload rejection reproducible, not production sizing.
  for attempt in $(seq 1 90); do
   if curl -fsS --max-time 2 "http://127.0.0.1:$port/" > /dev/null 2>&1; then
    curl -fsS --max-time 2 "http://127.0.0.1:$port/" | python3 -c 'import json,sys; x=json.load(sys.stdin); v=x["version"]; assert x["cluster_name"]==sys.argv[1] and v["number"]==sys.argv[2]; assert (v.get("distribution")=="opensearch") if sys.argv[3]=="opensearch" else (v.get("distribution", "")=="" and v.get("build_flavor")=="default"); print(json.dumps(x,sort_keys=True))' "$name" "$version" "$product"
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
  id=$(docker inspect -f '{{.Id}}' "$name")
  mkdir -p .testdata
  docker logs "$name" > ".testdata/$name-$id.log" 2>&1
  docker rm "$name"
  ;;
 *) echo 'usage: scripts/search-local.sh start|stop elasticsearch|opensearch' >&2;exit 1;;
esac
