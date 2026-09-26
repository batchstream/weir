#!/bin/sh
# Creates/restarts only this task-owned loopback replica set; never adopts unmarked data.
set -eu
cd "$(dirname "$0")/.."
root="$PWD/.testdata/mongo-m1"
binary="$PWD/.tools/mongodb-macos-aarch64--8.0.32/bin/mongod"
case "${1:-start}" in
 start)
  test -x "$binary"
  command -v mongosh >/dev/null
  test "$(mongosh --version)" = 2.6.0
  if [ -e "$root" ]; then
   test -f "$root/.weir-owner"
   test "$(cat "$root/.weir-owner")" = 'weir-milestone-1 mongodb-8.0.32 loopback-27028'
   if [ -f "$root/mongod.pid" ]; then
    args=$(ps -p "$(cat "$root/mongod.pid")" -o args= || true)
    case "$args" in *"$root"*) echo 'Task replica set already running'; exit 0;; esac
   fi
  else
   mkdir -p "$root"
   printf 'weir-milestone-1 mongodb-8.0.32 loopback-27028\n' > "$root/.weir-owner"
  fi
  "$binary" --dbpath "$root" --bind_ip 127.0.0.1 --port 27028 --replSet weir_m1 --setParameter enableTestCommands=1 --logpath "$root/mongod.log" --pidfilepath "$root/mongod.pid" --fork
  mongosh --quiet --norc 'mongodb://127.0.0.1:27028/?directConnection=true' --eval 'try { rs.status() } catch(e) { if(e.code!==94)throw e; rs.initiate({_id:"weir_m1",members:[{_id:0,host:"127.0.0.1:27028"}]}) }; for(let i=0;i<100;i++){if(db.hello().isWritablePrimary)break;sleep(100)}; if(rs.conf()._id!=="weir_m1")throw new Error("wrong replica set"); const d=db.getSiblingDB("weir_m1"); if(!d.getCollectionNames().includes("records"))d.createCollection("records")'
  ;;
 stop)
  # mongod shutdown only targets this task's loopback process/data path.
  if [ ! -f "$root/mongod.pid" ]; then echo 'No task process recorded' >&2; exit 1; fi
  pid=$(cat "$root/mongod.pid")
  args=$(ps -p "$pid" -o args= || true)
  case "$args" in *"$root"*) kill -TERM "$pid";; *) echo 'PID ownership mismatch; refusing to signal' >&2;exit 1;; esac
  ;;
 *) echo 'usage: scripts/mongo-local.sh start|stop' >&2;exit 1;;
esac
