#!/bin/sh
# Creates/restarts only this task-owned replica set; never adopts unmarked data.
# WEIR_MONGOD overrides the mongod binary. WEIR_MONGO_BIND adds addresses to the
# default loopback bind, e.g. the Docker bridge gateway on a Linux CI runner.
set -eu
cd "$(dirname "$0")/.."
root="$PWD/.testdata/mongo-local"
binary=${WEIR_MONGOD:-"$PWD/.tools/mongodb-macos-aarch64--8.0.32/bin/mongod"}
bind=127.0.0.1${WEIR_MONGO_BIND:+,$WEIR_MONGO_BIND}
case "${1:-start}" in
 start)
  test -x "$binary"
  if [ -e "$root" ]; then
   test -f "$root/.weir-owner"
   test "$(cat "$root/.weir-owner")" = 'weir-mongo-local mongodb-8.0.32 loopback-27028'
   if [ -f "$root/mongod.pid" ]; then
    args=$(ps -p "$(cat "$root/mongod.pid")" -o args= || true)
    case "$args" in *"$root"*) echo 'Task replica set already running'; exit 0;; esac
   fi
  else
   mkdir -p "$root"
   printf 'weir-mongo-local mongodb-8.0.32 loopback-27028\n' > "$root/.weir-owner"
  fi
  "$binary" --dbpath "$root" --bind_ip "$bind" --port 27028 --replSet weir_local --setParameter enableTestCommands=1 --logpath "$root/mongod.log" --pidfilepath "$root/mongod.pid" --fork
  go run ./internal/testutil/testmongo/localinit -address 127.0.0.1:27028
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
