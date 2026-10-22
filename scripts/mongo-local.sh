#!/bin/sh
# Creates/restarts only this script's fixture replica set; never adopts unmarked data.
# WEIR_MONGOD overrides the mongod binary. WEIR_MONGO_BIND adds comma-separated
# literal IP addresses to the default loopback bind, e.g. the Docker bridge
# gateway on a Linux CI runner. Empty and wildcard entries are refused.
set -euf
cd "$(dirname "$0")/.."
root="$PWD/.testdata/mongo-local"
binary=${WEIR_MONGOD:-"$PWD/.tools/mongodb-macos-aarch64--8.0.32/bin/mongod"}
owner='weir-mongo-local mongodb-8.0.32 port-27028'

valid_ipv4() {
 case "$1" in *[!0-9.]*|.*|*.|*..*) return 1;; esac
 octet_ifs=$IFS; IFS=.
 # shellcheck disable=SC2086
 set -- $1
 IFS=$octet_ifs
 [ "$#" -eq 4 ] || return 1
 for octet in "$@"; do
  case "$octet" in ?|[1-9]?|1??|2[0-4]?|25?) ;; *) return 1;; esac
  [ "$octet" -le 255 ] || return 1
 done
 [ "$1.$2.$3.$4" != 0.0.0.0 ]
}

valid_ipv6() {
 case "$1" in *[!0-9a-fA-F:]*|*:::*|'') return 1;; *:*) ;; *) return 1;; esac
 # An address of only zeros and colons is the unspecified (wildcard) address.
 case "$1" in *[1-9a-fA-F]*) return 0;; *) return 1;; esac
}

bind=127.0.0.1
if [ -n "${WEIR_MONGO_BIND+set}" ]; then
 case "$WEIR_MONGO_BIND" in ''|,*|*,|*,,*) echo "WEIR_MONGO_BIND has an empty entry: '$WEIR_MONGO_BIND'" >&2; exit 1;; esac
 bind_ifs=$IFS; IFS=,
 for address in $WEIR_MONGO_BIND; do
  if ! valid_ipv4 "$address" && ! valid_ipv6 "$address"; then
   echo "WEIR_MONGO_BIND entry is not a literal non-wildcard IP address: '$address'" >&2
   exit 1
  fi
 done
 IFS=$bind_ifs
 bind="$bind,$WEIR_MONGO_BIND"
fi

case "${1:-start}" in
 start)
  test -x "$binary"
  if [ -e "$root" ]; then
   test -f "$root/.weir-owner"
   test "$(cat "$root/.weir-owner")" = "$owner"
   if [ -f "$root/mongod.pid" ]; then
    args=$(ps -ww -p "$(cat "$root/mongod.pid")" -o args= || true)
    case "$args" in
     *"$root"*)
      case "$args" in
       *" --bind_ip $bind --port "*) echo 'Fixture replica set already running'; exit 0;;
       *) echo "Fixture replica set is already running with a different bind; stop it before starting with --bind_ip $bind" >&2; exit 1;;
      esac
      ;;
    esac
   fi
  else
   mkdir -p "$root"
   printf '%s\n' "$owner" > "$root/.weir-owner"
  fi
  "$binary" --dbpath "$root" --bind_ip "$bind" --port 27028 --replSet weir_local --setParameter enableTestCommands=1 --logpath "$root/mongod.log" --pidfilepath "$root/mongod.pid" --fork
  go run ./internal/testutil/testmongo/localinit -address 127.0.0.1:27028
  ;;
 stop)
  # mongod shutdown only targets the process that owns this fixture's data path.
  if [ ! -f "$root/mongod.pid" ]; then echo 'No fixture process recorded' >&2; exit 1; fi
  pid=$(cat "$root/mongod.pid")
  args=$(ps -ww -p "$pid" -o args= || true)
  case "$args" in *"$root"*) kill -TERM "$pid";; *) echo 'PID ownership mismatch; refusing to signal' >&2;exit 1;; esac
  ;;
 *) echo 'usage: scripts/mongo-local.sh start|stop' >&2;exit 1;;
esac
