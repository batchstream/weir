# Sourced only by the fixed ES image's bash. No external endpoint discovery.
set -euo pipefail
ulimit -f 1024
command -v curl
command -v nc
command -v timeout
[[ ${POD_IP:-} =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 20
curl_args=(-q --silent --show-error --fail --noproxy '*' --proxy '' --proto '=http' --max-redirs 0 --retry 0 --connect-timeout 1 --max-time 3 --max-filesize 262144)
version=$(curl "${curl_args[@]}" http://127.0.0.1:9200/)
[[ ${version//[[:space:]]/} == *'"number":"8.19.22"'* ]] || exit 21
settings=$(curl "${curl_args[@]}" 'http://127.0.0.1:9200/_nodes/_local/settings?flat_settings=true&filter_path=nodes.*.settings')
compact=${settings//[[:space:]]/}
for pair in 'network.host=127.0.0.1' 'http.host=127.0.0.1' 'transport.host=127.0.0.1' 'discovery.type=single-node' 'action.auto_create_index=false' 'xpack.security.enabled=false' 'xpack.ml.enabled=false' 'ingest.geoip.downloader.enabled=false' 'node.store.allow_mmap=false'; do
  key=${pair%=*}; value=${pair#*=}
  [[ $compact == *\"$key\":\"$value\"* ]] || exit 22
done
printf '%s\n%s\n' "$version" "$settings"
printf 'own-pod-ip=%s\n' "$POD_IP"
# Capture before judging. proc iterators and the four table reads are not atomic;
# every decision below nevertheless uses exactly the bytes printed here.
LC_ALL=C
socket_output_failed=false
socket_log() {
  if ! printf "$@"; then socket_output_failed=true; fi
}
socket_reject() {
  socket_log 'socket-reject table=%s exit=%s reason=%s\n' "${table:-unknown}" "$1" "$2"
  exit "$1"
}
socket_tables=(/proc/net/tcp /proc/net/tcp6 /proc/net/udp /proc/net/udp6)
socket_snapshots=()
for table in "${socket_tables[@]}"; do
  socket_raw=''
  socket_log 'socket-table-begin table=%s atomic=false max_bytes=65536 max_lines=256 read_seconds=2 process=unknown\n' "$table"
  [[ -r $table ]] || socket_reject 33 missing-table
  if IFS= read -r -d '' -n 65537 -t 2 socket_raw < "$table"; then socket_code=0; else socket_code=$?; fi
  socket_log '%s' "$socket_raw"
  socket_log '\nsocket-table-end table=%s bytes=%s read_status=%s\n' "$table" "${#socket_raw}" "$socket_code"
  [[ $socket_code -le 128 ]] || socket_reject 34 read-timeout
  [[ ${#socket_raw} -le 65536 ]] || socket_reject 35 table-byte-limit
  [[ $socket_code == 1 && $socket_raw == *$'\n' ]] || socket_reject 33 incomplete-table
  socket_lines=0
  while IFS= read -r row; do
    socket_lines=$((socket_lines+1))
    [[ $socket_lines -le 256 ]] || socket_reject 37 table-line-limit
  done <<< "${socket_raw%$'\n'}"
  socket_snapshots[${#socket_snapshots[@]}]=$socket_raw
done
seen=' '
for socket_index in 0 1 2 3; do
  table=${socket_tables[$socket_index]}
  first=true
  while IFS= read -r row; do
    if $first; then
      first=false
      [[ $row == *local_address* && $row == *inode* ]] || socket_reject 33 missing-header
      continue
    fi
    [[ -n $row ]] || continue
    read -r -a socket_fields <<< "$row"
    local=${socket_fields[1]:-unknown}; remote=${socket_fields[2]:-unknown}; state=${socket_fields[3]:-unknown}
    socket_log 'socket-row table=%s local=%s remote=%s state=%s uid=%s inode=%s process=unknown raw=%s\n' "$table" "$local" "$remote" "$state" "${socket_fields[7]:-unknown}" "${socket_fields[9]:-unknown}" "$row"
    # UDP's original rule rejects every nonempty data row, including short rows.
    [[ $socket_index -lt 2 ]] || socket_reject 32 udp-row
    [[ ${#socket_fields[@]} -ge 10 ]] || socket_reject 33 short-row
    address=${local%:*}; peer=${remote%:*}
    [[ $address == 0100007F || $address == 0000000000000000FFFF00000100007F ]] || socket_reject 23 tcp-local-address
    if [[ $state == 0A ]]; then
      port=${local##*:}
      case "$port" in
        23F0|2454) ;;
        1D17|1D19) [[ ${1:-bootstrap} == main ]] || socket_reject 28 main-listener-before-bootstrap ;;
        *) socket_reject 29 unexpected-listener ;;
      esac
      seen+="$port "
    else
      [[ $peer == 0100007F || $peer == 0000000000000000FFFF00000100007F || $peer == 00000000 || $peer == 00000000000000000000000000000000 ]] || socket_reject 24 tcp-peer-address
    fi
  done <<< "${socket_snapshots[$socket_index]}"
  # Keep missing-listener precedence before the UDP decisions, as before.
  if [[ $socket_index == 1 ]]; then
    for port in 23F0 2454; do [[ $seen == *" $port "* ]] || socket_reject 30 required-es-listener; done
    if [[ ${1:-bootstrap} == main ]]; then
      for port in 1D17 1D19; do [[ $seen == *" $port "* ]] || socket_reject 31 required-main-listener; done
    fi
  fi
done
[[ $socket_output_failed == false ]] || socket_reject 36 diagnostic-output-failed
for port in 9200 9300 7447 7449; do
  if timeout 2 nc -z -w 1 "$POD_IP" "$port"; then exit 25; else code=$?; [[ $code == 1 ]] || exit 26; fi
done
printf 'loopback-check-complete\n'
