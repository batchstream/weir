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
seen=' '
for table in /proc/net/tcp /proc/net/tcp6; do
  first=true
  while read -r slot local remote state rest; do
    if $first; then first=false; continue; fi
    [[ -n $slot ]] || continue
    address=${local%:*}; peer=${remote%:*}
    [[ $address == 0100007F || $address == 0000000000000000FFFF00000100007F ]] || exit 23
    if [[ $state == 0A ]]; then
      port=${local##*:}
      case "$port" in
        23F0|2454) ;;
        1D17|1D19) [[ ${1:-bootstrap} == main ]] || exit 28 ;;
        *) exit 29 ;;
      esac
      seen+="$port "
    else
      [[ $peer == 0100007F || $peer == 0000000000000000FFFF00000100007F || $peer == 00000000 || $peer == 00000000000000000000000000000000 ]] || exit 24
    fi
    printf '%s %s %s %s\n' "$table" "$local" "$remote" "$state"
  done < "$table"
done
for port in 23F0 2454; do [[ $seen == *" $port "* ]] || exit 30; done
if [[ ${1:-bootstrap} == main ]]; then
  for port in 1D17 1D19; do [[ $seen == *" $port "* ]] || exit 31; done
fi
for table in /proc/net/udp /proc/net/udp6; do
  first=true
  while read -r row; do
    if $first; then first=false; continue; fi
    [[ -z $row ]] || exit 32
  done < "$table"
done
for port in 9200 9300 7447 7449; do
  if timeout 2 nc -z -w 1 "$POD_IP" "$port"; then exit 25; else code=$?; [[ $code == 1 ]] || exit 26; fi
done
printf 'loopback-check-complete\n'
