#!/bin/bash
# Fixed diagnostic bytes only. The caller supplies the 19s + 1s kill bound.
set -euo pipefail
export LC_ALL=C
[[ $# == 1 && ( $1 == immediate || $1 == eof ) ]] || exit 64
[[ ${BASH_VERSINFO[0]} -ge 4 ]] || exit 69
[[ -p /dev/stdin ]] || exit 71
pause_pid=''
finish() {
    result=$?
    trap - EXIT
    if [[ -n $pause_pid ]]; then
        kill "$pause_pid" 2>/dev/null || true
        wait "$pause_pid" 2>/dev/null || true
    fi
    exit "$result"
}
trap finish EXIT
trap 'exit 75' TERM INT HUP
reject_early_input() {
    if IFS= read -r -t 0; then
        printf 'early-input-or-eof\n' >&2
        exit 72
    fi
}
reject_early_input
padding=X
for bit in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do padding=$padding$padding; done
padding=$padding${padding:0:16336}
printf '%s\n' '{"kind":"identity","schema":1,"diagnostic":"m30r7-fixed-bytes"}'
for sequence in 0 1 2 3 4 5; do
    if [[ $sequence != 0 ]]; then
        /bin/sleep 2 &
        pause_pid=$!
        wait "$pause_pid"
        pause_pid=''
    fi
    reject_early_input
    printf '{"kind":"diagnostic","sequence":%s,"padding":"%s"}\n' "$sequence" "$padding"
done
reject_early_input
printf '%s\n' '{"kind":"terminal","records":6,"diagnostic":"m30r7-fixed-bytes"}'
if [[ $1 == immediate ]]; then exit 0; fi
byte=''
status=0
IFS= read -r -n 1 -t 4 byte || status=$?
if [[ $status == 1 && -z $byte && -p /dev/stdin ]]; then
    printf 'ack-eof\n' >&2
    exit 0
fi
if [[ $status -gt 128 ]]; then
    printf 'ack-timeout\n' >&2
    exit 74
fi
printf 'ack-invalid-input-or-read-error status=%s\n' "$status" >&2
exit 73
