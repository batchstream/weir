set -euo pipefail
source /qualification/loopback-check.sh
# This is the sole pre-trial management write, not a document seed. A failed or
# ambiguous response terminates the Never init; no mutation is replayed.
printf '{"management":"create-empty-records","reserved":1,"started":1}\n'
body='{"settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"1s","translog.durability":"request"},"mappings":{"enabled":false}}'
reply=$(curl "${curl_args[@]}" -X PUT -H 'Content-Type: application/json' --data-binary "$body" http://127.0.0.1:9200/records)
printf '%s\n' "$reply"
[[ ${reply//[[:space:]]/} == '{"acknowledged":true,"shards_acknowledged":true,"index":"records"}' ]] || exit 27
printf '{"management":"create-empty-records","completed":1,"document_mutations":0}\n'
