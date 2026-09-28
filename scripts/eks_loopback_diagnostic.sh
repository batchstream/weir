# Read-only terminal init: this script never sources the management bootstrap.
set -euo pipefail
uname -smr
id
readlink /proc/self/ns/net
source /qualification/loopback-check.sh
printf '{"diagnostic":"complete","stop_exit":42,"management_mutations":0,"document_mutations":0}\n'
exit 42
