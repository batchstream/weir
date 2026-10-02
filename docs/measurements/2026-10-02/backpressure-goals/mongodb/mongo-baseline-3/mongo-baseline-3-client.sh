set -eu
mkfifo /bench/mongo-baseline-3/mongo-baseline-3.pipe
cat /bench/mongo-baseline-3/mongo-baseline-3.pipe > /bench/mongo-baseline-3/mongo-baseline-3-client.jsonl &
reader=$!
env WEIR_CAPACITY_INTEGRATION=1 /bench/client-final -mode trial -backend 'mongodb://127.0.0.1:27017/?directConnection=true' -target 127.0.0.1:7447 -rate 2500 -warm 20 -seconds 90 -prefix mongo-baseline-3 -pool 4 -workers 32 -write-every 10 -arrival-expiry-ms 100 -max-catchup 512 -client-queue 32 -load-delay-ms 3000 -mutation-reservation 28500 > /bench/mongo-baseline-3/mongo-baseline-3.pipe 2> /bench/mongo-baseline-3/mongo-baseline-3-client-stderr.log
wait "$reader"
touch /bench/mongo-baseline-3/mongo-baseline-3.done
