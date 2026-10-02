set -eu
mkfifo /bench/mongo-current-pilot/mongo-current-pilot.pipe
cat /bench/mongo-current-pilot/mongo-current-pilot.pipe > /bench/mongo-current-pilot/mongo-current-pilot-client.jsonl &
reader=$!
env WEIR_CAPACITY_INTEGRATION=1 /bench/client-final -mode trial -backend 'mongodb://127.0.0.1:27017/?directConnection=true' -target 127.0.0.1:7447 -rate 2500 -warm 20 -seconds 60 -prefix mongo-current-pilot -pool 4 -workers 32 -write-every 10 -arrival-expiry-ms 100 -max-catchup 512 -client-queue 256 -load-delay-ms 3000 -mutation-reservation 21000 > /bench/mongo-current-pilot/mongo-current-pilot.pipe 2> /bench/mongo-current-pilot/mongo-current-pilot-client-stderr.log
wait "$reader"
touch /bench/mongo-current-pilot/mongo-current-pilot.done
