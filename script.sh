#!/usr/bin/env bash

counter=0
base=10010

ARRAY[INDEXNR]=value


set -e

while [ $counter -le 10 ]
do
if [ ! -p ./in/$((counter + base)) ]; then
    mkfifo ./in/$((counter + base))
fi

tail -f  ./in/$((counter + base)) | java -jar target/ASD-1.0-jar-with-dependencies.jar listen_base_port=$((counter + base)) Contact=127.0.0.1:10010 >> ./logs/$((counter + base)) &
sleep 2s
((counter++))
done

counter=0
base=10010


while [ $counter -le 10 ]
do

echo "subscribe all" > ./in/$((counter + base))

((counter++))
done

#kill $(ps aux | grep "$target/ASD" | grep -v 'grep' | awk '{print $2}')
