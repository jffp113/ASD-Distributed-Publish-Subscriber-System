#!/usr/bin/env bash



counter=0
base=10010

rm ./logs/*

set -e

ARRAY=(0,0,1,0,2,1,3,2,4,8)


while [ $counter -le 10 ]
do

if [ ! -p ./in/$((counter + base)) ]; then
    mkfifo ./in/$((counter + base))
fi

tail -f  ./in/$((counter + base)) | java -jar target/ASD-1.0-jar-with-dependencies.jar listen_base_port=$((counter + base)) Contact=127.0.0.1:$((base + ARRAY[counter])) >> ./logs/$((counter + base)) &
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
