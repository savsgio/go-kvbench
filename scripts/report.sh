#!/usr/bin/env bash

if [ $# != 0 ]
then
    logfile="$1"-test
else
    logfile=test
fi

`rm -f benchmarks/*.csv`

STORES=("badger" "buntdb" "leveldb" "nutsdb" "pebble" "pogreb")

echo "name,type,set,get,set-mixed,get-mixed,del" >> benchmarks/nofsync_throughputs.csv
echo "name,type,set,get,set-mixed,get-mixed,del" >> benchmarks/nofsync_time.csv
echo "name,type,set,get,set-mixed,get-mixed,del" >> benchmarks/fsync_throughputs.csv
echo "name,type,set,get,set-mixed,get-mixed,del" >> benchmarks/fsync_time.csv

for i in "${STORES[@]}"
do

    data=`grep -e ^${i}/nofsync  benchmarks/${logfile}_${i}.log|awk '{print $4}'|xargs| tr ' ' ','`
    echo "${i}/nofsync,${data}" >> benchmarks/nofsync_throughputs.csv
    data=`grep -e ^${i}/nofsync  benchmarks/${logfile}_${i}.log|awk '{print $7}'|xargs| tr ' ' ','`
    echo "${i}/nofsync,${data}" >> benchmarks/nofsync_time.csv

    data=`grep -e ^${i}/fsync  benchmarks/${logfile}_${i}.log|awk '{print $4}'|xargs| tr ' ' ','`
    echo "${i}/fsync,${data}" >> benchmarks/fsync_throughputs.csv
    data=`grep -e ^${i}/nofsync  benchmarks/${logfile}_${i}.log|awk '{print $7}'|xargs| tr ' ' ','`
    echo "${i}/fsync,${data}" >> benchmarks/fsync_time.csv
done
