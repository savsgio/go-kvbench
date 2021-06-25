#!/usr/bin/env bash

SIZE=256

STORES=("badger" "buntdb" "leveldb" "nutsdb" "pebble" "pogreb")

export LD_LIBRARY_PATH=/usr/local/lib

`rm  -fr .*db`
`rm  -fr *.db`
`rm  -fr pogreb.*`
`rm -f benchmarks/test.log`

echo "=========== test nofsync ==========="
for i in "${STORES[@]}"
do
	dt=`date`

	echo "[$dt] Store: $i"
	./bin/kvbench -d 1m -size ${SIZE} -s "$i" >> benchmarks/test_$i.log 2>&1

	sleep 1m
done

`rm  -fr .*db`
`rm  -fr *.db`
`rm  -fr pogreb.*`

echo ""
echo "=========== test fsync ==========="

for i in "${STORES[@]}"
do
	dt=`date`

	echo "[$dt] Store: $i"
	./bin/kvbench -d 1m -size ${SIZE} -s "$i" -fsync >> benchmarks/test_$i.log 2>&1

	sleep 1m
done

`rm  -fr .*db`
`rm  -fr *.db`
`rm  -fr pogreb.*`
