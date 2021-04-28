# KVBench

Golang KV databases benchmarks.

_*Based on [smallnest/kvbench](https://github.com/smallnest/kvbench).*_

Features:

- Databases
  - [Badger](https://github.com/dgraph-io/badger)
  - [BuntDB](https://github.com/tidwall/buntdb)
  - [LevelDB](https://github.com/syndtr/goleveldb)
  - [NutsDB](https://github.com/xujiajun/nutsdb)
  - [Pebble](https://github.com/cockroachdb/pebble)
  - [Pogreb](https://github.com/akrylysov/pogreb)
- Option to disable fsync

## SSD benchmark

The following benchmarks show the throughput of inserting/reading keys (of size
9 bytes) and values (of size 256 bytes).

### nofsync

- **throughputs**

| name    | set    | get     | set-mixed | get-mixed | del     |
| ------- | ------ | ------- | --------- | --------- | ------- |
| badger  | 136381 | 353380  | 11117     | 404656    | 190054  |
| buntdb  | 36449  | 4956283 | 2275      | 27684     | 1137630 |
| leveldb | 240212 | 917005  | 30420     | 507333    | 233782  |
| nutsdb  | 163634 | 1523143 | 38053     | 487173    | 141004  |
| pebble  | 147759 | 474609  | 228020    | 315736    | 745286  |
| pogreb  | 92540  | 4335120 | 46682     | 561806    | 263947  |

- **time (latency)**

| name    | set  | get | set-mixed | get-mixed | del   |
| ------- | ---- | --- | --------- | --------- | ----- |
| badger  | 646  | 193 | 95926     | 212       | 420   |
| buntdb  | 4840 | 16  | 173138    | 1194      | 27136 |
| leveldb | 341  | 93  | 33234     | 164       | 359   |
| nutsdb  | 518  | 40  | 31425     | 211       | 521   |
| pebble  | 574  | 234 | 4398      | 266       | 109   |
| pogreb  | 901  | 20  | 21473     | 148       | 270   |

### fsync

- **throughputs**

| name    | set  | get     | set-mixed | get-mixed | del   |
| ------- | ---- | ------- | --------- | --------- | ----- |
| badger  | 1203 | 733283  | 1301      | 15659     | 1163  |
| buntdb  | 38   | 5550962 | 36        | 455       | 3241  |
| leveldb | 257  | 2265868 | 39        | 2257516   | 48422 |
| nutsdb  | 39   | 3938206 | 39        | 592       | 39    |
| pebble  | 225  | 4720694 | 38        | 4589332   | 239   |
| pogreb  | 39   | 4564589 | 39        | 510       | 39    |

- **time (latency)**

| name    | set  | get | set-mixed | get-mixed | del |
| ------- | ---- | --- | --------- | --------- | --- |
| badger  | 611  | 235 | 89945     | 205       | 438 |
| buntdb  | 2286 | 16  | 439442    | 3010      | 73  |
| leveldb | 346  | 90  | 32872     | 164       | 356 |
| nutsdb  | 509  | 54  | 26278     | 171       | 590 |
| pebble  | 563  | 175 | 4385      | 263       | 111 |
| pogreb  | 900  | 19  | 21421     | 148       | 315 |
