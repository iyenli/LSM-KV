# LSM KV System

## Description

Log-structured merge-tree, is a data structure with performance characteristics that make it attractive for providing indexed access to files with high insert volume, such as transactional log data. LSM trees, like other search trees, maintain key-value pairs. LSM trees maintain data in two or more separate structures, each of which is optimized for its respective underlying storage medium; data is synchronized between the two structures efficiently, in batches. 

LSM trees are used in data stores such as [Apache AsterixDB](https://asterixdb.apache.org/), [Bigtable](https://en.wikipedia.org/wiki/Bigtable), [HBase](https://en.wikipedia.org/wiki/HBase), [LevelDB](https://en.wikipedia.org/wiki/LevelDB), [SQLite4](https://en.wikipedia.org/wiki/SQLite4), [Tarantool](https://en.wikipedia.org/wiki/Tarantool), [RocksDB](https://en.wikipedia.org/wiki/RocksDB), [WiredTiger](https://en.wikipedia.org/wiki/WiredTiger), [Apache Cassandra](https://en.wikipedia.org/wiki/Apache_Cassandra), [InfluxDB](https://en.wikipedia.org/wiki/InfluxDB) and [ScyllaDB](https://en.wikipedia.org/wiki/Scylla_(database)). (Wikipedia).

In my implementation, the structrue I used as followed:

<img src="https://i.loli.net/2021/07/12/tsxWUqlchvy2fbY.png" alt="image-20210712221023639" style="zoom: 67%;" />

<img src="https://i.loli.net/2021/07/12/BYR8zZ39hKPXq6J.png" alt="image-20210712221044428" style="zoom:67%;" />

I used cache memory and bloom filter to improve access speed. More about this project, You can visit`/LSM-KV\LSM-Request` to get.

## Arch

```tex
----|
	| LSM-Release Final release, with bloom filter and cache memory
	| LSM-Cache Just for test, without bloom filter
	| LSM-Disk-Only Just for test, without bloom filter and cache memory
```

## Contact

Only pdf was commited, if you need source test data or latex file, please contact with author.

yiyanleee@gmail.com
