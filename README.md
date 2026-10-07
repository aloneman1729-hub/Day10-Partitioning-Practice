# Day 10 — Partitioning Practice

A Scala + Apache Spark project demonstrating how Spark partitions data and how partitioning can be optimized using RDD APIs.

## Objective

This project focuses on:

* Inspecting the number of partitions in an RDD
* Understanding partition data distribution
* Using `repartition()` to increase partitions
* Using `coalesce()` to decrease partitions
* Using `partitionBy()` with Pair RDDs
* Understanding when increasing or decreasing partitions improves performance
* Optimizing a dataset suffering from too few partitions

## Technologies

* Scala 2.12.18
* Apache Spark 3.5.1
* Spark Core
* sbt
* Java 17
* WSL2

## Project Structure

```text
Day10-Partitioning-Practice/
├── build.sbt
├── .gitignore
├── README.md
└── src/
    └── main/
        └── scala/
            └── com/
                └── day10partitioning/
                    └── PartitioningApp.scala
```

## Concepts Demonstrated

### 1. Inspecting Partitions

Spark RDDs can be inspected using:

```scala
rdd.getNumPartitions
```

The project also uses:

```scala
rdd.glom()
```

to inspect how many records are present in each partition.

The initial dataset contains 100 records across 2 partitions:

```text
[50, 50]
```

### 2. repartition()

`repartition()` changes the number of partitions and performs a shuffle.

Example:

```scala
val repartitionedRDD = originalRDD.repartition(8)
```

This increases the dataset from 2 partitions to 8 partitions.

Example distribution:

```text
[12, 12, 14, 14, 12, 12, 12, 12]
```

`repartition()` is useful when a dataset has too few partitions and more parallelism is needed.

### 3. coalesce()

`coalesce()` is primarily used to reduce the number of partitions.

Example:

```scala
val coalescedRDD = repartitionedRDD.coalesce(4)
```

This reduces the dataset from 8 partitions to 4 partitions.

`coalesce()` is generally more efficient than `repartition()` when reducing partitions because it can avoid a full shuffle.

### 4. partitionBy()

`partitionBy()` is used with Pair RDDs to control how records are distributed by key.

Example:

```scala
val partitionedSalesRDD =
  salesRDD.partitionBy(new HashPartitioner(4))
```

This creates 4 partitions using a `HashPartitioner`.

Partitioning by key is useful for operations such as:

* `reduceByKey`
* `groupByKey`
* `join`
* Other key-based transformations

### 5. Optimization Scenario

The project simulates a dataset suffering from too few partitions:

```text
Before optimization:
Partitions: 2
Records per partition: [500, 500]
```

The dataset is then repartitioned:

```text
After optimization:
Partitions: 8
Records per partition: [124, 124, 126, 126, 126, 126, 124, 124]
```

Increasing the number of partitions allows Spark to distribute work across more tasks and can improve parallelism when the original number of partitions is too small.

## repartition() vs coalesce()

| Feature                 | repartition()        | coalesce()                    |
| ----------------------- | -------------------- | ----------------------------- |
| Changes partition count | Yes                  | Yes                           |
| Increasing partitions   | Yes                  | Generally not used            |
| Decreasing partitions   | Yes                  | Yes                           |
| Full shuffle            | Yes                  | Usually no                    |
| Main purpose            | Redistribute data    | Reduce partitions             |
| Typical use             | Increase parallelism | Reduce unnecessary partitions |

## When to Increase Partitions

Increasing partitions can help when:

* The dataset is large.
* There are too few partitions.
* Available CPU resources are not being fully utilized.
* Individual partitions contain too much data.
* More parallel tasks can improve processing performance.

## When to Decrease Partitions

Decreasing partitions can help when:

* The dataset has too many small partitions.
* Task scheduling overhead becomes significant.
* The output needs fewer files.
* The workload does not require high parallelism.

## How to Run

From the project directory:

```bash
sbt run
```

The application prints:

* Original partition count
* Records per partition
* Partition count after `repartition()`
* Partition count after `coalesce()`
* Pair RDD partitioning
* Optimization results
* Partitioning summary

## Sample Output

```text
=== 1. ORIGINAL DATASET ===
Number of records: 100
Number of partitions: 2
Records per partition: [50, 50]

=== 2. AFTER REPARTITION(8) ===
Number of partitions: 8

=== 3. AFTER COALESCE(4) ===
Number of partitions: 4

=== 4. PAIR RDD BEFORE partitionBy ===
Number of partitions: 2

=== 5. PAIR RDD AFTER partitionBy ===
Number of partitions: 4

=== 6. OPTIMIZATION SCENARIO ===
Before optimization:
Partitions: 2

After optimization:
Partitions: 8
```

## Key Takeaways

1. Partitions determine how Spark distributes work across tasks.
2. Too few partitions can limit parallelism.
3. `repartition()` performs a shuffle and is useful for redistributing data.
4. `coalesce()` is generally preferred when reducing the number of partitions.
5. `partitionBy()` controls key-based data distribution in Pair RDDs.
6. The ideal number of partitions depends on data size, workload, and available resources.
7. Partitioning should be chosen carefully because unnecessary shuffles can increase execution time.

## Java 17 Compatibility

This project runs Apache Spark 3.5.1 with Java 17. JVM module access options are configured in `build.sbt` to support Spark's internal reflection and serialization requirements when running locally.
