package com.day10partitioning

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitioningApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day10-Partitioning-Practice")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("==============================================")
    println("       DAY 10 - PARTITIONING PRACTICE")
    println("==============================================")

    // ------------------------------------------------
    // 1. Create a dataset with too few partitions
    // ------------------------------------------------

    val originalRDD = sc.parallelize(1 to 100, 2)

    println("\n=== 1. ORIGINAL DATASET ===")
    println(s"Number of records: ${originalRDD.count()}")
    println(s"Number of partitions: ${originalRDD.getNumPartitions}")

    val originalDistribution =
      originalRDD.glom().map(_.size).collect()

    println(
      s"Records per partition: ${originalDistribution.mkString("[", ", ", "]")}"
    )

    // ------------------------------------------------
    // 2. Repartition
    // ------------------------------------------------

    val repartitionedRDD = originalRDD.repartition(8)

    println("\n=== 2. AFTER REPARTITION(8) ===")
    println(s"Number of partitions: ${repartitionedRDD.getNumPartitions}")

    val repartitionedDistribution =
      repartitionedRDD.glom().map(_.size).collect()

    println(
      s"Records per partition: ${repartitionedDistribution.mkString("[", ", ", "]")}"
    )

    // ------------------------------------------------
    // 3. Coalesce
    // ------------------------------------------------

    val coalescedRDD = repartitionedRDD.coalesce(4)

    println("\n=== 3. AFTER COALESCE(4) ===")
    println(s"Number of partitions: ${coalescedRDD.getNumPartitions}")

    val coalescedDistribution =
      coalescedRDD.glom().map(_.size).collect()

    println(
      s"Records per partition: ${coalescedDistribution.mkString("[", ", ", "]")}"
    )

    // ------------------------------------------------
    // 4. Pair RDD and partitionBy
    // ------------------------------------------------

    val salesRDD = sc.parallelize(
      Seq(
        ("Hyderabad", 1000),
        ("Hyderabad", 1500),
        ("Bangalore", 2000),
        ("Bangalore", 2500),
        ("Chennai", 1200),
        ("Chennai", 1800),
        ("Mumbai", 3000),
        ("Mumbai", 2200)
      ),
      2
    )

    println("\n=== 4. PAIR RDD BEFORE partitionBy ===")
    println(s"Number of partitions: ${salesRDD.getNumPartitions}")

    val partitionedSalesRDD =
      salesRDD.partitionBy(new HashPartitioner(4))

    println("\n=== 5. PAIR RDD AFTER partitionBy ===")
    println(s"Number of partitions: ${partitionedSalesRDD.getNumPartitions}")

    val pairDistribution =
      partitionedSalesRDD.glom().map(_.size).collect()

    println(
      s"Records per partition: ${pairDistribution.mkString("[", ", ", "]")}"
    )

    // ------------------------------------------------
    // 5. Optimization scenario
    // ------------------------------------------------

    println("\n=== 6. OPTIMIZATION SCENARIO ===")

    val tooFewPartitions = sc.parallelize(1 to 1000, 2)

    println("Before optimization:")
    println(s"Partitions: ${tooFewPartitions.getNumPartitions}")

    val optimizedRDD = tooFewPartitions.repartition(8)

    println("\nAfter optimization:")
    println(s"Partitions: ${optimizedRDD.getNumPartitions}")

    val beforeOptimization =
      tooFewPartitions.glom().map(_.size).collect()

    val afterOptimization =
      optimizedRDD.glom().map(_.size).collect()

    println(
      s"Before distribution: ${beforeOptimization.mkString("[", ", ", "]")}"
    )

    println(
      s"After distribution: ${afterOptimization.mkString("[", ", ", "]")}"
    )

    println("\n=== 7. PARTITIONING SUMMARY ===")

    println(
      """
repartition():
- Changes the number of partitions.
- Performs a shuffle.
- Useful when increasing parallelism.
- Can also decrease partitions, but may be more expensive.

coalesce():
- Primarily used to decrease partitions.
- Usually avoids a full shuffle.
- More efficient when reducing partitions.

partitionBy():
- Used with Pair RDDs.
- Controls how records are distributed by key.
- Useful for key-based operations such as reduceByKey and join.

Optimization:
- Too few partitions can limit parallelism.
- Increasing partitions can allow more tasks to run concurrently.
- The ideal number depends on data size and available CPU resources.
"""
    )

    sc.stop()
  }
}
