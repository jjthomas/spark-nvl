/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util.HashMap

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.vectorized.AggregateHashMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure whole stage codegen performance.
 * To run this:
 *  build/sbt "sql/test-only *BenchmarkWholeStageCodegen"
 */
class BenchmarkWholeStageCodegen extends SparkFunSuite {
  lazy val conf = new SparkConf().setMaster("local[1]").setAppName("benchmark")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.sql.autoBroadcastJoinThreshold", "1")
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sc)

  def runBenchmark(name: String, values: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, values)

    Seq(false, true).foreach { enabled =>
      benchmark.addCase(s"$name codegen=$enabled") { iter =>
        sqlContext.setConf("spark.sql.codegen.wholeStage", enabled.toString)
        f
      }
    }

    benchmark.run()
  }

  // These benchmark are skipped in normal build
  ignore("range/sum") {
    val N = 500L << 20
    runBenchmark("rang/sum", N) {
      sqlContext.range(N).groupBy().sum().collect()
    }
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    rang/filter/sum:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    rang/filter/sum codegen=false          14332 / 16646         36.0          27.8       1.0X
    rang/filter/sum codegen=true              897 / 1022        584.6           1.7      16.4X
    */
  }

  // These benchmark are skipped in normal build
  ignore("select-nvl") {
    val N = 500L << 20
    runBenchmark("select-nvl", N) {
      sqlContext.read.parquet("/Users/joseph/spark/assembly/tpch-sf1").selectExpr("C5 * 2 as foo")
    }
    /*
select-nvl:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
select-nvl codegen=true(nvl)                   199 /  283       2638.9           0.4       1.0X

select-nvl:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
select-nvl codegen=false                  270 /  351       1938.3           0.5       1.0X
select-nvl codegen=true                   170 /  209       3091.0           0.3       1.6X
    */
  }

  test("q6-nvl") {
    val N = 500L << 20
    runBenchmark("q6-nvl", N) {
      sqlContext.read.parquet("/Volumes/RD/tpch-sf1-q6-nodict").filter("shipdate_long >= 19940101 and shipdate_long < 19950101 and C6 >= 0.05 and C6 <= 0.07 and quantity < 24").selectExpr("sum(C5 * C6)").collect()
    }
    /*
(WITH RAMDISK & nvl)
Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

q6-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q6-nvl codegen=false                     1220 / 1368        429.9           2.3       1.0X
q6-nvl codegen=true                       836 /  966        627.0           1.6       1.5X

(WITHOUT RAMDISK & nvl)
q6-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q6-nvl codegen=false                     1069 / 1334        490.3           2.0       1.0X
q6-nvl codegen=true                       823 /  928        636.9           1.6       1.3X

(WITH RAMDISK & Java codegen)
q6-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q6-nvl codegen=false                     2775 / 3179        188.9           5.3       1.0X
q6-nvl codegen=true                      1098 / 1349        477.5           2.1       2.5X

Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

q6-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q6-nvl codegen=false                      192 /  235       2728.8           0.4       1.0X
q6-nvl codegen=true                       137 /  173       3838.7           0.3       1.4X

q6-nvl (no NVL):                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q6-nvl codegen=false                      184 /  242       2853.2           0.4       1.0X
q6-nvl codegen=true                       127 /  135       4131.4           0.2       1.4X
    */
  }

  ignore("q1-nvl") {
    val N = 500L << 20
    val df = sqlContext.read.parquet("/Users/joseph/spark/assembly/tpch-sf1-q1").cache()
    runBenchmark("q1-nvl", N) {
      df.filter("shipdate_long <= 19981111").selectExpr("quantity", "C5", "C6", "C5 * (1 - C6) as a", "C5 * (1 - C6) * (1 + C7) as b", "returnflag", "linestatus").groupBy("returnflag", "linestatus").sum("quantity", "C5", "C6", "a", "b").collect()
    }
    /*
(**** WITH NVL ****)
Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

q6-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q6-nvl codegen=false                       45 /   71      11541.6           0.1       1.0X
q6-nvl codegen=true                        42 /   50      12625.5           0.1       1.1X

[info] - q6-nvl (8 seconds, 797 milliseconds)
org.apache.spark.sql.execution.WholeStageCodegen$NvlGeneratorException: don't use NVL for scan only
null
Running benchmark: q1-nvl
  Running case: q1-nvl codegen=false
  Running case: q1-nvl codegen=true

Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

q1-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q1-nvl codegen=false                       99 /  123       5279.9           0.2       1.0X
q1-nvl codegen=true                        56 /   77       9379.9           0.1       1.8X

(**** WITHOUT NVL ****)
Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

q6-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q6-nvl codegen=false                       54 /   74       9661.1           0.1       1.0X
q6-nvl codegen=true                        36 /   46      14611.0           0.1       1.5X

[info] - q6-nvl (9 seconds, 71 milliseconds)
org.apache.spark.sql.execution.WholeStageCodegen$NvlGeneratorException: don't use NVL for scan only
null
Running benchmark: q1-nvl
  Running case: q1-nvl codegen=false
  Running case: q1-nvl codegen=true

Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

q1-nvl:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
q1-nvl codegen=false                       86 /  126       6107.2           0.2       1.0X
q1-nvl codegen=true                        57 /   85       9130.0           0.1       1.5X

    */
  }


  // These benchmark are skipped in normal build
  ignore("range/filter/sum") {
    val N = 500L << 20
    runBenchmark("rang/filter/sum", N) {
      sqlContext.range(N).filter("(id & 1) = 1").groupBy().sum().collect()
    }
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    rang/filter/sum:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    rang/filter/sum codegen=false          14332 / 16646         36.0          27.8       1.0X
    rang/filter/sum codegen=true              897 / 1022        584.6           1.7      16.4X
    */
  }

  ignore("range/limit/sum") {
    val N = 500L << 20
    runBenchmark("range/limit/sum", N) {
      sqlContext.range(N).limit(1000000).groupBy().sum().collect()
    }
    /*
    Westmere E56xx/L56xx/X56xx (Nehalem-C)
    range/limit/sum:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    range/limit/sum codegen=false             609 /  672        861.6           1.2       1.0X
    range/limit/sum codegen=true              561 /  621        935.3           1.1       1.1X
    */
  }

  ignore("range/sample/sum") {
    val N = 500 << 20
    runBenchmark("range/sample/sum", N) {
      sqlContext.range(N).sample(true, 0.01).groupBy().sum().collect()
    }
    /*
    Westmere E56xx/L56xx/X56xx (Nehalem-C)
    range/sample/sum:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    range/sample/sum codegen=false         53888 / 56592          9.7         102.8       1.0X
    range/sample/sum codegen=true          41614 / 42607         12.6          79.4       1.3X
    */

    runBenchmark("range/sample/sum", N) {
      sqlContext.range(N).sample(false, 0.01).groupBy().sum().collect()
    }
    /*
    Westmere E56xx/L56xx/X56xx (Nehalem-C)
    range/sample/sum:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    range/sample/sum codegen=false         12982 / 13384         40.4          24.8       1.0X
    range/sample/sum codegen=true            7074 / 7383         74.1          13.5       1.8X
    */
  }

  ignore("stat functions") {
    val N = 100L << 20

    runBenchmark("stddev", N) {
      sqlContext.range(N).groupBy().agg("id" -> "stddev").collect()
    }

    runBenchmark("kurtosis", N) {
      sqlContext.range(N).groupBy().agg("id" -> "kurtosis").collect()
    }


    /**
      Using ImperativeAggregate (as implemented in Spark 1.6):

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      stddev:                            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      stddev w/o codegen                      2019.04            10.39         1.00 X
      stddev w codegen                        2097.29            10.00         0.96 X
      kurtosis w/o codegen                    2108.99             9.94         0.96 X
      kurtosis w codegen                      2090.69            10.03         0.97 X

      Using DeclarativeAggregate:

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      stddev:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      stddev codegen=false                     5630 / 5776         18.0          55.6       1.0X
      stddev codegen=true                      1259 / 1314         83.0          12.0       4.5X

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      kurtosis:                           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      kurtosis codegen=false                 14847 / 15084          7.0         142.9       1.0X
      kurtosis codegen=true                    1652 / 2124         63.0          15.9       9.0X
      */
  }

  ignore("aggregate with keys") {
    val N = 20 << 20

    runBenchmark("Aggregate w keys", N) {
      sqlContext.range(N).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()
    }

    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      Aggregate w keys:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      Aggregate w keys codegen=false           2429 / 2644          8.6         115.8       1.0X
      Aggregate w keys codegen=true            1535 / 1571         13.7          73.2       1.6X
    */
  }

  ignore("broadcast hash join") {
    val N = 20 << 20
    val M = 1 << 16
    val dim = broadcast(sqlContext.range(M).selectExpr("id as k", "cast(id as string) as v"))

    runBenchmark("Join w long", N) {
      sqlContext.range(N).join(dim, (col("id") % M) === col("k")).count()
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w long:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w long codegen=false                5351 / 5531          3.9         255.1       1.0X
    Join w long codegen=true                  275 /  352         76.2          13.1      19.4X
    */

    runBenchmark("Join w long duplicated", N) {
      val dim = broadcast(sqlContext.range(M).selectExpr("cast(id/10 as long) as k"))
      sqlContext.range(N).join(dim, (col("id") % M) === col("k")).count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w long duplicated:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w long duplicated codegen=false      4752 / 4906          4.4         226.6       1.0X
    Join w long duplicated codegen=true       722 /  760         29.0          34.4       6.6X
    */

    val dim2 = broadcast(sqlContext.range(M)
      .selectExpr("cast(id as int) as k1", "cast(id as int) as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 ints", N) {
      sqlContext.range(N).join(dim2,
        (col("id") % M).cast(IntegerType) === col("k1")
          && (col("id") % M).cast(IntegerType) === col("k2")).count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 ints:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 ints codegen=false              9011 / 9121          2.3         429.7       1.0X
    Join w 2 ints codegen=true               2565 / 2816          8.2         122.3       3.5X
    */

    val dim3 = broadcast(sqlContext.range(M)
      .selectExpr("id as k1", "id as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 longs", N) {
      sqlContext.range(N).join(dim3,
        (col("id") % M) === col("k1") && (col("id") % M) === col("k2"))
        .count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 longs:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 longs codegen=false             5905 / 6123          3.6         281.6       1.0X
    Join w 2 longs codegen=true              2230 / 2529          9.4         106.3       2.6X
      */

    val dim4 = broadcast(sqlContext.range(M)
      .selectExpr("cast(id/10 as long) as k1", "cast(id/10 as long) as k2"))

    runBenchmark("Join w 2 longs duplicated", N) {
      sqlContext.range(N).join(dim4,
        (col("id") bitwiseAND M) === col("k1") && (col("id") bitwiseAND M) === col("k2"))
        .count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 longs duplicated:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 longs duplicated codegen=false      6420 / 6587          3.3         306.1       1.0X
    Join w 2 longs duplicated codegen=true      2080 / 2139         10.1          99.2       3.1X
     */

    runBenchmark("outer join w long", N) {
      sqlContext.range(N).join(dim, (col("id") % M) === col("k"), "left").count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    outer join w long:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    outer join w long codegen=false          5667 / 5780          3.7         270.2       1.0X
    outer join w long codegen=true            216 /  226         97.2          10.3      26.3X
      */

    runBenchmark("semi join w long", N) {
      sqlContext.range(N).join(dim, (col("id") % M) === col("k"), "leftsemi").count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    semi join w long:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    semi join w long codegen=false           4690 / 4953          4.5         223.7       1.0X
    semi join w long codegen=true             211 /  229         99.2          10.1      22.2X
     */
  }

  ignore("sort merge join") {
    val N = 2 << 20
    runBenchmark("merge join", N) {
      val df1 = sqlContext.range(N).selectExpr(s"id * 2 as k1")
      val df2 = sqlContext.range(N).selectExpr(s"id * 3 as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    merge join:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    merge join codegen=false                 1588 / 1880          1.3         757.1       1.0X
    merge join codegen=true                  1477 / 1531          1.4         704.2       1.1X
      */

    runBenchmark("sort merge join", N) {
      val df1 = sqlContext.range(N)
        .selectExpr(s"(id * 15485863) % ${N*10} as k1")
      val df2 = sqlContext.range(N)
        .selectExpr(s"(id * 15485867) % ${N*10} as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    sort merge join:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    sort merge join codegen=false            3626 / 3667          0.6        1728.9       1.0X
    sort merge join codegen=true             3405 / 3438          0.6        1623.8       1.1X
      */
  }

  ignore("shuffle hash join") {
    val N = 4 << 20
    sqlContext.setConf("spark.sql.shuffle.partitions", "2")
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "10000000")
    sqlContext.setConf("spark.sql.join.preferSortMergeJoin", "false")
    runBenchmark("shuffle hash join", N) {
      val df1 = sqlContext.range(N).selectExpr(s"id as k1")
      val df2 = sqlContext.range(N / 5).selectExpr(s"id * 3 as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    shuffle hash join:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    shuffle hash join codegen=false          1538 / 1742          2.7         366.7       1.0X
    shuffle hash join codegen=true            892 / 1329          4.7         212.6       1.7X
     */
  }

  ignore("cube") {
    val N = 5 << 20

    runBenchmark("cube", N) {
      sqlContext.range(N).selectExpr("id", "id % 1000 as k1", "id & 256 as k2")
        .cube("k1", "k2").sum("id").collect()
    }

    /**
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      cube:                               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      cube codegen=false                       3188 / 3392          1.6         608.2       1.0X
      cube codegen=true                        1239 / 1394          4.2         236.3       2.6X
      */
  }

  ignore("hash and BytesToBytesMap") {
    val N = 10 << 20

    val benchmark = new Benchmark("BytesToBytesMap", N)

    benchmark.addCase("hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var s = 0
      while (i < N) {
        key.setInt(0, i % 1000)
        val h = Murmur3_x86_32.hashUnsafeWords(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes, 42)
        s += h
        i += 1
      }
    }

    benchmark.addCase("fast hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var s = 0
      while (i < N) {
        key.setInt(0, i % 1000)
        val h = Murmur3_x86_32.hashLong(i % 1000, 42)
        s += h
        i += 1
      }
    }

    benchmark.addCase("arrayEqual") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val valueBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      var s = 0
      while (i < N) {
        key.setInt(0, i % 1000)
        if (key.equals(value)) {
          s += 1
        }
        i += 1
      }
    }

    benchmark.addCase("Java HashMap (Long)") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val valueBytes = new Array[Byte](16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      val map = new HashMap[Long, UnsafeRow]()
      while (i < 65536) {
        value.setInt(0, i)
        map.put(i.toLong, value)
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        if (map.get(i % 100000) != null) {
          s += 1
        }
        i += 1
      }
    }

    benchmark.addCase("Java HashMap (two ints) ") { iter =>
      var i = 0
      val valueBytes = new Array[Byte](16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      val map = new HashMap[Long, UnsafeRow]()
      while (i < 65536) {
        value.setInt(0, i)
        val key = (i.toLong << 32) + Integer.rotateRight(i, 15)
        map.put(key, value)
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        val key = ((i & 100000).toLong << 32) + Integer.rotateRight(i & 100000, 15)
        if (map.get(key) != null) {
          s += 1
        }
        i += 1
      }
    }

    benchmark.addCase("Java HashMap (UnsafeRow)") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val valueBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      val map = new HashMap[UnsafeRow, UnsafeRow]()
      while (i < 65536) {
        key.setInt(0, i)
        value.setInt(0, i)
        map.put(key, value.copy())
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        key.setInt(0, i % 100000)
        if (map.get(key) != null) {
          s += 1
        }
        i += 1
      }
    }

    Seq("off", "on").foreach { heap =>
      benchmark.addCase(s"BytesToBytesMap ($heap Heap)") { iter =>
        val taskMemoryManager = new TaskMemoryManager(
          new StaticMemoryManager(
            new SparkConf().set("spark.memory.offHeap.enabled", s"${heap == "off"}")
              .set("spark.memory.offHeap.size", "102400000"),
            Long.MaxValue,
            Long.MaxValue,
            1),
          0)
        val map = new BytesToBytesMap(taskMemoryManager, 1024, 64L<<20)
        val keyBytes = new Array[Byte](16)
        val valueBytes = new Array[Byte](16)
        val key = new UnsafeRow(1)
        key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
        val value = new UnsafeRow(1)
        value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
        var i = 0
        while (i < N) {
          key.setInt(0, i % 65536)
          val loc = map.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
            Murmur3_x86_32.hashLong(i % 65536, 42))
          if (loc.isDefined) {
            value.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
            value.setInt(0, value.getInt(0) + 1)
            i += 1
          } else {
            loc.append(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
              value.getBaseObject, value.getBaseOffset, value.getSizeInBytes)
          }
        }
      }
    }

    benchmark.addCase("Aggregate HashMap") { iter =>
      var i = 0
      val numKeys = 65536
      val schema = new StructType()
        .add("key", LongType)
        .add("value", LongType)
      val map = new AggregateHashMap(schema)
      while (i < numKeys) {
        val row = map.findOrInsert(i.toLong)
        row.setLong(1, row.getLong(1) +  1)
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        if (map.find(i % 100000) != -1) {
          s += 1
        }
        i += 1
      }
    }

    /**
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    BytesToBytesMap:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    hash                                      112 /  116         93.2          10.7       1.0X
    fast hash                                  65 /   69        160.9           6.2       1.7X
    arrayEqual                                 66 /   69        159.1           6.3       1.7X
    Java HashMap (Long)                       137 /  182         76.3          13.1       0.8X
    Java HashMap (two ints)                   182 /  230         57.8          17.3       0.6X
    Java HashMap (UnsafeRow)                  511 /  565         20.5          48.8       0.2X
    BytesToBytesMap (off Heap)                481 /  515         21.8          45.9       0.2X
    BytesToBytesMap (on Heap)                 529 /  600         19.8          50.5       0.2X
    Aggregate HashMap                          56 /   62        187.9           5.3       2.0X
      */
    benchmark.run()
  }

  ignore("collect") {
    val N = 1 << 20

    val benchmark = new Benchmark("collect", N)
    benchmark.addCase("collect 1 million") { iter =>
      sqlContext.range(N).collect()
    }
    benchmark.addCase("collect 2 millions") { iter =>
      sqlContext.range(N * 2).collect()
    }
    benchmark.addCase("collect 4 millions") { iter =>
      sqlContext.range(N * 4).collect()
    }
    benchmark.run()

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    collect:                            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    collect 1 million                         439 /  654          2.4         418.7       1.0X
    collect 2 millions                        961 / 1907          1.1         916.4       0.5X
    collect 4 millions                       3193 / 3895          0.3        3044.7       0.1X
     */
  }

  ignore("collect limit") {
    val N = 1 << 20

    val benchmark = new Benchmark("collect limit", N)
    benchmark.addCase("collect limit 1 million") { iter =>
      sqlContext.range(N * 4).limit(N).collect()
    }
    benchmark.addCase("collect limit 2 millions") { iter =>
      sqlContext.range(N * 4).limit(N * 2).collect()
    }
    benchmark.run()

    /**
    model name      : Westmere E56xx/L56xx/X56xx (Nehalem-C)
    collect limit:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    collect limit 1 million                   833 / 1284          1.3         794.4       1.0X
    collect limit 2 millions                 3348 / 4005          0.3        3193.3       0.2X
     */
  }
}
