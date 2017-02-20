package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions.udf

object UDFTest {
  def model(a: Long, b: Long, c: Long) = 3*a + 2*b + c

  def main(args: Array[String]) {
    System.setProperty("useNvl", "false")
    System.setProperty("offHeap", "false")
    System.setProperty("pythonNvl", "false")
    val sparkConf = new SparkConf().setAppName("UDFTest").set("spark.executor.memory", args(1))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val m = udf(model(_:Long,_:Long,_:Long))
    val df = sqlContext.read.parquet(args(0)).cache()
    val a = new scala.collection.mutable.ArrayBuffer[Long]
    for (i <- 0 until 11) {
      val start = System.currentTimeMillis
      df.withColumn("model", m(df("a"), df("b"), df("c"))).selectExpr("sum(model)").show()
      a.append(System.currentTimeMillis - start)
    }
    println("average time: " + a.drop(1).sum / 10.0)
  }
}
