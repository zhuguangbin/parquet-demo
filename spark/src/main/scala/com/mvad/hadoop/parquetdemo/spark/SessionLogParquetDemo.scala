package com.mvad.hadoop.parquetdemo.spark

/**
 * Created by zhugb on 15-5-4.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

object SessionLogParquetDemo {

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("SparkSQL:[demo][SparkSQLUsingSessionLogParquet]"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    // Create a DataFrame from Parquet files
    val df = sqlContext.read.format("parquet").load("/mvad/warehouse/session/dspan/date=2016-03-01/")
    // Your can alsoCreate a DataFrame from data sources
    //    val df = sqlContext.load("/mvad/warehouse/session/dspan/date=2015-05-01/","parquet")
    df.registerTempTable("sessionlog")
    sqlContext.tableNames.foreach(println)
    df.printSchema()

    val sql =
      """
        |select eventType, count(cookie) as count from sessionlog group by eventType
      """.stripMargin
    val result = sqlContext.sql(sql)
    result.cache()

    // only show 20 records
    result.show()
    result.show(100)

    // collect to driver for post processing
    result.collect().map(_.mkString(",")).foreach(println)

    //save result
    result.write.format("parquet").save("/tmp/result-parquet")
    result.rdd.saveAsTextFile("/tmp/result-txt")
    result.rdd.saveAsObjectFile("/tmp/result-sequence")

    // bug api, only create to default DB
    //    result.saveAsTable("result")
    //    result.insertInto("result")

    sc.stop()
  }
}
