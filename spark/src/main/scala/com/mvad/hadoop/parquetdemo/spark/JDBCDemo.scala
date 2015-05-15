package com.mvad.hadoop.parquetdemo.spark

/**
 * Created by zhugb on 15-5-15.
 */

import org.apache.spark.{SparkConf, SparkContext}

object JDBCDemo {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("SparkSQL:[demo][SparkSQLUsingMySQL]"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val jdbcDF = sqlContext.load("jdbc",
      Map("url" -> "jdbc:mysql://st1dg:3306/hadoop","dbtable" -> "hadoop.sparksql_history",
        "user" -> "hadoop", "password" -> "RNymee2527#"))
    jdbcDF.registerTempTable("sparksql_history")
    sqlContext.tableNames.foreach(println)
    jdbcDF.printSchema()
    jdbcDF.show(10)

    val sql = "select count(id) as runcount, retcode from sparksql_history group by retcode"
    val result = sqlContext.sql(sql)
    result.show()
    result.rdd.saveAsTextFile("/tmp/result-jdbc")

    // bug api, only create to default DB
    result.saveAsTable("result-jdbc")

    sc.stop()
  }

}
