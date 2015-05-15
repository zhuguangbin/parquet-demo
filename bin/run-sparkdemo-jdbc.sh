#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

SPARK_JAR="$FWDIR"/lib/parquet-spark-demo_2.10-0.0.1-SNAPSHOT.jar

LOGDIR="$FWDIR"/logs

# run in local mode
spark-submit --master local --conf spark.dynamicAllocation.enabled=false --class com.mvad.hadoop.parquetdemo.spark.JDBCDemo --name 'SparkSQL:[demo][JDBCDemo]' --queue bi $SPARK_JAR > $LOGDIR/spark-demo-jdbc.log

