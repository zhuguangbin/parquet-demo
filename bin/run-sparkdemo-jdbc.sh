#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

$SPARK_JAR=`ls "$FWDIR"/lib/parquet-spark-demo*.jar`

LOGDIR="$FWDIR"/logs

# run in local mode
spark-submit --master local --conf spark.dynamicAllocation.enabled=false --class com.mvad.hadoop.parquetdemo.spark.JDBCDemo --name 'SparkSQL:[demo][JDBCDemo]' --queue bi $SPARK_JAR > $LOGDIR/spark-demo-jdbc.log

