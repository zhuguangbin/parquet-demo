#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

$SPARK_JAR=`ls "$FWDIR"/lib/parquet-spark-demo*.jar`

LOGDIR="$FWDIR"/logs

spark-submit --class com.mvad.hadoop.parquetdemo.spark.HiveDemo --name 'SparkSQL:[demo][HiveDemo]' --queue bi $SPARK_JAR > $LOGDIR/spark-demo-hive.log


