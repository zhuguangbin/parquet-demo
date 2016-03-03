#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

SPARK_JAR=`ls "$FWDIR"/lib/parquet-spark-demo*.jar`

LOGDIR="$FWDIR"/logs

spark-submit --class com.mvad.hadoop.parquetdemo.spark.SessionLogParquetDemo --name 'SparkSQL:[demo][SessionLogParquetDemo]' --queue bi $SPARK_JAR > $LOGDIR/spark-demo-sessionlog.log


