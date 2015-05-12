#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

. "$FWDIR"/bin/hadoop-env.sh
set -e

function print_usage(){
  echo "Usage: run-mapreducedemo-sessionlog.sh <in> <out>"
}

if [ $# != 2 ]; then
  print_usage
  exit
fi

HADOOP_JAR="$FWDIR"/lib/parquet-mapreduce-demo_2.10-0.0.1-SNAPSHOT.jar

LOGDIR="$FWDIR"/logs

hadoop jar $HADOOP_JAR com.mvad.hadoop.parquetdemo.mapreduce.CookieEventCountByPublisher



