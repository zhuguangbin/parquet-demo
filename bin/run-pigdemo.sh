#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

. "$FWDIR"/bin/hadoop-env.sh
set -e

function print_usage(){
  echo "Usage: run-pig-demo.sh <YYYY-MM-dd>"
}

if [ $# != 1 ]; then
  print_usage
  exit
fi

DATE=$1
LOGDIR="$FWDIR"/logs
PIGDIR="$FWDIR"/pig
PIGSCRIPT=`ls "$PIGDIR"/parquet-demo.pig`

source /opt/mv-bash/helper-func.sh

# wait dspan-parquet log, check if produced at present
hdfs_wait /mvad/warehouse/session/dspan/date=$DATE/_SUCCESS 300 60
if [ $? -ne 0 ]; then
  echo -e "`date +"%Y-%m-%d %T"`\tERROR\twait dspan parquet log timeout, exit."
  exit 1
fi

# run pig job
pig -param date=$DATE $PIGSCRIPT > $LOGDIR/pig-demo.log

if [ $? -ne 0 ]; then
    echo -e "`date +"%Y-%m-%d %T"`\tERROR\tjob failed, exit."
    exit 1
else
   echo -e "`date +"%Y-%m-%d %T"`\tINFO\tjob success."
fi


