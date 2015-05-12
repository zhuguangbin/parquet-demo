#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"


LOGDIR="$FWDIR"/logs
HIVEDIR="$FWDIR"/hive
HQLSCRIPT="$HIVEDIR"/sparksqldemo.hql

spark-sql -f $HQLSCRIPT > $LOGDIR/sparksqldemo.log


