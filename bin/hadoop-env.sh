#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

LIBDIR="$FWDIR"/lib

function appendToClasspath(){
  if [ -n "$1" ]; then
    if [ -n "$HADOOP_CLASSPATH" ]; then
      HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$1"
    else
      HADOOP_CLASSPATH="$1"
    fi
  fi
}

for f in "${LIBDIR}"/*.jar; do
  appendToClasspath "$f"
done

export HADOOP_CLASSPATH