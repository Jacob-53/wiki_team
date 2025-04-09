#!/bin/bash

SPARK_HOME=/home/juper7619/app/spark-3.5.1-bin-hadoop3
TS=$1
PY_PATH=$2

$SPARK_HOME/bin/spark-submit \
--master spark://jiwon-spark.asia-northeast3-c.c.beaming-team-455602-e4.internal:7077 \
--executor-memory 4G \
--executor-cores 2 \
$PY_PATH $TS
~                      
