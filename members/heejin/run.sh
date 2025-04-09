#!/bin/bash

SPARK_HOME=/home/gmlwls5168/app/spark-3.5.1-bin-hadoop3
DS=$1
PY_PATH=/home/gmlwls5168/code/test/test_parquet.py


echo "DS_NODASH ==> $DS"

$SPARK_HOME/bin/spark-submit \
 --master spark://spark-nijin-1.asia-northeast3-c.c.praxis-zoo-455400-d2.internal:7077 \
 --executor-memory 2G \
 --executor-cores 2 \
 $PY_PATH $DS
