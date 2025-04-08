#!/bin/bash
DS=$1
PY_PATH=$2

echo "DS=====> $DS"

/home/jacob8753/app/spark-3.5.1-bin-hadoop3/bin/spark-submit --master spark://10.178.0.2:7077 --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/home/jacob8753/keys/abiding-ascent-455400-u6-c8e90511db0d.json --executor-memory 6G --executor-cores 4 $PY_PATH --date $DS
