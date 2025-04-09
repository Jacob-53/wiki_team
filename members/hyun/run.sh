DS=$1
PY_PATH=$2
SPARK_HOME=/home/wsl/app/spark-3.5.1-bin-hadoop3

echo "DS_NODASH ==> $DS"
echo "PY_PATH   ==> $PY_PATH"

$SPARK_HOME/bin/spark-submit \
  --master spark://spark-1.asia-northeast3-c.c.cool-artwork-455400-v4.internal:7077 \
  --executor-memory 6G \
  --executor-cores 4 \
  "$PY_PATH" "$DS"
