from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, split, lower, coalesce
import sys

BASE_PATH = "gs://jiwon-bucket"
ts_nodash = sys.argv[1]
ds = ts_nodash[:8]
ds_partition = ts_nodash[:4] + "-" + ts_nodash[4:6]
hour = ts_nodash[9:11]

# SparkSession 생성 (마스터 URL 지정 X, spark-submit에서 설정됨)
spark = SparkSession.builder.appName(f"RemoteSparkJob_{ts_nodash}").getOrCreate()

def load_pageviews(file_path):
    return spark.read.option("delimiter", " ").csv(file_path, inferSchema=True) \
            .toDF("domain", "title", "views", "size") \
            .withColumn("dt", lit(ds)) \
            .withColumn("hour", lit(hour))

gz_path = f"{BASE_PATH}/wiki/{ds_partition}/dt={ds}/pageviews-{ds}-{hour}0000.gz"
save_path = f"{BASE_PATH}/data/wiki/parquet/dt={ds}/hour={hour}"

df = load_pageviews(gz_path)
df.createOrReplaceTempView("t_raw")

df_r = spark.sql("""
SELECT
    LOWER(COALESCE(SPLIT(domain, '\\\\.')[0], 'na')) AS domain,
    EXPLODE(SPLIT(title, '_')) AS word,
    views,
    dt,
    hour
FROM t_raw
""")

df_r.write \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .parquet(save_path)

# Spark 세션 종료
spark.stop()
