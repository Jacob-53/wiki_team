from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, split, lower, coalesce
import sys
from datetime import datetime, timedelta

BASE_PATH = "gs://jiwon-bucket"
ts_nodash = sys.argv[1]
ds = ts_nodash[:8]

# SparkSession 생성 (마스터 URL 지정 X, spark-submit에서 설정됨)
spark = SparkSession.builder.appName(f"RemoteSparkJob_{ts_nodash}").getOrCreate()

save_path = f"gs://jiwon-bucket/data/wiki/traffic/dt={ds}"

def get_previous_date(ds):
    date = datetime.strptime(ds, "%Y%m%d")
    previous_date = date - timedelta(days=1)
    return previous_date.strftime("%Y%m%d")

def load_parquet(ds):
    path = f"gs://jiwon-bucket/data/wiki/parquet/dt={ds}"
    df = spark.read.parquet(path)
    df_filtered = df.filter(col("domain") == "ko") \
        .groupBy("title") \
        .sum("views") \
        .withColumnRenamed("sum(views)", "views")
    return df_filtered


previous_date = get_previous_date(ds)
df_yesterday = load_parquet(previous_date)
df_yesterday.createOrReplaceTempView("data_yesterday")


df_today = load_parquet(ds)
df_today.createOrReplaceTempView("data_today")

fdf = spark.sql("""
SELECT
    t.title,
    y.views AS previous_views,
    t.views AS current_views
FROM
    data_today t
LEFT JOIN
    data_yesterday y
ON
    t.title = y.title
WHERE
    y.title IS NULL
OR
    t.views >= 2 * y.views
ORDER BY
    t.views DESC
LIMIT 50;
""")

fdf.write \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .parquet(save_path)

# Spark 세션 종료
spark.stop()
