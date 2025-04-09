from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name, regexp_extract, split, explode, lower, coalesce
from datetime import datetime
import sys

APP_NAME = "TestSaveParquet"
RAW_BASE = "gs://nijin-bucket/wiki/"
SAVE_BASE = "gs://nijin-bucket/data/wiki/parquet/"
DT = sys.argv[1]

spark = SparkSession.builder.appName(f"{APP_NAME}{DT}").getOrCreate()

def extract_prefix_and_partition(date_str: str) -> tuple[str, str]:
    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
    return parsed_date.strftime("%Y-%m"), parsed_date.strftime("%Y%m%d")
def load_pageviews(file_path, date_str):
    return spark.read.option("delimiter", " ").csv(file_path, inferSchema=True) \
        .toDF("domain", "title", "views", "size") \
        .withColumn("dt", lit(date_str)) \
        .withColumn("hour", regexp_extract(input_file_name(), r'pageviews-\d{8}-(\d{2})0000', 1))

prefix, partition = extract_prefix_and_partition(DT)
raw_path = f"{RAW_BASE.rstrip('/')}/{prefix}/dt={partition}"

df = load_pageviews(raw_path, partition)
df.show(5)

df = df.withColumn("domain", lower(coalesce(split(col("domain"), "\\.").getItem(0), lit("na"))))

# title을 '_' 기준으로 나누고 explode → 같은 이름(title)으로 덮어쓰기
df = df.withColumn("title", explode(split(col("title"), "_")))

# 최종 컬럼 선택
df = df.select("domain", "title", "views", "dt", "hour")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("dt", "hour").parquet(SAVE_BASE)

spark.stop()
