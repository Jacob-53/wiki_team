from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, split, lower, coalesce

spark = SparkSession.builder.appName("GCS Wiki Snappy Export").getOrCreate()

def load_pageviews(file_path, date_str):
    return spark.read.option("delimiter", " ").csv(file_path, inferSchema=True) \
        .toDF("domain", "title", "views", "size") \
        .withColumn("dt", lit(date_str))

df = load_pageviews("gs://jacob-wiki-bucket/wiki/2024-01/dt=20240101/pageviews-20240101-*.gz", "2024-01-01")
df.createOrReplaceTempView("t_raw")

df_r = spark.sql("""
SELECT
    LOWER(COALESCE(SPLIT(domain, '\\\\.')[0], 'na')) AS domain,
    EXPLODE(SPLIT(title, '_')) AS word,
    views,
    dt 
FROM t_raw
""")
df_r.createOrReplaceTempView("t_split")

df_r.write \
    .option("compression", "snappy") \
    .mode("append") \
    .partitionBy("dt") \
    .parquet("gs://jacob-wiki-bucket/wiki/test/parquet/")

spark.stop()

