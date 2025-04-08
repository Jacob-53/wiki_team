from pyspark.sql import SparkSession
import sys

dt = sys.argv[1]

spark = SparkSession.builder.appName("RemoteSparkjob_{dt}").getOrCreate()
df = spark.read.text("gs://jacob-wiki-bucket/wiki/2024-01/dt=20240130/pageviews-20240130-150000.gz")
df.show(3)


spark.stop()

