

import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def main(today):
    spark = SparkSession.builder.appName(f"wiki_outlier_{today}").getOrCreate()

    # 날짜 계산
    yesterday = (datetime.strptime(today, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")

    # 데이터 로드
    df_today = spark.read.parquet(f"gs://jacob-wiki-bucket/data/wiki/parquet/dt={today}")
    df_yesterday = spark.read.parquet(f"gs://jacob-wiki-bucket/data/wiki/parquet/dt={yesterday}")

    # 필터 및 집계
    df_today = df_today.filter(col("domain") == "ko") \
        .groupBy("title") \
        .sum("views") \
        .withColumnRenamed("sum(views)", "current_views")

    df_yesterday = df_yesterday.filter(col("domain") == "ko") \
        .groupBy("title") \
        .sum("views") \
        .withColumnRenamed("sum(views)", "previous_views")

    # 이상치 탐지
    df_joined = df_today.join(df_yesterday, on="title", how="left_outer") \
        .fillna(0, subset=["previous_views"]) \
        .withColumn("anomaly_type", when(col("previous_views") == 0, "NEW")
                    .when(col("current_views") >= 2 * col("previous_views"), "SURGE")
                    .otherwise("NORMAL"))

    df_anomalies = df_joined.filter(col("anomaly_type") != "NORMAL") \
        .orderBy(col("current_views").desc()) \
        .limit(50)

    # 저장
    df_anomalies.write.mode("overwrite").parquet(f"gs://jacob-wiki-bucket/data/outlier/dt={today}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Execution date in YYYYMMDD")
    args = parser.parse_args()
    main(args.date)

