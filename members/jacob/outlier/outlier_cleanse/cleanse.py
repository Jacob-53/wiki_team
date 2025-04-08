
import argparse
from datetime import datetime
import re
from urllib.parse import quote_plus

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def clean_title(title):
    title = re.sub(r"\(.*?\)", "", title)
    title = re.sub(r"[^가-힣a-zA-Z0-9\s]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title

def make_link(title, today):
    cleaned = clean_title(title)
    date_kor = datetime.strptime(today, "%Y%m%d").strftime("%Y년 %m월 %d일")
    query = f"{date_kor} {cleaned}"
    return f"https://www.google.com/search?q={quote_plus(query)}&tbm=nws"

def main(today):
    spark = SparkSession.builder.appName(f"cleanse_generate_links_{today}").getOrCreate()

    input_path = f"gs://jacob-wiki-bucket/data/outlier/dt={today}"
    output_path = f"gs://jacob-wiki-bucket/data/title_links/dt={today}"

    df = spark.read.parquet(input_path)

    make_link_udf = udf(lambda title: make_link(title, today), StringType())
    clean_title_udf = udf(clean_title, StringType())

    df_links = df.withColumn("cleaned_title", clean_title_udf(col("title"))) \
                 .withColumn("search_link", make_link_udf(col("title")))

    df_links.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Execution date in YYYYMMDD")
    args = parser.parse_args()
    main(args.date)

