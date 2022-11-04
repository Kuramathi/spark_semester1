import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header","true").csv("src/resources/exo1/data.csv")
    df = wordcount(df, "text")
    df.show()
    df.write.partitionBy("count").mode("overwrite").parquet("data/exo1/output")


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
