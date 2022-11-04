import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("ZipCode") \
        .master("local[*]") \
        .getOrCreate()

    df_city = spark.read.csv("src/resources/exo2/city_zipcode.csv",
                             header='true',
                             sep=",")

    df_clients = spark.read.csv("src/resources/exo2/clients_bdd.csv",
                                header='true',
                                sep=",")

    df_clients = keep_adults(df_clients)

    df = join_city_clients(df_clients, df_city, "zip")

    df.show()

    df.write.mode("overwrite").parquet("data/exo2/output")

def keep_adults(df):
    return df.filter(f.col("age") >= 18)

def join_city_clients(df1, df2, col_name):
    return df1.join(df2, col_name)


def main2():
    spark = SparkSession.builder \
        .appName("ZipCode") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.parquet("data/exo2/output")

    new_df = add_zipcode(df)
    new_df.filter(df.name == "Goodwin").show()


def add_zipcode(df):
    return df.withColumn("departement",
                         f.when((df.zip > 20000) & (df.zip <= 20190), "2A").when((df.zip > 20190) & (df.zip <= 20999),"2B")
                         .otherwise(f.col("zip").substr(0, 2)))


if __name__ == '__main__':
    main2()
