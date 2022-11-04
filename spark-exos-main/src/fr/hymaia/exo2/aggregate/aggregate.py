import os
import pyspark.sql.functions as f


def aggregate(df):
    return df.groupby("departement").agg(f.count("name")).sort(f.col("count(name)").desc()).withColumnRenamed("count(name)", "nb_people")
