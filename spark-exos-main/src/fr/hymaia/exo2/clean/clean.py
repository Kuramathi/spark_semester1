import pyspark.sql.functions as f
from src.fr.hymaia.exo2.main import keep_adults, join_city_clients, add_zipcode


def clean(df_clients, df_city):

    df_clients = keep_adults(df_clients)
    df = join_city_clients(df_clients, df_city, "zip")
    return add_zipcode(df)
