from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.main import keep_adults, join_city_clients, add_zipcode
from src.fr.hymaia.exo2.clean.clean import clean
from src.fr.hymaia.exo2.aggregate.aggregate import aggregate
from pyspark.sql import Row


class TestKeepAdults(unittest.TestCase):
    def test_keep_adults(self):
        # Given
        input = spark.createDataFrame([
            Row(name='Thomas', age=21),
            Row(name='Enzo', age=80),
            Row(name='Raph', age=1)
        ])

        # When
        actual = keep_adults(input)

        # Then
        expected = spark.createDataFrame([
            Row(name='Thomas', age=21),
            Row(name='Enzo', age=80)
        ])

        self.assertListEqual(actual.collect(), expected.collect())


class TestBadKeepAdults(unittest.TestCase):
    def test_bad_keep_adults(self):
        # Given
        input = spark.createDataFrame([
            Row(name='Turing', age=21),
            Row(name='Hopper', age=42),
            Row(name='Batman', age=17),
            Row(name='Brutus Junior', age=18.5)
        ])

        # When
        actual = keep_adults(input)

        # Then
        expected = spark.createDataFrame([
            Row(name='Turing', age=21),
            Row(name='Hopper', age=42)
        ])

        self.assertListEqual(actual.collect(), expected.collect())


class TestJoinCityAndClients(unittest.TestCase):
    def test_join_city_clients(self):
        clients = spark.createDataFrame([
            Row(name='Thomas', age=21, zip="94100"),
            Row(name='Enzo', age=80, zip="13001"),
            Row(name='Raph', age=1, zip="94100"),
            Row(name='Juan', age=42, zip="75001")
        ])

        city = spark.createDataFrame([
            Row(zip='94100', city="Saint-Maur"),
            Row(zip='91540', city="Marseille"),
            Row(zip='75001', city="PARIS")
        ])

        actual = join_city_clients(clients, city, "zip")

        expected = spark.createDataFrame([
            Row(zip="94100", name='Thomas', age=21, city="Saint-Maur"),
            Row(zip="91540", name='Enzo', age=80, city="Marseille"),
            Row(zip="91540", name='Raph', age=1, city="Saint-Maur"),
            Row(zip="75001", name='Juan', age=42, city="PARIS")
        ])

        self.assertCountEqual(actual.collect(), expected.collect())


class TestAddZipcode(unittest.TestCase):
    def test_add_zipcode(self):
        clients = spark.createDataFrame([
            Row(zip="10001", name='Thomas', age=21, city="Saint-Maur"),
            Row(zip="91540", name='Enzo', age=80, city="Marseille"),
            Row(zip="91540", name='Raph', age=1, city="Saint-Maur"),
            Row(zip="75001", name='Juan', age=42, city="PARIS"),
            Row(zip="20001", name='Sananes', age=9, city="AJACCIO")
        ])

        actual = add_zipcode(clients)

        expected = spark.createDataFrame([
            Row(zip="10001", name='Thomas', age=21, city="Saint-Maur", departement = "94"),
            Row(zip="91540", name='Enzo', age=80, city="Marseille", departement = "13"),
            Row(zip="91540", name='Raph', age=1, city="Saint-Maur", departement = "94"),
            Row(zip="75001", name='Juan', age=42, city="PARIS", departement = "75"),
            Row(zip="20001", name='Sananes', age=9, city="AJACCIO", departement = "2A")
        ])

        self.assertCountEqual(actual.collect(), expected.collect())


class TestJobs(unittest.TestCase):
    def test_job1(self):
        clients = spark.createDataFrame([
            Row(name='Thomas', age=21, zip="94100"),
            Row(name='Enzo', age=80, zip="13001"),
            Row(name='Raph', age=1, zip="94100"),
            Row(name='Juan', age=42, zip="75001")
        ])

        city = spark.createDataFrame([
            Row(zip='94100', city="Saint-Maur"),
            Row(zip='91540', city="Marseille"),
            Row(zip='75001', city="PARIS")
        ])

        actual = clean(clients, city)

        expected = spark.createDataFrame([
            Row(zip="10001", name='Thomas', age=21, city="Saint-Maur", departement = "94"),
            Row(zip="91540", name='Enzo', age=80, city="Marseille", departement = "13"),
            Row(zip="75001", name='Juan', age=42, city="PARIS", departement = "75")
        ])

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_jobs2(self):
        clean_df = spark.createDataFrame([
            Row(zip="10001", name='Thomas', age=21, city="Saint-Maur", departement = "94"),
            Row(zip="91540", name='Enzo', age=80, city="Marseille", departement = "13"),
            Row(zip="91540", name='Raph', age=1, city="Saint-Maur", departement = "94"),
            Row(zip="75001", name='Juan', age=42, city="PARIS", departement = "75"),
            Row(zip="20001", name='Sananes', age=9, city="AJACCIO", departement = "2A")
        ])

        actual = aggregate(clean_df)

        expected = spark.createDataFrame([
            Row(departement="94", nb_people=2),
            Row(departement="13", nb_people=1),
            Row(departement="75", nb_people=1),
            Row(departement="2A", nb_people=1)
        ])

        self.assertCountEqual(actual.collect(), expected.collect())
