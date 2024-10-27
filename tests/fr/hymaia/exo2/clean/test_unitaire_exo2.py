import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.fr.hymaia.exo2.clean.data_processing import (
    filter_major_clients,
    join_clients_villes,
    add_departement_column,
)


class TestSparkJobs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("Unit Test for Exo2")
            .master("local[*]")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_major_clients(self):
        # Given
        data = [
            Row(name="John", age=25, zip="75020"),
            Row(name="Jane", age=17, zip="75018"),
            Row(name="Doe", age=30, zip="75015"),
        ]
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("zip", StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(data, schema)

        # When
        result_df = filter_major_clients(df)

        # Then
        expected_data = [
            Row(name="John", age=25, zip="75020"),
            Row(name="Doe", age=30, zip="75015"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, schema)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_join_clients_villes(self):
        # Given
        clients_data = [
            Row(name="John", age=25, zip="75020"),
            Row(name="Doe", age=30, zip="75015"),
        ]
        villes_data = [Row(zip="75020", city="Paris"), Row(zip="75015", city="Lyon")]
        clients_df = self.spark.createDataFrame(clients_data)
        villes_df = self.spark.createDataFrame(villes_data)

        # When
        result_df = join_clients_villes(clients_df, villes_df)

        # Then
        # Réordonner les colonnes pour correspondre au résultat attendu
        result_df = result_df.select("name", "age", "zip", "city").orderBy("zip")
        expected_data = [
            Row(name="John", age=25, zip="75020", city="Paris"),
            Row(name="Doe", age=30, zip="75015", city="Lyon"),
        ]
        expected_df = self.spark.createDataFrame(expected_data).orderBy("zip")
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_add_departement_column(self):
        # Given
        data = [
            Row(name="John", age=25, zip="75020", city="Paris"),
            Row(name="Doe", age=30, zip="20190", city="Ajaccio"),
            Row(name="Jane", age=35, zip="20200", city="Bastia"),
        ]
        df = self.spark.createDataFrame(data)

        # When
        result_df = add_departement_column(df)

        # Then
        expected_data = [
            Row(name="John", age=25, zip="75020", city="Paris", departement="75"),
            Row(name="Doe", age=30, zip="20190", city="Ajaccio", departement="2A"),
            Row(name="Jane", age=35, zip="20200", city="Bastia", departement="2B"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertEqual(result_df.collect(), expected_df.collect())
