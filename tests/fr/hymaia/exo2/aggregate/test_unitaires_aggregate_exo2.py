import tempfile
import unittest
from pyspark.sql import SparkSession, Row
from src.fr.hymaia.exo2.aggregate.data_processing import (
    read_clean_data,
    calculate_population_by_departement,
    write_csv,
)
import os


class TestAggregateJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("TestAggregateJob")
            .master("local[*]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_calculate_population_by_departement_nominal(self):
        # Given
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris", departement="75"),
            Row(name="Bob", age=35, zip="75015", city="Paris", departement="75"),
            Row(name="Charlie", age=40, zip="20200", city="Bastia", departement="2B"),
        ]
        df = self.spark.createDataFrame(data)

        # When
        result_df = calculate_population_by_departement(df)

        # Then
        # Créer les données attendues avec le bon ordre
        expected_data = [
            Row(departement="75", nb_people=2),
            Row(departement="2B", nb_people=1),
        ]
        expected_df = self.spark.createDataFrame(expected_data).orderBy(
            "nb_people", "departement", ascending=[False, True]
        )

        # Appliquer le même ordre à result_df pour s'assurer qu'ils sont comparés correctement
        result_df = result_df.orderBy(
            "nb_people", "departement", ascending=[False, True]
        )

        # Comparaison des DataFrames après le tri
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_population_by_departement_single_departement(self):
        # Given
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris", departement="75"),
            Row(name="Bob", age=35, zip="75015", city="Paris", departement="75"),
        ]
        df = self.spark.createDataFrame(data)

        # When
        result_df = calculate_population_by_departement(df)

        # Then
        expected_data = [Row(departement="75", nb_people=2)]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_population_by_departement_missing_column(self):
        # Given
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris"),
            Row(name="Bob", age=35, zip="75015", city="Paris"),
        ]
        df = self.spark.createDataFrame(data)

        # When/Then
        with self.assertRaises(Exception):
            calculate_population_by_departement(df)

    def test_calculate_population_by_departement_empty_dataframe(self):
        # Given
        df = self.spark.createDataFrame(
            [],
            schema="name STRING, age INT, zip STRING, city STRING, departement STRING",
        )

        # When
        result_df = calculate_population_by_departement(df)

        # Then
        self.assertEqual(result_df.count(), 0)

    def test_read_clean_data_nominal(self):
        # Given
        temp_dir = tempfile.mkdtemp()
        clean_data_path = os.path.join(temp_dir, "clean_data.parquet")
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris", departement="75"),
            Row(name="Bob", age=35, zip="75015", city="Paris", departement="75"),
        ]
        self.spark.createDataFrame(data).write.parquet(clean_data_path)

        # When
        df = read_clean_data(self.spark, clean_data_path)

        # Then
        self.assertEqual(df.count(), 2)

    def test_read_clean_data_missing_file(self):
        # Given
        clean_data_path = "/path/to/non_existent_file.parquet"

        # When/Then
        with self.assertRaises(Exception):
            read_clean_data(self.spark, clean_data_path)

    def test_write_csv_nominal(self):
        # Given
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "output")
        data = [Row(departement="75", nb_people=2), Row(departement="2B", nb_people=1)]
        df = self.spark.createDataFrame(data)

        # When
        write_csv(df, output_path)

        # Then
        written_files = os.listdir(output_path)
        self.assertTrue(any(file.endswith(".csv") for file in written_files))

    def test_write_csv_empty_dataframe(self):
        # Given
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "output")
        df = self.spark.createDataFrame([], schema="departement STRING, nb_people INT")

        # When
        write_csv(df, output_path)

        # Then
        written_files = os.listdir(output_path)
        self.assertTrue(any(file.endswith(".csv") for file in written_files))
