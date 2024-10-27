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
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris", departement="75"),
            Row(name="Bob", age=35, zip="75015", city="Paris", departement="75"),
            Row(name="Charlie", age=40, zip="20200", city="Bastia", departement="2B"),
        ]
        df = self.spark.createDataFrame(data)
        result_df = calculate_population_by_departement(df)
        expected_data = [
            Row(departement="75", nb_people=2),
            Row(departement="2B", nb_people=1),
        ]
        expected_df = self.spark.createDataFrame(expected_data).orderBy(
            "nb_people", "departement", ascending=[False, True]
        )
        result_df = result_df.orderBy(
            "nb_people", "departement", ascending=[False, True]
        )
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_population_by_departement_single_departement(self):
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris", departement="75"),
            Row(name="Bob", age=35, zip="75015", city="Paris", departement="75"),
        ]
        df = self.spark.createDataFrame(data)
        result_df = calculate_population_by_departement(df)
        expected_data = [Row(departement="75", nb_people=2)]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_population_by_departement_missing_column(self):
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris"),
            Row(name="Bob", age=35, zip="75015", city="Paris"),
        ]
        df = self.spark.createDataFrame(data)
        with self.assertRaises(Exception):
            calculate_population_by_departement(df)

    def test_calculate_population_by_departement_empty_dataframe(self):
        df = self.spark.createDataFrame(
            [],
            schema="name STRING, age INT, zip STRING, city STRING, departement STRING",
        )
        result_df = calculate_population_by_departement(df)
        self.assertEqual(result_df.count(), 0)

    def test_read_clean_data_nominal(self):
        temp_dir = tempfile.mkdtemp()
        clean_data_path = os.path.join(temp_dir, "clean_data.parquet")
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris", departement="75"),
            Row(name="Bob", age=35, zip="75015", city="Paris", departement="75"),
        ]
        self.spark.createDataFrame(data).write.parquet(clean_data_path)
        df = read_clean_data(self.spark, clean_data_path)
        self.assertEqual(df.count(), 2)

    def test_read_clean_data_missing_file(self):
        clean_data_path = "/path/to/non_existent_file.parquet"
        with self.assertRaises(Exception):
            read_clean_data(self.spark, clean_data_path)

    def test_write_csv_nominal(self):
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "output")
        data = [Row(departement="75", nb_people=2), Row(departement="2B", nb_people=1)]
        df = self.spark.createDataFrame(data)
        write_csv(df, output_path)
        written_files = os.listdir(output_path)
        self.assertTrue(any(file.endswith(".csv") for file in written_files))

    def test_write_csv_empty_dataframe(self):
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "output")
        df = self.spark.createDataFrame([], schema="departement STRING, nb_people INT")
        write_csv(df, output_path)
        written_files = os.listdir(output_path)
        self.assertTrue(any(file.endswith(".csv") for file in written_files))
