import unittest
import os
import tempfile
from pyspark.sql import SparkSession, Row
from src.fr.hymaia.exo2.aggregate.data_processing import (
    read_clean_data,
    calculate_population_by_departement,
    write_csv,
)


class TestIntegrationAggregate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("Integration Test for Aggregate Job")
            .master("local[*]")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_complete_aggregate_job(self):
        temp_dir = tempfile.mkdtemp()
        clean_data_path = os.path.join(temp_dir, "clean_data.parquet")
        output_path = os.path.join(temp_dir, "output")
        data = [
            Row(name="Alice", age=30, zip="75020", city="Paris", departement="75"),
            Row(name="Bob", age=35, zip="75015", city="Paris", departement="75"),
            Row(name="Charlie", age=40, zip="20200", city="Bastia", departement="2B"),
        ]
        self.spark.createDataFrame(data).write.parquet(
            clean_data_path, mode="overwrite"
        )
        clean_df = read_clean_data(self.spark, clean_data_path)
        population_df = calculate_population_by_departement(clean_df)
        write_csv(population_df, output_path)
        result_df = self.spark.read.csv(output_path, header=True, inferSchema=True)
        result_df = result_df.orderBy("departement")
        expected_data = [
            Row(departement="75", nb_people=2),
            Row(departement="2B", nb_people=1),
        ]
        expected_df = self.spark.createDataFrame(expected_data).orderBy("departement")
        self.assertEqual(result_df.collect(), expected_df.collect())
        for root, _, files in os.walk(temp_dir):
            for file in files:
                os.remove(os.path.join(root, file))


if __name__ == "__main__":
    unittest.main()
