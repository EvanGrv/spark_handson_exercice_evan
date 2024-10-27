import unittest
import os
import tempfile
from pyspark.sql import SparkSession, Row
from src.fr.hymaia.exo2.clean.data_processing import (
    read_data,
    filter_major_clients,
    join_clients_villes,
    add_departement_column,
    write_parquet,
)


class TestIntegrationClean(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("Integration Test for Clean Job")
            .master("local[*]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_complete_clean_job(self):
        # Given
        temp_dir = tempfile.mkdtemp()
        clients_path = os.path.join(temp_dir, "clients_bdd.csv")
        villes_path = os.path.join(temp_dir, "city_zipcode.csv")
        output_path = os.path.join(temp_dir, "output")

        # Écrire les données de test
        clients_data = [("John", 25, "75020"), ("Doe", 30, "20190")]
        villes_data = [("75020", "Paris"), ("20190", "Ajaccio")]
        self.spark.createDataFrame(clients_data, ["name", "age", "zip"]).write.csv(
            clients_path, header=True, mode="overwrite"
        )
        self.spark.createDataFrame(villes_data, ["zip", "city"]).write.csv(
            villes_path, header=True, mode="overwrite"
        )

        # When
        clients_df, villes_df = read_data(self.spark, clients_path, villes_path)
        major_clients_df = filter_major_clients(clients_df)
        joined_df = join_clients_villes(major_clients_df, villes_df)
        result_df = add_departement_column(joined_df)

        # Écrire le résultat en Parquet
        write_parquet(result_df, output_path)

        # Then
        result_df = self.spark.read.parquet(output_path)
        result_df = (
            result_df.withColumn("zip", result_df["zip"].cast("string"))
            .select("name", "age", "zip", "city", "departement")
            .orderBy("zip")
        )

        # Créer le DataFrame attendu
        expected_data = [
            Row(name="John", age=25, zip="75020", city="Paris", departement="75"),
            Row(name="Doe", age=30, zip="20190", city="Ajaccio", departement="2A"),
        ]
        expected_df = self.spark.createDataFrame(expected_data).orderBy("zip")

        # Comparaison des DataFrames
        self.assertEqual(result_df.collect(), expected_df.collect())

        # Nettoyer les fichiers temporaires
        for root, _, files in os.walk(temp_dir):
            for file in files:
                os.remove(os.path.join(root, file))


if __name__ == "__main__":
    unittest.main()
