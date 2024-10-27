import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql.types import StructType, StructField, StringType


class TestWordCount(unittest.TestCase):
    # Création d'un SparkSession partagé pour tous les tests de cette classe
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("Unit Test for WordCount")
            .master("local[*]")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        # Arrêt du SparkSession partagé après l'exécution de tous les tests
        cls.spark.stop()

    def test_wordcount_nominal(self):
        # Given
        data = [Row(text="hello hello world")]
        df = self.spark.createDataFrame(data)

        # When
        result_df = wordcount(df, "text")

        # Then
        expected_data = [Row(word="hello", count=2), Row(word="world", count=1)]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_wordcount_empty(self):
        # Given
        schema = StructType([StructField("text", StringType(), True)])
        empty_df = self.spark.createDataFrame([], schema)

        # When
        result_df = wordcount(empty_df, "text")

        # Then
        self.assertTrue(result_df.rdd.isEmpty())
