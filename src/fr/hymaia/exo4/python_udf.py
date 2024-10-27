from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def main():
    # Initialiser la SparkSession
    spark = SparkSession.builder.appName("Python UDF").master("local[*]").getOrCreate()

    # Charger les données
    df = spark.read.csv(
        "/Users/evan/PycharmProjects/spark-handson/src/resources/exo4/sell.csv",
        header=True,
        inferSchema=True,
    )

    # Ajouter la colonne category_name
    df = df.withColumn(
        "category_name", when(col("category") < 6, "food").otherwise("furniture")
    )

    # Afficher le résultat
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
