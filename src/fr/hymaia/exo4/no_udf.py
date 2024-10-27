from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when
from pyspark.sql.window import Window


def main():
    # Initialiser la SparkSession
    spark = (
        SparkSession.builder.appName("Window Functions")
        .master("local[*]")
        .getOrCreate()
    )

    # Charger les données
    df = spark.read.csv(
        "/Users/evan/PycharmProjects/spark-handson/src/resources/exo0/sell.csv",
        header=True,
        inferSchema=True,
    )

    # Ajouter la colonne category_name
    df = df.withColumn(
        "category_name", when(col("category") < 6, "food").otherwise("furniture")
    )

    # Calculer total_price_per_category_per_day
    window_spec_day = Window.partitionBy("category_name", "date")
    df = df.withColumn(
        "total_price_per_category_per_day", spark_sum("price").over(window_spec_day)
    )

    # Calculer total_price_per_category_per_day_last_30_days
    window_spec_30_days = (
        Window.partitionBy("category_name")
        .orderBy(col("date").cast("timestamp"))
        .rangeBetween(-2592000, 0)
    )
    df = df.withColumn(
        "total_price_per_category_per_day_last_30_days",
        spark_sum("price").over(window_spec_30_days),
    )

    # Afficher le résultat
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
