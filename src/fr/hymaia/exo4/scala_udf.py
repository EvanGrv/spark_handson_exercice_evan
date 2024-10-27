from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.column import Column, _to_seq, _to_java_column


def addCategoryName(spark, col):
    # Récupération du SparkContext
    sc = spark.sparkContext
    # Utilisation de l'UDF Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # Retour d'un objet colonne avec l'application de l'UDF Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    spark = (
        SparkSession.builder.appName("Scala UDF")
        .master("local[*]")
        .config(
            "spark.jars",
            "/Users/evan/PycharmProjects/spark-handson/src/resources/exo0/udf.jar",
        )
        .getOrCreate()
    )

    # Lecture des données
    df = spark.read.csv(
        "/Users/evan/PycharmProjects/spark-handson/src/resources/exo0/sell.csv",
        header=True,
        inferSchema=True,
    )

    # Application de l'UDF Scala pour ajouter la colonne "category_name"
    df = df.withColumn("category_name", addCategoryName(spark, col("category")))

    # Affichage du résultat
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
