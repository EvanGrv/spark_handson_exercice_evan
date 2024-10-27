from pyspark.sql.functions import col

# Lire le fichier Parquet du premier job
def read_clean_data(spark, clean_data_path):
    return spark.read.parquet(clean_data_path)


# Calculer la population par département
def calculate_population_by_departement(df):
    return (
        df.groupBy("departement")
        .count()
        .withColumnRenamed("count", "nb_people")
        .orderBy(col("nb_people").desc(), col("departement"))
    )


# Écrire le résultat en CSV
def write_csv(df, output_path):
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
