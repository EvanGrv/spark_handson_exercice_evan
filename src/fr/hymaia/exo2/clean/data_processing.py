from pyspark.sql.functions import col, when

# Lire les fichiers
def read_data(spark, clients_path, villes_path):
    clients_df = spark.read.csv(clients_path, header=True, inferSchema=True)
    villes_df = spark.read.csv(villes_path, header=True, inferSchema=True)
    return clients_df, villes_df


# Filtrer les clients majeurs
def filter_major_clients(clients_df):
    return clients_df.where(col("age") >= 18)


# Joindre les villes et clients
def join_clients_villes(clients_df, villes_df):
    return clients_df.join(villes_df, "zip")


# Ajouter la colonne "departement"
def add_departement_column(df):
    return df.withColumn(
        "departement",
        when(
            col("zip").substr(1, 2) == "20",
            when(col("zip") <= "20190", "2A").otherwise("2B"),
        ).otherwise(col("zip").substr(1, 2)),
    )


# Écrire le résultat en Parquet
def write_parquet(df, output_path):
    df.write.mode("overwrite").partitionBy("departement").parquet(output_path)
