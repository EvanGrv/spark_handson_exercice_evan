from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.clean.data_processing import (
    read_data,
    filter_major_clients,
    join_clients_villes,
    add_departement_column,
    write_parquet,
)


def create_spark_session():
    return (
        SparkSession.builder.appName("spark_clean_job").master("local[*]").getOrCreate()
    )


def main(
    clients_path="src/resources/exo2/clients_bdd.csv",
    villes_path="src/resources/exo2/city_zipcode.csv",
    output_path="data/exo2/clean",
):
    spark = create_spark_session()

    # Traitement
    clients_df, villes_df = read_data(spark, clients_path, villes_path)
    major_clients_df = filter_major_clients(clients_df)
    joined_df = join_clients_villes(major_clients_df, villes_df)
    result_df = add_departement_column(joined_df)
    write_parquet(result_df, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
