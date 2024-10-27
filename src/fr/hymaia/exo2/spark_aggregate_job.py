from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.aggregate.data_processing import (
    read_clean_data,
    calculate_population_by_departement,
    write_csv,
)


def create_spark_session():
    return (
        SparkSession.builder.appName("spark_aggregate_job")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    # Chemins des fichiers
    clean_data_path = "data/clean"
    output_path = "data/exo2/aggregate"

    # Traitement
    clean_df = read_clean_data(spark, clean_data_path)
    population_df = calculate_population_by_departement(clean_df)
    population_df.show()
    write_csv(population_df, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
