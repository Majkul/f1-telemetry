import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    parser.add_argument("--raw-path", required=True)
    return parser.parse_args()

def get_spark():
    return SparkSession.builder \
        .appName("F1_Dims_ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

def write_jdbc(df, table, url, user, password):
    df.write.jdbc(
        url=url,
        table=table,
        mode="append",
        properties={
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }
    )

if __name__ == "__main__":
    args = get_args()
    spark = get_spark()

    path = args.raw_path

    drivers_raw = spark.read.parquet(f"{path}/drivers_raw.parquet")
    teams_raw = spark.read.parquet(f"{path}/teams_raw.parquet")
    compounds_raw = spark.read.parquet(f"{path}/compounds_raw.parquet")

    write_jdbc(drivers_raw, "dim_driver", args.db_url, args.db_user, args.db_password)
    write_jdbc(teams_raw, "dim_team", args.db_url, args.db_user, args.db_password)
    write_jdbc(compounds_raw, "dim_compound", args.db_url, args.db_user, args.db_password)

    print("Załadowano: dim_driver, dim_team, dim_compound")
    spark.stop()