import os
import sys
import json
import logging

from pyspark.sql import SparkSession

sys.path.append("/home/jovyan")

from src.utils import get_blob_service_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradeCorpReader")


CSV_FILES = [
    "categories.csv",
    "customers.csv",
    "employees.csv",
    "order_details.csv",
    "orders.csv",
    "products.csv",
    "shippers.csv",
    "suppliers.csv",
]


REFERENCE_FILES = [
    "country_currency.csv",
    "exchange_rates.json",
]


# ============================================================
# Téléchargement des 8 fichiers métier
# ============================================================


def download_files(
    container_name="raw", local_dir="/home/jovyan/data/tmp/tradecorp_raw"
):
    os.makedirs(local_dir, exist_ok=True)

    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(container_name)

    local_paths = {}

    for filename in CSV_FILES:

        logger.info(f"Téléchargement de {filename}")

        blob_client = container_client.get_blob_client(filename)

        local_path = os.path.join(local_dir, filename)

        with open(local_path, "wb") as file:
            file.write(blob_client.download_blob().readall())

        local_paths[filename] = local_path

    return local_paths


# ============================================================
# Lecture Spark des 8 fichiers métier
# ============================================================


def read_files(spark, local_paths):

    dataframes = {}

    for filename, path in local_paths.items():

        table_name = filename.replace(".csv", "")

        logger.info(f"Lecture Spark de {filename}")

        dataframes[table_name] = spark.read.csv(path, header=True, inferSchema=True)

    return dataframes


# ============================================================
# Écriture intermédiaire des DataFrames métier
# ============================================================


def write_intermediate_data(
    dataframes, output_dir="/home/jovyan/data/tmp/reader_output"
):
    os.makedirs(output_dir, exist_ok=True)

    for name, df in dataframes.items():

        output_path = os.path.join(output_dir, name)

        logger.info(f"Écriture intermédiaire de {name} vers {output_path}")

        df.write.mode("overwrite").parquet(output_path)


# ============================================================
# Téléchargement des fichiers de référence
# ============================================================


def download_reference_files(
    container_name="raw", local_dir="/home/jovyan/data/tmp/tradecorp_reference"
):
    os.makedirs(local_dir, exist_ok=True)

    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(container_name)

    local_paths = {}

    for filename in REFERENCE_FILES:

        blob_name = f"reference/{filename}"

        logger.info(f"Téléchargement de {blob_name}")

        blob_client = container_client.get_blob_client(blob_name)

        local_path = os.path.join(local_dir, filename)

        with open(local_path, "wb") as file:
            file.write(blob_client.download_blob().readall())

        local_paths[filename] = local_path

    return local_paths


# ============================================================
# Lecture des fichiers de référence
# ============================================================


def read_reference_files(spark, local_paths):

    logger.info("Lecture de country_currency.csv")

    country_currency = spark.read.csv(
        local_paths["country_currency.csv"], header=True, inferSchema=True
    )

    logger.info("Lecture de exchange_rates.json")

    with open(local_paths["exchange_rates.json"], "r", encoding="utf-8") as file:
        exchange_rates = json.load(file)

    return country_currency, exchange_rates


# ============================================================
# Main
# ============================================================


def main():

    spark = SparkSession.builder.appName("TradeCorpReader").getOrCreate()

    try:

        # ----------------------------
        # Fichiers métier
        # ----------------------------

        local_paths = download_files()

        dataframes = read_files(spark, local_paths)

        for name, df in dataframes.items():
            logger.info(f"{name} : {df.count()} lignes")

        # ----------------------------
        # Écriture intermédiaire
        # ----------------------------

        write_intermediate_data(dataframes)

        # ----------------------------
        # Fichiers de référence
        # ----------------------------

        reference_paths = download_reference_files()

        country_currency, exchange_rates = read_reference_files(spark, reference_paths)

        logger.info(f"country_currency : " f"{country_currency.count()} lignes")

        logger.info(f"exchange_rates : " f"{len(exchange_rates['rates'])} devises")

        logger.info(
            "Tous les fichiers ont été téléchargés, " "lus et sauvegardés avec succès"
        )

    except Exception:

        logger.exception("Erreur dans reader.py")

        raise

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
