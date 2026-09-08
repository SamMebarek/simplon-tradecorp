import os
import sys
import shutil
import logging

from pyspark.sql import SparkSession

sys.path.append("/home/jovyan")

from src.utils import get_blob_service_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradeCorpWriter")


INPUT_PATH = "/home/jovyan/data/tmp/" "transformer_output/orders_enriched"


# ============================================================
# Écriture locale + upload ADLS
# ============================================================


def write_parquet_to_adls(
    df,
    output_name="orders_enriched",
    container_name="clean",
    local_dir="/home/jovyan/data/tmp/tradecorp_clean",
):
    local_path = os.path.join(local_dir, output_name)

    os.makedirs(local_dir, exist_ok=True)

    # --------------------------------------------------------
    # Nettoyage local
    # --------------------------------------------------------

    if os.path.exists(local_path):
        logger.info(f"Suppression de l'ancien dossier local : {local_path}")

        shutil.rmtree(local_path)

    # --------------------------------------------------------
    # Écriture locale Parquet
    # --------------------------------------------------------

    logger.info(f"Écriture locale Parquet : {local_path}")

    df.write.mode("overwrite").parquet(local_path)

    # --------------------------------------------------------
    # Connexion ADLS
    # --------------------------------------------------------

    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(container_name)

    # --------------------------------------------------------
    # Nettoyage du dossier distant
    # --------------------------------------------------------

    logger.info(f"Nettoyage ADLS : {container_name}/{output_name}/")

    blobs = container_client.list_blobs(name_starts_with=f"{output_name}/")

    for blob in blobs:

        logger.info(f"Suppression de {blob.name}")

        container_client.delete_blob(blob.name)

    logger.info("Ancien contenu ADLS supprimé")

    # --------------------------------------------------------
    # Upload du nouveau Parquet
    # --------------------------------------------------------

    logger.info(f"Upload de {output_name} vers ADLS")

    for root, _, files in os.walk(local_path):

        for filename in files:

            file_path = os.path.join(root, filename)

            relative_path = os.path.relpath(file_path, local_path)

            blob_name = f"{output_name}/{relative_path}".replace("\\", "/")

            blob_client = container_client.get_blob_client(blob_name)

            with open(file_path, "rb") as file:

                blob_client.upload_blob(file, overwrite=True)

    logger.info(
        f"{output_name} envoyé dans " f"le conteneur {container_name} avec succès"
    )


# ============================================================
# Main
# ============================================================


def main():

    spark = SparkSession.builder.appName("TradeCorpWriter").getOrCreate()

    try:

        # ----------------------------
        # Lecture sortie transformer
        # ----------------------------

        logger.info(f"Lecture du Parquet transformé : {INPUT_PATH}")

        df = spark.read.parquet(INPUT_PATH)

        logger.info(f"Nombre de lignes à écrire : {df.count()}")

        # ----------------------------
        # Écriture + upload Azure
        # ----------------------------

        write_parquet_to_adls(df, output_name="orders_enriched", container_name="clean")

        logger.info("Writer terminé avec succès")

    except Exception:

        logger.exception("Erreur dans writer.py")

        raise

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
