import os
import sys
import shutil
import logging

sys.path.append("/home/jovyan")
from src.utils import get_blob_service_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradeCorpWriter")


def write_parquet_to_adls(
    df,
    output_name="orders_enriched",
    container_name="clean",
    local_dir="/home/jovyan/data/tmp/tradecorp_clean",
):
    local_path = os.path.join(local_dir, output_name)

    os.makedirs(local_dir, exist_ok=True)

    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    logger.info(f"Écriture locale Parquet : {local_path}")

    df.write.mode("overwrite").parquet(local_path)

    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)

    logger.info(f"Upload de {output_name} vers ADLS")

    for root, _, files in os.walk(local_path):
        for filename in files:
            file_path = os.path.join(root, filename)

            relative_path = os.path.relpath(file_path, local_path)

            blob_name = f"{output_name}/{relative_path}".replace("\\", "/")

            blob_client = container_client.get_blob_client(blob_name)

            with open(file_path, "rb") as file:
                blob_client.upload_blob(file, overwrite=True)

    logger.info(f"{output_name} envoyé dans le conteneur clean avec succès")
