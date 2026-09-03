import sys
import json
import logging
from urllib.request import urlopen

sys.path.append("/home/jovyan")

from src.utils import get_blob_service_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("FetchExchangeRates")


API_URL = "https://api.exchangerate-api.com/v4/latest/USD"

CONTAINER_NAME = "raw"
BLOB_NAME = "reference/exchange_rates.json"


def fetch_exchange_rates():
    logger.info("Récupération des taux de change")

    with urlopen(API_URL) as response:
        data = json.loads(response.read().decode("utf-8"))

    logger.info(f"Taux récupérés : {len(data['rates'])} devises")

    return data


def upload_exchange_rates(data):
    logger.info(f"Upload vers {CONTAINER_NAME}/{BLOB_NAME}")

    blob_service_client = get_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME, blob=BLOB_NAME
    )

    json_data = json.dumps(data)

    blob_client.upload_blob(json_data, overwrite=True)

    logger.info("exchange_rates.json uploadé avec succès")


def main():
    try:
        data = fetch_exchange_rates()

        upload_exchange_rates(data)

        logger.info("Mise à jour des taux terminée")

    except Exception:
        logger.exception("Erreur lors de la récupération des taux de change")
        raise


if __name__ == "__main__":
    main()
