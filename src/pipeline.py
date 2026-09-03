import sys
import logging

from pyspark.sql import SparkSession

sys.path.append("/home/jovyan")

from src.reader import (
    download_files,
    read_files,
    download_reference_files,
    read_reference_files,
)

from src.transformer import transform
from src.enrichment import add_currency_column
from src.writer import write_parquet_to_adls

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradeCorpPipeline")


def main():

    spark = None

    try:

        logger.info("Démarrage du pipeline TradeCorp")

        spark = SparkSession.builder.appName("TradeCorpPipeline").getOrCreate()

        # ====================================================
        # 1. Lecture des fichiers métier
        # ====================================================

        logger.info("Début de la lecture des données métier")

        local_paths = download_files()

        dataframes = read_files(spark, local_paths)

        logger.info("Lecture des données métier terminée")

        # ====================================================
        # 2. Transformation
        # ====================================================

        logger.info("Début des transformations")

        df_enriched = transform(dataframes)

        logger.info("Transformations terminées")

        # ====================================================
        # 3. Lecture des fichiers de référence
        # ====================================================

        logger.info("Début de la lecture des fichiers de référence")

        reference_paths = download_reference_files()

        country_currency, exchange_rates = read_reference_files(spark, reference_paths)

        logger.info("Lecture des fichiers de référence terminée")

        # ====================================================
        # 4. Enrichissement devise
        # ====================================================

        logger.info("Début de l'enrichissement devise")

        df_enriched = add_currency_column(df_enriched, country_currency, exchange_rates)

        logger.info("Enrichissement devise terminé")

        # ====================================================
        # 5. Écriture
        # ====================================================

        logger.info("Début de l'écriture")

        write_parquet_to_adls(df_enriched, output_name="orders_enriched")

        logger.info("Écriture terminée")

        logger.info("Pipeline TradeCorp terminé avec succès")

    except Exception:

        logger.exception("Erreur lors de l'exécution du pipeline TradeCorp")

        raise

    finally:

        if spark is not None:

            logger.info("Arrêt de la SparkSession")

            spark.stop()


if __name__ == "__main__":
    main()
