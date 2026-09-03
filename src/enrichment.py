import sys
import logging

from pyspark.sql.functions import col, create_map, lit
from itertools import chain

sys.path.append("/home/jovyan")


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradeCorpEnrichment")


def add_currency_column(df_enriched, country_currency, exchange_rates):
    """
    Ajoute :
    - currency : devise du pays du client
    - sous_total_local : sous_total converti dans la devise locale
    """

    logger.info("Ajout de la devise du client")

    # Mapping pays devise
    country_currency = country_currency.select(
        col("country").alias("customer_country"), col("currency")
    )

    df_enriched = df_enriched.join(country_currency, on="customer_country", how="left")

    logger.info("Application des taux de change")

    rates = exchange_rates["rates"]

    rates_map = create_map(
        *list(
            chain.from_iterable(
                (lit(currency), lit(rate)) for currency, rate in rates.items()
            )
        )
    )

    df_enriched = df_enriched.withColumn("exchange_rate", rates_map[col("currency")])

    df_enriched = df_enriched.withColumn(
        "sous_total_local", col("sous_total") * col("exchange_rate")
    )

    logger.info("Enrichissement devise terminé")

    return df_enriched
