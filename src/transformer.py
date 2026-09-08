import os
import sys
import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.append("/home/jovyan")

from src.utils import (
    clean_customers,
    clean_orders,
    clean_order_details,
    add_sous_total,
    clean_employees,
    clean_products,
)

from src.enrichment import add_currency_column

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradeCorpTransformer")


# ============================================================
# Chemins partagés entre les conteneurs Airflow
# ============================================================

INPUT_DIR = "/home/jovyan/data/tmp/reader_output"

REFERENCE_DIR = "/home/jovyan/data/tmp/tradecorp_reference"

OUTPUT_DIR = "/home/jovyan/data/tmp/transformer_output/orders_enriched"


TABLES = [
    "categories",
    "customers",
    "employees",
    "order_details",
    "orders",
    "products",
    "shippers",
    "suppliers",
]


# ============================================================
# Lecture des données produites par reader.py
# ============================================================


def read_intermediate_data(spark, input_dir=INPUT_DIR):

    dataframes = {}

    for table_name in TABLES:

        path = os.path.join(input_dir, table_name)

        logger.info(f"Lecture intermédiaire de {table_name} depuis {path}")

        dataframes[table_name] = spark.read.parquet(path)

    return dataframes


# ============================================================
# Lecture des fichiers de référence
# ============================================================


def read_reference_data(spark, reference_dir=REFERENCE_DIR):

    country_currency_path = os.path.join(reference_dir, "country_currency.csv")

    exchange_rates_path = os.path.join(reference_dir, "exchange_rates.json")

    logger.info("Lecture de country_currency.csv")

    country_currency = spark.read.csv(
        country_currency_path, header=True, inferSchema=True
    )

    logger.info("Lecture de exchange_rates.json")

    with open(exchange_rates_path, "r", encoding="utf-8") as file:

        exchange_rates = json.load(file)

    return country_currency, exchange_rates


# ============================================================
# Transformation
# ============================================================


def transform(dataframes):

    # ========================================================
    # 1. Nettoyage des tables
    # ========================================================

    customers = clean_customers(dataframes["customers"])

    orders = clean_orders(dataframes["orders"])

    order_details = clean_order_details(dataframes["order_details"])

    order_details = add_sous_total(order_details)

    employees = clean_employees(dataframes["employees"])

    products = clean_products(dataframes["products"])

    categories = dataframes["categories"]

    shippers = dataframes["shippers"]

    # ========================================================
    # 2. Préparation des colonnes
    # ========================================================

    customers = customers.select(
        "customer_id",
        col("company_name").alias("customer_name"),
        col("country").alias("customer_country"),
        col("city").alias("customer_city"),
    )

    employees = employees.select("employee_id", "full_name")

    categories = categories.select("category_id", "category_name")

    shippers = shippers.select("shipper_id", col("company_name").alias("shipper_name"))

    products = products.select("product_id", "category_id", "product_name", "en_stock")

    orders = orders.select(
        "order_id",
        "customer_id",
        "employee_id",
        "order_date",
        "required_date",
        "shipped_date",
        "freight",
        "shipper_id",
        "is_shipped",
    )

    order_details = order_details.select(
        "order_id", "product_id", "prix_unitaire", "quantite", "discount", "sous_total"
    )

    # ========================================================
    # 3. Produit + catégorie
    # ========================================================

    products_categories = products.join(categories, on="category_id", how="inner")

    # ========================================================
    # 4. Jointure finale
    # ========================================================

    enriched = (
        order_details.join(orders, on="order_id", how="inner")
        .join(customers, on="customer_id", how="inner")
        .join(products_categories, on="product_id", how="inner")
        .join(employees, on="employee_id", how="inner")
        .join(shippers, on="shipper_id", how="inner")
    )

    # ========================================================
    # 5. Sélection du schéma
    # ========================================================

    enriched = enriched.select(
        col("order_id").cast("integer").alias("order_id"),
        col("customer_id").cast("string").alias("customer_id"),
        col("employee_id").cast("integer").alias("employee_id"),
        col("product_id").cast("integer").alias("product_id"),
        "order_date",
        "required_date",
        "shipped_date",
        col("freight").cast("double").alias("freight"),
        "is_shipped",
        col("prix_unitaire").cast("double").alias("prix_unitaire"),
        col("quantite").cast("integer").alias("quantite"),
        col("discount").cast("double").alias("discount"),
        col("sous_total").cast("double").alias("sous_total"),
        "customer_name",
        "customer_country",
        "customer_city",
        "product_name",
        "category_name",
        "en_stock",
        "full_name",
        "shipper_name",
    )

    return enriched


# ============================================================
# Écriture intermédiaire pour writer.py
# ============================================================


def write_intermediate_data(dataframe, output_dir=OUTPUT_DIR):

    logger.info(f"Écriture du résultat transformé vers {output_dir}")

    dataframe.write.mode("overwrite").parquet(output_dir)

    logger.info("Résultat intermédiaire sauvegardé avec succès")


# ============================================================
# Main
# ============================================================


def main():

    spark = SparkSession.builder.appName("TradeCorpTransformer").getOrCreate()

    try:

        # ----------------------------
        # Lecture sortie reader
        # ----------------------------

        logger.info("Lecture des données produites par reader.py")

        dataframes = read_intermediate_data(spark)

        # ----------------------------
        # Transformation métier
        # ----------------------------

        logger.info("Début des transformations")

        enriched = transform(dataframes)

        logger.info("Transformations terminées")

        # ----------------------------
        # Lecture des références
        # ----------------------------

        logger.info("Lecture des données de référence")

        country_currency, exchange_rates = read_reference_data(spark)

        # ----------------------------
        # Enrichissement devise
        # ----------------------------

        logger.info("Début de l'enrichissement devise")

        enriched = add_currency_column(enriched, country_currency, exchange_rates)

        logger.info("Enrichissement devise terminé")

        # ----------------------------
        # Vérification
        # ----------------------------

        logger.info(f"Nombre de lignes : {enriched.count()}")

        enriched.printSchema()

        # ----------------------------
        # Écriture pour writer.py
        # ----------------------------

        write_intermediate_data(enriched)

        logger.info("Transformer terminé avec succès")

    except Exception:

        logger.exception("Erreur dans transformer.py")

        raise

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
