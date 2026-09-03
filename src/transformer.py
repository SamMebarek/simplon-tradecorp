import sys
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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradeCorpTransformer")


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
    # 5. Sélection du schéma final attendu
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
# TEST DIRECT DU FICHIER
# ============================================================


def main():

    from src.reader import download_files, read_files

    spark = SparkSession.builder.appName("TradeCorpTransformer").getOrCreate()

    try:

        logger.info("Téléchargement des fichiers")

        local_paths = download_files()

        logger.info("Lecture des fichiers")

        dataframes = read_files(spark, local_paths)

        logger.info("Construction du DataFrame enrichi")

        enriched = transform(dataframes)

        logger.info("Schéma du DataFrame final")

        enriched.printSchema()

        logger.info(f"Nombre de lignes : {enriched.count()}")

        enriched.show(10, truncate=False)

    except Exception:
        logger.exception("Erreur dans transformer.py")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
