import os

from azure.storage.blob import BlobServiceClient

from pyspark.sql.functions import col, trim, initcap, upper, concat_ws, when, round

from pyspark.sql.types import DateType, DoubleType, IntegerType

# ============================================================
# CONNEXION AZURE STORAGE
# ============================================================


def get_blob_service_client():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

    if not account_name:
        raise ValueError("AZURE_STORAGE_ACCOUNT_NAME n'est pas défini")

    if not account_key:
        raise ValueError("AZURE_STORAGE_ACCOUNT_KEY n'est pas défini")

    account_url = f"https://{account_name}.blob.core.windows.net"

    return BlobServiceClient(account_url=account_url, credential=account_key)


# ============================================================
# CUSTOMERS
# ============================================================


def clean_customers(df):
    return (
        df.withColumn("company_name", trim(col("company_name")))
        .withColumn("contact_name", initcap(trim(col("contact_name"))))
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("city", trim(col("city")))
        .dropDuplicates(["customer_id"])
    )


# ============================================================
# ORDERS
# ============================================================


def clean_orders(df):
    df = (
        df.filter(col("shipped_date").isNotNull())
        .withColumn("order_date", col("order_date").cast(DateType()))
        .withColumn("required_date", col("required_date").cast(DateType()))
        .withColumn("shipped_date", col("shipped_date").cast(DateType()))
        .withColumn("freight", col("freight").cast(DoubleType()))
        .withColumnRenamed("ship_via", "shipper_id")
    )

    df = df.withColumn(
        "is_shipped", when(col("shipped_date").isNotNull(), True).otherwise(False)
    )

    return df


# ============================================================
# ORDER DETAILS
# ============================================================


def clean_order_details(df):
    return (
        df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("discount", col("discount").cast(DoubleType()))
        .withColumnRenamed("unit_price", "prix_unitaire")
        .withColumnRenamed("quantity", "quantite")
    )


def add_sous_total(df):
    return df.withColumn(
        "sous_total",
        round(col("prix_unitaire") * col("quantite") * (1 - col("discount")), 2),
    )


# ============================================================
# EMPLOYEES
# ============================================================


def clean_employees(df):
    df = df.select(
        "employee_id",
        "first_name",
        "last_name",
        "title",
        "hire_date",
        "city",
        "country",
    )

    return df.withColumn(
        "full_name", concat_ws(" ", trim(col("first_name")), trim(col("last_name")))
    )


# ============================================================
# PRODUCTS
# ============================================================


def clean_products(df):
    return df.withColumn("unit_price", col("unit_price").cast(DoubleType())).withColumn(
        "en_stock", when(col("units_in_stock") > 0, True).otherwise(False)
    )
