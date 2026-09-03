import sys

from pyspark.sql import SparkSession

sys.path.append("/home/jovyan")

from src.utils import (
    clean_orders,
    add_sous_total,
    clean_customers,
)

from src.enrichment import add_currency_column

spark = SparkSession.builder.appName("TestTransformers").getOrCreate()


# ============================================================
# Q44 - Test clean_orders
# ============================================================


def test_clean_orders_supprime_shipped_date_null():

    data = [
        (
            1,
            "C001",
            1,
            "1997-01-10",
            "1997-01-20",
            "1997-01-15",
            1,
            20.0,
        ),
        (
            2,
            "C002",
            2,
            "1997-02-10",
            "1997-02-20",
            None,
            2,
            15.0,
        ),
    ]

    columns = [
        "order_id",
        "customer_id",
        "employee_id",
        "order_date",
        "required_date",
        "shipped_date",
        "ship_via",
        "freight",
    ]

    df_test = spark.createDataFrame(data, columns)

    df_result = clean_orders(df_test)

    assert df_result.count() == 1

    assert df_result.filter(df_result.shipped_date.isNull()).count() == 0


# ============================================================
# Q45 - Test add_sous_total
# ============================================================


def test_add_sous_total():

    data = [(10.0, 2, 0.1)]

    columns = [
        "prix_unitaire",
        "quantite",
        "discount",
    ]

    df_test = spark.createDataFrame(data, columns)

    df_result = add_sous_total(df_test)

    sous_total = df_result.collect()[0]["sous_total"]

    assert sous_total == 18.0


# ============================================================
# Q46 - Test clean_customers
# ============================================================


def test_clean_customers():

    data = [
        (
            "C001",
            " Test Company ",
            "   jean dupont   ",
            " Manager ",
            " 1 rue test ",
            " Paris ",
            "",
            "75000",
            " france ",
            "0102030405",
            "",
        )
    ]

    columns = [
        "customer_id",
        "company_name",
        "contact_name",
        "contact_title",
        "address",
        "city",
        "region",
        "postal_code",
        "country",
        "phone",
        "fax",
    ]

    df_test = spark.createDataFrame(data, columns)

    df_result = clean_customers(df_test)

    row = df_result.collect()[0]

    assert row["contact_name"] == "Jean Dupont"
    assert row["country"] == "FRANCE"


def test_add_currency_column():

    # DataFrame enrichi simulé
    data_enriched = [(1, "FRANCE", 100.0)]

    columns_enriched = ["order_id", "customer_country", "sous_total"]

    df_enriched = spark.createDataFrame(data_enriched, columns_enriched)

    # Mapping pays -> devise simulé
    data_currency = [("FRANCE", "EUR")]

    columns_currency = ["country", "currency"]

    country_currency = spark.createDataFrame(data_currency, columns_currency)

    # Taux simulé : aucun appel API
    exchange_rates = {"base": "USD", "rates": {"USD": 1.0, "EUR": 0.8}}

    # Enrichissement
    df_result = add_currency_column(df_enriched, country_currency, exchange_rates)

    row = df_result.collect()[0]

    # Vérifications
    assert row["currency"] == "EUR"
    assert row["sous_total_local"] == 80.0
