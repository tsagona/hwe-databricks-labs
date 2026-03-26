"""Tests for Week 4 — Bronze layer ingestion."""

import os
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DecimalType, StringType, StructField, StructType, TimestampType,
)

from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W4_LAB = os.path.join(_REPO_ROOT, "labs", "week4", "week4_lab.ipynb")

# ---------------------------------------------------------------------------
# Fixtures — create temp views that replace the read_files() cells
# ---------------------------------------------------------------------------

@pytest.fixture()
def bronze_stores_view(schemas):
    """Create stores_raw temp view with test data."""
    spark = schemas
    ts = datetime(2025, 1, 1)
    data = [
        Row(store_nbr="S001", name="Downtown Books", address="100 Main St",
            city="Springfield", state="IL", zip="62701",
            ingestion_timestamp=ts, source_filename="stores.csv"),
        Row(store_nbr="S002", name="Airport Books", address="200 Terminal Dr",
            city="Springfield", state="IL", zip="62702",
            ingestion_timestamp=ts, source_filename="stores.csv"),
    ]
    spark.createDataFrame(data).createOrReplaceTempView("stores_raw")
    return spark


@pytest.fixture()
def bronze_categories_view(schemas):
    """Create categories_raw temp view with 3-level hierarchy."""
    spark = schemas
    ts = datetime(2025, 1, 1)
    data = [
        Row(category_id="1", category_name="Fiction",
            parent_category_id="", ingestion_timestamp=ts,
            source_filename="categories.csv"),
        Row(category_id="3", category_name="Science Fiction",
            parent_category_id="1", ingestion_timestamp=ts,
            source_filename="categories.csv"),
        Row(category_id="11", category_name="Space Opera",
            parent_category_id="3", ingestion_timestamp=ts,
            source_filename="categories.csv"),
    ]
    spark.createDataFrame(data).createOrReplaceTempView("categories_raw")
    return spark


@pytest.fixture()
def bronze_books_view(schemas):
    """Create books_raw temp view with test books."""
    spark = schemas
    ts = datetime(2025, 1, 1)
    data = [
        Row(isbn="978-0-00-000001-1", title="Test Book One",
            author="Author A", category_id="11",
            ingestion_timestamp=ts, source_filename="books.csv"),
        Row(isbn="978-0-00-000002-2", title="Test Book Two",
            author="Author B", category_id="11",
            ingestion_timestamp=ts, source_filename="books.csv"),
    ]
    spark.createDataFrame(data).createOrReplaceTempView("books_raw")
    return spark


@pytest.fixture()
def bronze_online_orders_view(schemas):
    """Create online_orders_raw temp view with test orders."""
    spark = schemas
    ts = datetime(2025, 1, 1)
    data = [
        Row(order_id="ONL-001", order_timestamp=datetime(2025, 6, 15, 10, 0),
            customer_email="alice@example.com", customer_name="Alice Smith",
            customer_address="123 Elm St", customer_city="Springfield",
            customer_state="IL", customer_zip="62701",
            items='[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":2,"unit_price":19.99}]',
            payment_method="credit_card", total_amount=39.98,
            ingestion_timestamp=ts, source_filename="online_orders_1.csv"),
    ]
    df = spark.createDataFrame(data)
    # Cast total_amount to DECIMAL(10,2) to match bronze table schema
    from pyspark.sql import functions as F
    df = df.withColumn("total_amount", F.col("total_amount").cast("decimal(10,2)"))
    df.createOrReplaceTempView("online_orders_raw")
    return spark


@pytest.fixture()
def bronze_instore_orders_view(schemas):
    """Create instore_orders_raw temp view with test orders."""
    spark = schemas
    ts = datetime(2025, 1, 1)
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("store_nbr", StringType()),
        StructField("customer_email", StringType()),
        StructField("items", StringType()),
        StructField("payment_method", StringType()),
        StructField("total_amount", DecimalType(10, 2)),
        StructField("cashier_name", StringType()),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("source_filename", StringType()),
    ])
    data = [(
        "INS-001", datetime(2025, 6, 15, 11, 0), "S001", None,
        '[{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]',
        "cash", Decimal("24.99"), "Bob Jones", ts, "instore_orders_1.csv",
    )]
    df = spark.createDataFrame(data, schema=schema)
    df.createOrReplaceTempView("instore_orders_raw")
    return spark


# ---------------------------------------------------------------------------
# Helper to run a SQL cell from the notebook
# ---------------------------------------------------------------------------

def _run_cell(spark, pattern):
    sql = find_cell(_W4_LAB, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    spark.sql(sql)


# ---------------------------------------------------------------------------
# Tests — INSERT OVERWRITE (static reference data)
# ---------------------------------------------------------------------------

def test_stores_insert_overwrite(bronze_stores_view):
    spark = bronze_stores_view
    _run_cell(spark, "bronze_stores_load")
    rows = spark.sql("SELECT * FROM bronze.stores").collect()
    assert len(rows) == 2
    names = {r.name for r in rows}
    assert names == {"Downtown Books", "Airport Books"}


def test_categories_insert_overwrite(bronze_categories_view):
    spark = bronze_categories_view
    _run_cell(spark, "bronze_categories_load")
    rows = spark.sql("SELECT * FROM bronze.categories").collect()
    assert len(rows) == 3
    ids = {r.category_id for r in rows}
    assert ids == {"1", "3", "11"}


def test_books_insert_overwrite(bronze_books_view):
    spark = bronze_books_view
    _run_cell(spark, "bronze_books_load")
    rows = spark.sql("SELECT * FROM bronze.books").collect()
    assert len(rows) == 2
    assert rows[0].ingestion_timestamp is not None
    assert rows[0].source_filename is not None


# ---------------------------------------------------------------------------
# Tests — MERGE INTO (transactional data)
# ---------------------------------------------------------------------------

def test_online_orders_merge(bronze_online_orders_view):
    spark = bronze_online_orders_view
    _run_cell(spark, "bronze_online_orders_merge")
    rows = spark.sql("SELECT * FROM bronze.online_orders").collect()
    assert len(rows) == 1
    assert rows[0].order_id == "ONL-001"
    assert rows[0].customer_email == "alice@example.com"


def test_instore_orders_merge(bronze_instore_orders_view):
    spark = bronze_instore_orders_view
    _run_cell(spark, "bronze_instore_orders_merge")
    rows = spark.sql("SELECT * FROM bronze.instore_orders").collect()
    assert len(rows) == 1
    assert rows[0].order_id == "INS-001"
    assert rows[0].cashier_name == "Bob Jones"


def test_merge_is_idempotent(bronze_online_orders_view):
    spark = bronze_online_orders_view
    _run_cell(spark, "bronze_online_orders_merge")
    count_after_first = spark.sql("SELECT COUNT(*) AS cnt FROM bronze.online_orders").collect()[0].cnt
    # Run the same MERGE again
    _run_cell(spark, "bronze_online_orders_merge")
    count_after_second = spark.sql("SELECT COUNT(*) AS cnt FROM bronze.online_orders").collect()[0].cnt
    assert count_after_first == count_after_second
