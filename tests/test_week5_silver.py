"""Tests for Week 5 — Silver layer transformations."""

import os
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W5_LAB = os.path.join(_REPO_ROOT, "labs", "week5", "week5_lab.ipynb")


# ---------------------------------------------------------------------------
# Fixture — populate bronze tables with test data
# ---------------------------------------------------------------------------

@pytest.fixture()
def bronze_data(schemas):
    """Insert test rows into bronze tables for silver tests."""
    spark = schemas

    # --- bronze.categories: 3-level hierarchy ---
    spark.sql("""
        INSERT INTO bronze.categories VALUES
        ('1',  'Fiction',         '',  current_timestamp(), 'categories.csv'),
        ('3',  'Science Fiction', '1', current_timestamp(), 'categories.csv'),
        ('11', 'Space Opera',     '3', current_timestamp(), 'categories.csv')
    """)

    # --- bronze.stores ---
    spark.sql("""
        INSERT INTO bronze.stores VALUES
        ('S001', 'Downtown Books', '100 Main St', 'Springfield', 'IL', '62701',
         current_timestamp(), 'stores.csv')
    """)

    # --- bronze.books: 5 rows, only 2 should pass ISBN+title validation ---
    spark.sql("""
        INSERT INTO bronze.books VALUES
        ('978-0-00-000001-1', 'Test Book One',  'Author A', '11', current_timestamp(), 'books.csv'),
        ('978-0-00-000002-2', 'Test Book Two',  'Author B', '11', current_timestamp(), 'books.csv'),
        ('BADISBN',           'Bad ISBN Book',   'Author C', '11', current_timestamp(), 'books.csv'),
        ('978-0-00-000004-4', '',                'Author D', '11', current_timestamp(), 'books.csv'),
        ('978-0-00-000005-5', '   ',             'Author E', '11', current_timestamp(), 'books.csv')
    """)

    # --- bronze.online_orders: 2 orders from same customer, different timestamps ---
    spark.sql("""
        INSERT INTO bronze.online_orders VALUES
        ('ONL-001', CAST('2025-06-01 10:00:00' AS TIMESTAMP),
         'alice@example.com', 'Alice Old', '100 Old St', 'OldCity', 'IL', '60001',
         '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":2,"unit_price":19.99}]',
         'credit_card', CAST(39.98 AS DECIMAL(10,2)),
         current_timestamp(), 'online_orders_1.csv'),
        ('ONL-002', CAST('2025-07-15 14:00:00' AS TIMESTAMP),
         'alice@example.com', 'Alice New', '200 New Ave', 'NewCity', 'IL', '60002',
         '[{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]',
         'debit_card', CAST(24.99 AS DECIMAL(10,2)),
         current_timestamp(), 'online_orders_2.csv')
    """)

    # --- bronze.instore_orders: 2 orders ---
    # One with NULL email (should become 'in-store' sentinel)
    # One with email
    spark.sql("""
        INSERT INTO bronze.instore_orders VALUES
        ('INS-001', CAST('2025-06-15 11:00:00' AS TIMESTAMP),
         'S001', NULL,
         '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":1,"unit_price":19.99}]',
         'cash', CAST(19.99 AS DECIMAL(10,2)), 'Bob Jones',
         current_timestamp(), 'instore_orders_1.csv'),
        ('INS-002', CAST('2025-06-16 12:00:00' AS TIMESTAMP),
         'S001', 'bob@example.com',
         '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":3,"unit_price":19.99},{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]',
         'credit_card', CAST(84.96 AS DECIMAL(10,2)), 'Jane Doe',
         current_timestamp(), 'instore_orders_2.csv')
    """)

    return spark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_cell(spark, pattern):
    sql = find_cell(_W5_LAB, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    spark.sql(sql)


def _run_silver_stores(spark):
    _run_cell(spark, "silver_stores_merge")


def _run_silver_categories(spark):
    _run_cell(spark, "silver_categories_merge")


def _run_silver_books(spark):
    _run_cell(spark, "silver_books_merge")


def _run_silver_customers(spark):
    _run_cell(spark, "silver_customers_merge")


def _run_orders_unified_view(spark):
    _run_cell(spark, "silver_orders_unified_view")


def _run_silver_orders(spark):
    _run_orders_unified_view(spark)
    _run_cell(spark, "silver_orders_merge")


def _run_order_items_exploded_view(spark):
    _run_cell(spark, "silver_order_items_exploded_view")


def _run_silver_order_items(spark):
    _run_order_items_exploded_view(spark)
    _run_cell(spark, "silver_order_items_merge")


# ---------------------------------------------------------------------------
# Tests — silver.stores
# ---------------------------------------------------------------------------

def test_stores_merge(bronze_data):
    spark = bronze_data
    _run_silver_stores(spark)
    rows = spark.sql("SELECT * FROM silver.stores").collect()
    assert len(rows) == 1
    assert rows[0].store_nbr == "S001"
    assert rows[0].name == "Downtown Books"
    # Audit columns should not be present
    col_names = spark.sql("SELECT * FROM silver.stores").columns
    assert "ingestion_timestamp" not in col_names
    assert "source_filename" not in col_names


# ---------------------------------------------------------------------------
# Tests — silver.categories
# ---------------------------------------------------------------------------

def test_categories_merge(bronze_data):
    spark = bronze_data
    _run_silver_categories(spark)
    rows = spark.sql("SELECT * FROM silver.categories").collect()
    assert len(rows) == 3
    # Check self-referential hierarchy
    space_opera = [r for r in rows if r.category_id == "11"][0]
    assert space_opera.parent_category_id == "3"
    fiction = [r for r in rows if r.category_id == "1"][0]
    assert fiction.parent_category_id == ""


# ---------------------------------------------------------------------------
# Tests — silver.books
# ---------------------------------------------------------------------------

def test_books_filters_invalid_isbn(bronze_data):
    spark = bronze_data
    _run_silver_books(spark)
    rows = spark.sql("SELECT * FROM silver.books").collect()
    # Only 2 of 5 should pass: valid ISBN + non-empty title
    assert len(rows) == 2
    isbns = {r.isbn for r in rows}
    assert isbns == {"978-0-00-000001-1", "978-0-00-000002-2"}


def test_books_trims_whitespace(bronze_data):
    spark = bronze_data
    # Insert a book with leading/trailing whitespace
    spark.sql("""
        INSERT INTO bronze.books VALUES
        ('978-0-00-000099-9', '  Padded Title  ', '  Padded Author  ', ' 11 ',
         current_timestamp(), 'books.csv')
    """)
    _run_silver_books(spark)
    row = spark.sql(
        "SELECT * FROM silver.books WHERE isbn = '978-0-00-000099-9'"
    ).collect()
    assert len(row) == 1
    assert row[0].title == "Padded Title"
    assert row[0].author == "Padded Author"
    assert row[0].category_id == "11"


# ---------------------------------------------------------------------------
# Tests — silver.customers
# ---------------------------------------------------------------------------

def test_customers_takes_most_recent(bronze_data):
    spark = bronze_data
    _run_silver_customers(spark)
    rows = spark.sql("SELECT * FROM silver.customers").collect()
    assert len(rows) == 1
    alice = rows[0]
    assert alice.email == "alice@example.com"
    # Should have fields from the MORE RECENT order (ONL-002, 2025-07-15)
    assert alice.name == "Alice New"
    assert alice.address == "200 New Ave"
    assert alice.city == "NewCity"


# ---------------------------------------------------------------------------
# Tests — silver.orders (unified from online + instore)
# ---------------------------------------------------------------------------

def test_orders_unified(bronze_data):
    spark = bronze_data
    _run_silver_orders(spark)
    rows = spark.sql("SELECT * FROM silver.orders").collect()
    # 2 online + 2 instore = 4 total
    assert len(rows) == 4
    channels = {r.order_channel for r in rows}
    assert channels == {"online", "in-store"}


def test_orders_online_sentinel(bronze_data):
    spark = bronze_data
    _run_silver_orders(spark)
    online = spark.sql(
        "SELECT * FROM silver.orders WHERE order_channel = 'online'"
    ).collect()
    for row in online:
        assert row.store_nbr == "online"


def test_orders_instore_null_email_sentinel(bronze_data):
    spark = bronze_data
    _run_silver_orders(spark)
    ins001 = spark.sql(
        "SELECT * FROM silver.orders WHERE order_id = 'INS-001'"
    ).collect()
    assert len(ins001) == 1
    # NULL email should be replaced with 'in-store'
    assert ins001[0].customer_email == "in-store"

    # INS-002 had an actual email — should be preserved
    ins002 = spark.sql(
        "SELECT * FROM silver.orders WHERE order_id = 'INS-002'"
    ).collect()
    assert ins002[0].customer_email == "bob@example.com"


# ---------------------------------------------------------------------------
# Tests — silver.order_items (exploded from JSON)
# ---------------------------------------------------------------------------

def test_order_items_exploded(bronze_data):
    spark = bronze_data
    _run_silver_order_items(spark)
    rows = spark.sql("SELECT * FROM silver.order_items").collect()
    # ONL-001: 1 item, ONL-002: 1 item, INS-001: 1 item, INS-002: 2 items = 5
    assert len(rows) == 5


def test_order_items_drops_title(bronze_data):
    spark = bronze_data
    _run_silver_order_items(spark)
    cols = spark.sql("SELECT * FROM silver.order_items").columns
    assert "title" not in cols


def test_order_items_decimal_cast(bronze_data):
    spark = bronze_data
    _run_silver_order_items(spark)
    schema = spark.sql("SELECT * FROM silver.order_items").schema
    unit_price_field = [f for f in schema.fields if f.name == "unit_price"][0]
    assert "DecimalType" in str(unit_price_field.dataType)
