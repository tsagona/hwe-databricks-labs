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


# ===========================================================================
# Helpers
# ===========================================================================

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


# ===========================================================================
# Tests — silver.stores (DO MODIFY - implement these!)
# ===========================================================================

def test_stores_merge(spark):
    # TODO: Implement this test
    pass

# ---------------------------------------------------------------------------
# Tests — silver.categories
# ---------------------------------------------------------------------------

def test_categories_merge(spark):
    # TODO: Implement this test
    pass

# ---------------------------------------------------------------------------
# Tests — silver.books
# ---------------------------------------------------------------------------

def test_books_filters_invalid_isbn(spark):
    # TODO: Implement this test
    pass

def test_books_trims_whitespace(spark):
    # TODO: Implement this test
    pass

# ---------------------------------------------------------------------------
# Tests — silver.customers
# ---------------------------------------------------------------------------

def test_customers_takes_most_recent(spark):
    # TODO: Implement this test
    pass

# ---------------------------------------------------------------------------
# Tests — silver.orders (unified from online + instore)
# ---------------------------------------------------------------------------

def test_orders_unified(spark):
    # TODO: Implement this test
    pass

def test_orders_online_sentinel(spark):
    # TODO: Implement this test
    pass

def test_orders_instore_null_email_sentinel(spark):
    # TODO: Implement this test
    pass

# ---------------------------------------------------------------------------
# Tests — silver.order_items (exploded from JSON)
# ---------------------------------------------------------------------------

def test_order_items_exploded(spark):
    # TODO: Implement this test
    pass

def test_order_items_drops_title(spark):
    # TODO: Implement this test
    pass

def test_order_items_decimal_cast(spark):
    # TODO: Implement this test
    pass


# ===========================================================================
# Test fixtures — populate bronze tables with test data
# (COMPLETE - Do not modify)
# ===========================================================================

@pytest.fixture(autouse=True)
def bronze_data(spark):
    """Automatically populate bronze tables for all silver tests.

    This fixture runs before every test in this module, inserting test
    data into all bronze tables that silver transformations read from.
    """


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
