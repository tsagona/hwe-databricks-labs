"""Tests for Week 4 — Bronze layer ingestion."""

import os
from decimal import Decimal

import pytest

from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W4_LAB = os.path.join(_REPO_ROOT, "labs", "week4", "week4_lab.ipynb")


# ===========================================================================
# Helper to run a SQL cell from the notebook
# ===========================================================================

def _run_cell(spark, pattern):
    sql = find_cell(_W4_LAB, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    spark.sql(sql)


# ===========================================================================
# Tests — INSERT OVERWRITE (static reference data)
# (DO MODIFY - implement these!)
# ===========================================================================

def test_stores_insert_overwrite(spark):
    # TODO: Implement this test
    pass

def test_categories_insert_overwrite(spark):
    # TODO: Implement this test
    pass

def test_books_insert_overwrite(spark):
    # TODO: Implement this test
    pass

# ---------------------------------------------------------------------------
# Tests — MERGE INTO (transactional data)
# ---------------------------------------------------------------------------

def test_online_orders_merge(spark):
    # TODO: Implement this test
    pass

def test_instore_orders_merge(spark):
    # TODO: Implement this test
    pass

def test_merge_is_idempotent(spark):
    # TODO: Implement this test
    pass

# ---------------------------------------------------------------------------
# Additional tests — data quality and schema validation
# ---------------------------------------------------------------------------

def test_stores_has_audit_columns(spark):
    # TODO: Implement this test
    pass

def test_categories_hierarchy_preserved(spark):
    # TODO: Implement this test
    pass

def test_online_orders_decimal_precision(spark):
    # TODO: Implement this test
    pass

def test_instore_orders_nullable_email(spark):
    # TODO: Implement this test
    pass

def test_instore_orders_has_cashier_name(spark):
    # TODO: Implement this test
    pass

def test_books_preserves_category_reference(spark):
    # TODO: Implement this test
    pass

def test_merge_updates_existing_rows(spark):
    """Verify that MERGE UPDATE clause works when row already exists."""
    # TODO: Implement this test
    pass


# ===========================================================================
# Test fixtures — automatically create all source temp views
# (COMPLETE - Do not modify)
# ===========================================================================

@pytest.fixture(autouse=True)
def bronze_source_data(spark):
    """Automatically create all bronze source temp views for every test.

    This fixture runs before every test in this module, creating all 5
    source CSV temp views that bronze transformations read from.
    """
    # stores_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW stores_raw AS
        SELECT 'S001' AS store_nbr, 'Downtown Books' AS name, '100 Main St' AS address,
               'Springfield' AS city, 'IL' AS state, '62701' AS zip,
               current_timestamp() AS ingestion_timestamp, 'stores.csv' AS source_filename
        UNION ALL
        SELECT 'S002', 'Airport Books', '200 Terminal Dr',
               'Springfield', 'IL', '62702',
               current_timestamp(), 'stores.csv'
    """)

    # categories_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW categories_raw AS
        SELECT '1' AS category_id, 'Fiction' AS category_name, '' AS parent_category_id,
               current_timestamp() AS ingestion_timestamp, 'categories.csv' AS source_filename
        UNION ALL
        SELECT '3', 'Science Fiction', '1', current_timestamp(), 'categories.csv'
        UNION ALL
        SELECT '11', 'Space Opera', '3', current_timestamp(), 'categories.csv'
    """)

    # books_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW books_raw AS
        SELECT '978-0-00-000001-1' AS isbn, 'Test Book One' AS title,
               'Author A' AS author, '11' AS category_id,
               current_timestamp() AS ingestion_timestamp, 'books.csv' AS source_filename
        UNION ALL
        SELECT '978-0-00-000002-2', 'Test Book Two',
               'Author B', '11',
               current_timestamp(), 'books.csv'
    """)

    # online_orders_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW online_orders_raw AS
        SELECT
            'ONL-001' AS order_id,
            CAST('2025-06-15 10:00:00' AS TIMESTAMP) AS order_timestamp,
            'alice@example.com' AS customer_email,
            'Alice Smith' AS customer_name,
            '123 Elm St' AS customer_address,
            'Springfield' AS customer_city,
            'IL' AS customer_state,
            '62701' AS customer_zip,
            '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":2,"unit_price":19.99}]' AS items,
            'credit_card' AS payment_method,
            CAST(39.98 AS DECIMAL(10,2)) AS total_amount,
            current_timestamp() AS ingestion_timestamp,
            'online_orders_1.csv' AS source_filename
    """)

    # instore_orders_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW instore_orders_raw AS
        SELECT
            'INS-001' AS order_id,
            CAST('2025-06-15 11:00:00' AS TIMESTAMP) AS transaction_timestamp,
            'S001' AS store_nbr,
            CAST(NULL AS STRING) AS customer_email,
            '[{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]' AS items,
            'cash' AS payment_method,
            CAST(24.99 AS DECIMAL(10,2)) AS total_amount,
            'Bob Jones' AS cashier_name,
            current_timestamp() AS ingestion_timestamp,
            'instore_orders_1.csv' AS source_filename
    """)
