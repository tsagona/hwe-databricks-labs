"""Tests for Week 4 — Bronze layer ingestion."""

import os
import pytest

from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W4_LAB = os.path.join(_REPO_ROOT, "labs", "week4", "week4_lab.ipynb")


# ===========================================================================
# Tests — INSERT OVERWRITE (static reference data)
# (DO MODIFY - implement these!)
# ===========================================================================

def test_stores_insert_overwrite(spark):
    _run_cell(spark, "bronze_stores_load")
    downtown = spark.sql("SELECT * FROM bronze.stores WHERE name = 'Downtown Books'").collect()
    airport = spark.sql("SELECT * FROM bronze.stores WHERE name = 'Airport Books'").collect()
    # TODO: assert len(downtown) equals 1 and len(airport) equals 1


def test_categories_insert_overwrite(spark):
    _run_cell(spark, "bronze_categories_load")
    fiction = spark.sql("SELECT * FROM bronze.categories WHERE category_id = '1'").collect()
    sci_fi = spark.sql("SELECT * FROM bronze.categories WHERE category_id = '3'").collect()
    space_opera = spark.sql("SELECT * FROM bronze.categories WHERE category_id = '11'").collect()
    # TODO: assert len(fiction), len(sci_fi), and len(space_opera) each equal 1


def test_books_insert_overwrite(spark):
    _run_cell(spark, "bronze_books_load")
    nulls = spark.sql("""
        SELECT * FROM bronze.books
        WHERE ingestion_timestamp IS NULL OR source_filename IS NULL
    """).collect()
    # TODO: assert len(nulls) equals 0


# ---------------------------------------------------------------------------
# Tests — MERGE INTO (transactional data)
# ---------------------------------------------------------------------------

def test_online_orders_merge(spark):
    _run_cell(spark, "bronze_online_orders_merge")
    row = spark.sql("SELECT * FROM bronze.online_orders WHERE order_id = 'ONL-001'").collect()
    # row is a list of Row objects; row[0].customer_email is a string
    # TODO: assert that exactly one row exists for ONL-001 and it has the correct customer_email


def test_instore_orders_merge(spark):
    _run_cell(spark, "bronze_instore_orders_merge")
    row = spark.sql("SELECT * FROM bronze.instore_orders WHERE order_id = 'INS-001'").collect()
    # row is a list of Row objects; row[0].cashier_name is a string
    # TODO: assert that exactly one row exists for INS-001 and it has the correct cashier_name


def test_merge_is_idempotent(spark):
    _run_cell(spark, "bronze_online_orders_merge")
    rows_after_first = spark.sql("SELECT * FROM bronze.online_orders").collect()
    _run_cell(spark, "bronze_online_orders_merge")
    rows_after_second = spark.sql("SELECT * FROM bronze.online_orders").collect()
    # TODO: assert that len(rows_after_first) equals len(rows_after_second) (running MERGE twice should not add rows)


# ---------------------------------------------------------------------------
# Additional tests — data quality and schema validation
# ---------------------------------------------------------------------------

def test_categories_hierarchy_preserved(spark):
    _run_cell(spark, "bronze_categories_load")
    fiction = spark.sql("SELECT * FROM bronze.categories WHERE category_id = '1'").collect()[0]
    sci_fi = spark.sql("SELECT * FROM bronze.categories WHERE category_id = '3'").collect()[0]
    space_opera = spark.sql("SELECT * FROM bronze.categories WHERE category_id = '11'").collect()[0]
    # fiction, sci_fi, space_opera are Row objects; .parent_category_id is a string
    # TODO: assert the correct parent_category_id for each:
    # fiction (top-level) should have empty string, sci_fi should reference fiction, space_opera should reference sci_fi


def test_instore_orders_nullable_email(spark):
    _run_cell(spark, "bronze_instore_orders_merge")
    row = spark.sql("SELECT * FROM bronze.instore_orders").collect()
    # row is a list of Row objects; row[0].customer_email is None or a string
    # TODO: assert that row[0].customer_email is None (the test data has a NULL email that should be preserved in bronze)


def test_instore_orders_has_cashier_name(spark):
    _run_cell(spark, "bronze_instore_orders_merge")
    row = spark.sql("SELECT * FROM bronze.instore_orders").collect()
    # row is a list of Row objects; row[0].cashier_name is a string
    # TODO: assert that row[0].cashier_name equals the expected value


def test_books_preserves_category_reference(spark):
    _run_cell(spark, "bronze_books_load")
    wrong_category = spark.sql("SELECT * FROM bronze.books WHERE category_id != '11'").collect()
    # TODO: assert len(wrong_category) equals 0 (all books should reference category_id '11')


def test_merge_updates_existing_rows(spark):
    """Verify that MERGE UPDATE clause works when row already exists."""
    _run_cell(spark, "bronze_online_orders_merge")

    # Replace the source view with an updated version of ONL-001 (email changed)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW online_orders_raw AS
        SELECT
            'ONL-001' AS order_id,
            CAST('2025-06-15 10:00:00' AS TIMESTAMP) AS order_timestamp,
            'alice_updated_email@example.com' AS customer_email,
            'Alice Smith' AS customer_name,
            '123 Elm St' AS customer_address,
            'Springfield' AS customer_city,
            'IL' AS customer_state,
            '62701' AS customer_zip,
            '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":2,"unit_price":19.99}]' AS items,
            'credit_card' AS payment_method,
            CAST(99.99 AS DECIMAL(10,2)) AS total_amount,
            current_timestamp() AS ingestion_timestamp,
            'online_orders_1.csv' AS source_filename
    """)

    _run_cell(spark, "bronze_online_orders_merge")
    rows = spark.sql("SELECT * FROM bronze.online_orders").collect()
    # TODO: assert that len(rows) equals 1 (MERGE updated, not inserted) and rows[0].customer_email equals 'alice_updated_email@example.com'


# ===========================================================================
# DO NOT MODIFY ANYTHING BELOW THIS LINE
# ===========================================================================

def _run_cell(spark, pattern):
    sql = find_cell(_W4_LAB, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    spark.sql(sql)


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
