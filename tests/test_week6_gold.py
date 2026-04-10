"""Tests for Week 6 — Gold layer star schema."""

import os
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W6_LAB = os.path.join(_REPO_ROOT, "labs", "week6", "week6_lab.ipynb")


# ===========================================================================
# Tests — gold.dim_customer (DO MODIFY - implement these!)
# ===========================================================================

def test_dim_customer_from_silver(spark):
    _run_cell(spark, "gold_dim_customer_merge")
    alice = spark.sql("SELECT * FROM gold.dim_customer WHERE email = 'alice@example.com'").collect()
    bob = spark.sql("SELECT * FROM gold.dim_customer WHERE email = 'bob@example.com'").collect()
    # TODO: assert len(alice) equals 1 and len(bob) equals 1


def test_dim_customer_sentinel(spark):
    _run_cell(spark, "gold_dim_customer_merge")
    _run_cell(spark, "gold_dim_customer_sentinel")
    row = spark.sql("SELECT * FROM gold.dim_customer WHERE email = 'in-store'").collect()
    # row is a list of Row objects; row[0].name is a string
    # TODO: assert that row[0].name equals 'In-Store Customer'


# ---------------------------------------------------------------------------
# Tests — gold.dim_store
# ---------------------------------------------------------------------------

def test_dim_store_from_silver(spark):
    _run_cell(spark, "gold_dim_store_merge")
    store = spark.sql("SELECT * FROM gold.dim_store WHERE store_nbr = 'S001'").collect()
    # TODO: assert len(store) equals 1


def test_dim_store_sentinel(spark):
    _run_cell(spark, "gold_dim_store_merge")
    _run_cell(spark, "gold_dim_store_sentinel")
    row = spark.sql("SELECT * FROM gold.dim_store WHERE store_nbr = 'online'").collect()
    # row is a list of Row objects; row[0].name is a string
    # TODO: assert that row[0].name equals 'Online'


# ---------------------------------------------------------------------------
# Tests — gold.dim_book (hierarchy flattening)
# ---------------------------------------------------------------------------

def test_dim_book_flattens_hierarchy(spark):
    _run_cell(spark, "gold_dim_book_merge")
    book = spark.sql("SELECT * FROM gold.dim_book").collect()[0]
    # book is a Row object; .subgenre, .genre, .category are strings
    # TODO: assert that book.subgenre, book.genre, and book.category are correctly flattened:
    # subgenre = 'Space Opera', genre = 'Science Fiction', category = 'Fiction'


# ---------------------------------------------------------------------------
# Tests — gold.fact_sales
# ---------------------------------------------------------------------------

def test_fact_sales_all_items_present(spark):
    _run_cell(spark, "gold_fact_sales_merge")
    onl_001 = spark.sql("SELECT * FROM gold.fact_sales WHERE order_id = 'ONL-001'").collect()
    onl_002 = spark.sql("SELECT * FROM gold.fact_sales WHERE order_id = 'ONL-002'").collect()
    ins_001 = spark.sql("SELECT * FROM gold.fact_sales WHERE order_id = 'INS-001'").collect()
    ins_002 = spark.sql("SELECT * FROM gold.fact_sales WHERE order_id = 'INS-002'").collect()
    # TODO: assert len(onl_001), len(onl_002), len(ins_001) each equal 1, and len(ins_002) equals 2
    # (INS-002 had 2 line items, so it should produce 2 fact rows)


def test_fact_sales_line_total(spark):
    _run_cell(spark, "gold_fact_sales_merge")
    mismatches = spark.sql("""
        SELECT * FROM gold.fact_sales
        WHERE line_total != quantity * unit_price
    """).collect()
    # mismatches is a list of Row objects (you want it to have length 0)
    # TODO: assert that mismatches is empty (line_total should equal quantity * unit_price for every row)


def test_fact_sales_fk_lookups(spark):
    _run_cell(spark, "gold_fact_sales_merge")
    null_cust = spark.sql("SELECT * FROM gold.fact_sales WHERE customer_id IS NULL").collect()
    null_book = spark.sql("SELECT * FROM gold.fact_sales WHERE book_id IS NULL").collect()
    null_date = spark.sql("SELECT * FROM gold.fact_sales WHERE date_id IS NULL").collect()
    null_store = spark.sql("SELECT * FROM gold.fact_sales WHERE store_id IS NULL").collect()
    # TODO: assert len of each equals 0 (no fact rows should have NULL foreign keys)


def test_fact_sales_degenerate_dims(spark):
    _run_cell(spark, "gold_fact_sales_merge")
    online = spark.sql("SELECT * FROM gold.fact_sales WHERE order_channel = 'online'").collect()
    instore = spark.sql("SELECT * FROM gold.fact_sales WHERE order_channel = 'in-store'").collect()
    null_payment = spark.sql("SELECT * FROM gold.fact_sales WHERE payment_method IS NULL").collect()
    # TODO: assert len(online) > 0, len(instore) > 0, and len(null_payment) equals 0


# ===========================================================================
# DO NOT MODIFY ANYTHING BELOW THIS LINE
# ===========================================================================

def _run_cell(spark, pattern):
    sql = find_cell(_W6_LAB, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    spark.sql(sql)


@pytest.fixture(autouse=True)
def silver_data(spark):
    """Automatically populate silver tables for all gold tests.

    This fixture runs before every test in this module, inserting test
    data into all silver tables that gold transformations read from.
    """


    # --- silver.categories: 3-level hierarchy ---
    spark.sql("""
        INSERT INTO silver.categories VALUES
        ('1',  'Fiction',         ''),
        ('3',  'Science Fiction', '1'),
        ('11', 'Space Opera',     '3')
    """)

    # --- silver.stores ---
    spark.sql("""
        INSERT INTO silver.stores VALUES
        ('S001', 'Downtown Books', '100 Main St', 'Springfield', 'IL', '62701')
    """)

    # --- silver.books ---
    spark.sql("""
        INSERT INTO silver.books VALUES
        ('978-0-00-000001-1', 'Test Book One', 'Author A', '11'),
        ('978-0-00-000002-2', 'Test Book Two', 'Author B', '11')
    """)

    # --- silver.customers ---
    spark.sql("""
        INSERT INTO silver.customers VALUES
        ('alice@example.com', 'Alice Smith', '123 Elm St', 'Springfield', 'IL', '62701'),
        ('bob@example.com',   'Bob Jones',   '456 Oak Ave', 'Springfield', 'IL', '62702')
    """)

    # --- silver.orders ---
    spark.sql("""
        INSERT INTO silver.orders VALUES
        ('ONL-001', 'online',   CAST('2025-06-15 10:00:00' AS TIMESTAMP),
         'alice@example.com', 'online', 'credit_card', CAST(39.98 AS DECIMAL(10,2)), NULL),
        ('ONL-002', 'online',   CAST('2025-06-15 14:00:00' AS TIMESTAMP),
         'bob@example.com', 'online', 'debit_card', CAST(24.99 AS DECIMAL(10,2)), NULL),
        ('INS-001', 'in-store', CAST('2025-06-15 11:00:00' AS TIMESTAMP),
         'in-store', 'S001', 'cash', CAST(19.99 AS DECIMAL(10,2)), 'Bob Jones'),
        ('INS-002', 'in-store', CAST('2025-06-16 12:00:00' AS TIMESTAMP),
         'bob@example.com', 'S001', 'credit_card', CAST(84.96 AS DECIMAL(10,2)), 'Jane Doe')
    """)

    # --- silver.order_items ---
    spark.sql("""
        INSERT INTO silver.order_items VALUES
        ('ONL-001', 'online',   '978-0-00-000001-1', 2, CAST(19.99 AS DECIMAL(10,2))),
        ('ONL-002', 'online',   '978-0-00-000002-2', 1, CAST(24.99 AS DECIMAL(10,2))),
        ('INS-001', 'in-store', '978-0-00-000001-1', 1, CAST(19.99 AS DECIMAL(10,2))),
        ('INS-002', 'in-store', '978-0-00-000001-1', 3, CAST(19.99 AS DECIMAL(10,2))),
        ('INS-002', 'in-store', '978-0-00-000002-2', 1, CAST(24.99 AS DECIMAL(10,2)))
    """)



@pytest.fixture(autouse=True)
def gold_dims_populated(spark):
    """Automatically populate gold dimensions for fact table tests.

    This fixture runs after silver_data (also autouse), pre-populating
    gold dimensions with known surrogate key values so fact table tests
    can verify FK lookups work correctly.
    """


    # dim_customer with known IDs
    spark.sql("""
        INSERT INTO gold.dim_customer (customer_id, email, name, address, city, state, zip)
        VALUES
        (1, 'alice@example.com', 'Alice Smith', '123 Elm St', 'Springfield', 'IL', '62701'),
        (2, 'bob@example.com', 'Bob Jones', '456 Oak Ave', 'Springfield', 'IL', '62702'),
        (3, 'in-store', 'In-Store Customer', NULL, NULL, NULL, NULL)
    """)

    # dim_store with known IDs
    spark.sql("""
        INSERT INTO gold.dim_store (store_id, store_nbr, name, address, city, state, zip)
        VALUES
        (1, 'S001', 'Downtown Books', '100 Main St', 'Springfield', 'IL', '62701'),
        (2, 'online', 'Online', NULL, NULL, NULL, NULL)
    """)

    # dim_book with known IDs
    spark.sql("""
        INSERT INTO gold.dim_book (book_id, isbn, title, author, subgenre, genre, category)
        VALUES
        (1, '978-0-00-000001-1', 'Test Book One', 'Author A', 'Space Opera', 'Science Fiction', 'Fiction'),
        (2, '978-0-00-000002-2', 'Test Book Two', 'Author B', 'Space Opera', 'Science Fiction', 'Fiction')
    """)

    # dim_date — just the dates we need
    spark.sql("""
        INSERT INTO gold.dim_date (date_id, full_date, day_of_week, day_num_in_month,
            day_name, day_abbrev, weekday_flag, week_num_in_year, week_begin_date,
            week_begin_date_key, month, month_name, month_abbrev, quarter, year,
            yearmo, fiscal_month, fiscal_quarter, fiscal_year,
            last_day_in_month_flag, same_day_year_ago_date)
        VALUES
        (20250615, CAST('2025-06-15' AS DATE), 1, 15,
         'Sunday', 'Sun', 'N', 24, CAST('2025-06-15' AS DATE),
         20250615, 6, 'June', 'Jun', 2, 2025,
         202506, 6, 2, 2025,
         'N', CAST('2024-06-15' AS DATE)),
        (20250616, CAST('2025-06-16' AS DATE), 2, 16,
         'Monday', 'Mon', 'Y', 25, CAST('2025-06-16' AS DATE),
         20250616, 6, 'June', 'Jun', 2, 2025,
         202506, 6, 2, 2025,
         'N', CAST('2024-06-16' AS DATE))
    """)
