# Test Framework Guide

## Overview

This test framework validates the SQL transformations in the lab notebooks (weeks 4-6) using pytest and local Spark.

## How It Works

1. **Tag cells in notebooks** with `-- @test:tag_name`
2. **Write test functions** that extract and run those cells
3. **Use autouse fixtures** to automatically set up test data

## Creating Test Data with SQL and Autouse Fixtures

All test fixtures use **`autouse=True`** - they run automatically before every test without needing to be explicitly referenced in test parameters.

### Creating Temporary Views (Week 4 Pattern)

Week 4 tests mock CSV source files by creating temporary views:

```python
@pytest.fixture(autouse=True)
def bronze_source_data(spark):
    """Automatically create all bronze source temp views for every test.

    This fixture runs before every test in this module, creating all 5
    source CSV temp views that bronze transformations read from.
    """
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW stores_raw AS
        SELECT 'S001' AS store_nbr, 'Downtown Books' AS name,
               '100 Main St' AS address, 'Springfield' AS city,
               'IL' AS state, '62701' AS zip,
               current_timestamp() AS ingestion_timestamp,
               'stores.csv' AS source_filename
        UNION ALL
        SELECT 'S002', 'Airport Books', '200 Terminal Dr',
               'Springfield', 'IL', '62702',
               current_timestamp(), 'stores.csv'
    """)
    # No return statement - just sets up the temp views
```

### Inserting Into Tables (Week 5/6 Pattern)

Week 5 and 6 tests populate actual tables using SQL INSERT:

```python
@pytest.fixture(autouse=True)
def bronze_data(spark):
    """Automatically populate bronze tables for all silver tests.

    This fixture runs before every test in this module, inserting test
    data into all bronze tables that silver transformations read from.
    """
    spark.sql("""
        INSERT INTO bronze.categories VALUES
        ('1',  'Fiction',         '',  current_timestamp(), 'categories.csv'),
        ('3',  'Science Fiction', '1', current_timestamp(), 'categories.csv')
    """)
    # No return statement - just populates tables
```

### Handling NULL Values

Use `CAST(NULL AS type)` for nullable columns:

```python
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW orders_raw AS
    SELECT
        'INS-001' AS order_id,
        CAST(NULL AS STRING) AS customer_email,  -- NULL email
        'cash' AS payment_method
""")
```

### Type Casting

Always cast types explicitly for precision:

```python
# Decimals
CAST(99.99 AS DECIMAL(10,2)) AS total_amount

# Timestamps
CAST('2025-06-15 10:00:00' AS TIMESTAMP) AS order_date

# Dates
CAST('2025-06-15' AS DATE) AS order_date

# Use current_timestamp() for audit columns
current_timestamp() AS ingestion_timestamp
```

## Example: Week 4 Bronze Tests

### Creating Temp Views for Multiple Rows

```python
@pytest.fixture(autouse=True)
def bronze_source_data(spark):
    """Automatically create all bronze source temp views for every test.

    This fixture runs before every test in this module, creating all 5
    source CSV temp views that bronze transformations read from.
    """
    # stores_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW stores_raw AS
        SELECT 'S001' AS store_nbr, 'Downtown Books' AS name,
               '100 Main St' AS address, 'Springfield' AS city,
               'IL' AS state, '62701' AS zip,
               current_timestamp() AS ingestion_timestamp,
               'stores.csv' AS source_filename
        UNION ALL
        SELECT 'S002', 'Airport Books', '200 Terminal Dr',
               'Springfield', 'IL', '62702',
               current_timestamp(), 'stores.csv'
    """)

    # books_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW books_raw AS
        SELECT '978-0-00-000001-1' AS isbn, 'Test Book One' AS title,
               'Author A' AS author, '11' AS category_id,
               current_timestamp() AS ingestion_timestamp,
               'books.csv' AS source_filename
        UNION ALL
        SELECT '978-0-00-000002-2', 'Test Book Two',
               'Author B', '11',
               current_timestamp(), 'books.csv'
    """)
    # ... (remaining views)
```

### Handling NULL Values

NULL values are handled cleanly with SQL CAST:

```python
# Inside autouse fixture - instore_orders_raw temp view
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW instore_orders_raw AS
    SELECT
        'INS-001' AS order_id,
        CAST('2025-06-15 11:00:00' AS TIMESTAMP) AS transaction_timestamp,
        'S001' AS store_nbr,
        CAST(NULL AS STRING) AS customer_email,  -- NULL handled cleanly
        '[{"isbn":"978-0-00-000002-2","quantity":1,"unit_price":24.99}]' AS items,
        'cash' AS payment_method,
        CAST(24.99 AS DECIMAL(10,2)) AS total_amount,
        'Bob Jones' AS cashier_name,
        current_timestamp() AS ingestion_timestamp,
        'instore_orders_1.csv' AS source_filename
""")
```

## Example: Week 5 Silver Tests

Week 5 uses SQL INSERT for populating bronze tables:

```python
@pytest.fixture(autouse=True)
def bronze_data(spark):
    """Automatically populate bronze tables for all silver tests.

    This fixture runs before every test in this module, inserting test
    data into all bronze tables that silver transformations read from.
    """
    # SQL INSERT - clean and readable
    spark.sql("""
        INSERT INTO bronze.categories VALUES
        ('1',  'Fiction',         '',  current_timestamp(), 'categories.csv'),
        ('3',  'Science Fiction', '1', current_timestamp(), 'categories.csv'),
        ('11', 'Space Opera',     '3', current_timestamp(), 'categories.csv')
    """)

    spark.sql("""
        INSERT INTO bronze.books VALUES
        ('978-0-00-000001-1', 'Test Book One', 'Author A', '11',
         current_timestamp(), 'books.csv')
    """)
```

## Writing Test Functions

With autouse fixtures, tests only need the `spark` parameter:

```python
def test_stores_insert_overwrite(spark):
    # bronze_source_data fixture already ran automatically!
    # All temp views are ready to use

    # Find and run the cell tagged with @test:bronze_stores_load
    sql = find_cell(_W4_LAB, "bronze_stores_load")
    spark.sql(sql)

    # Validate results
    rows = spark.sql("SELECT * FROM bronze.stores").collect()
    assert len(rows) == 2
    names = {r.name for r in rows}
    assert names == {"Downtown Books", "Airport Books"}
```

**Key points:**
- Tests only need `(spark)` parameter - no fixture parameters!
- Fixtures with `autouse=True` run automatically before every test
- No need to explicitly reference fixtures in test signatures
- Crystal clear and simple - just focus on the test logic!

## Summary: What Students DON'T Need

❌ **No Row objects:** `Row(name="Alice", age=30)`
❌ **No StructType/StructField:** Explicit schema definitions
❌ **No DataFrames in Python:** `spark.createDataFrame(data)`
❌ **No manual type casting in Python:** `.withColumn("amount", F.col("amount").cast(...))`
❌ **No datetime imports:** All dates/times use SQL CAST

## Summary: What Students DO Use

✅ **SQL CREATE VIEW:** `CREATE TEMPORARY VIEW ... AS SELECT ...`
✅ **SQL INSERT:** `INSERT INTO table VALUES (...)`
✅ **SQL CAST for types:** `CAST(99.99 AS DECIMAL(10,2))`
✅ **SQL UNION ALL:** For multi-row temp views
✅ **SQL NULL handling:** `CAST(NULL AS STRING)`
✅ **Decimal for assertions:** `assert row.amount == Decimal("99.99")`

## Running Tests

```bash
# Run all tests
pytest tests/

# Run specific week
pytest tests/test_week4_bronze.py -v

# Run single test
pytest tests/test_week4_bronze.py::test_stores_insert_overwrite -v

# Show detailed output
pytest tests/test_week5_silver.py -v --tb=short
```

## Test Coverage Summary

- **Week 4 (13 tests)**: Bronze layer ingestion, MERGE idempotency, audit columns
- **Week 5 (11 tests)**: Silver transformations, data quality, referential integrity
- **Week 6 (9 tests)**: Gold star schema, surrogate keys, dimension/fact validation

Total: **33 comprehensive tests** validating the entire medallion architecture!
