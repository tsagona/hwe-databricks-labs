# How to Write Your Own Tests

This guide walks you through writing pytest tests for your Databricks notebook transformations.

## Quick Start

**To test a notebook transformation, you need:**
1. Tag the SQL cell in your notebook with `-- @test:tag_name`
2. Create a fixture to set up test data
3. Write a test function that runs the cell and validates results

## Table of Contents
- [Step-by-Step Example](#step-by-step-example)
- [Tagging Cells in Notebooks](#tagging-cells-in-notebooks)
- [Creating Test Data Fixtures](#creating-test-data-fixtures)
- [Writing Test Functions](#writing-test-functions)
- [Running Tests](#running-tests)
- [Common Patterns](#common-patterns)
- [Troubleshooting](#troubleshooting)

---

## Step-by-Step Example

Let's write a test for a bronze layer transformation that loads stores data.

### Step 1: Tag the Cell in Your Notebook

In your `week4_lab.ipynb`, add a tag comment as the **first line** of the cell:

```sql
-- @test:bronze_stores_load
INSERT OVERWRITE bronze.stores (store_nbr, name, address, city, state, zip, ingestion_timestamp, source_filename)
SELECT
  store_nbr,
  name,
  address,
  city,
  state,
  zip,
  ingestion_timestamp,
  source_filename
FROM stores_raw
```

The tag `-- @test:bronze_stores_load` lets your test find this cell.

### Step 2: Create a Fixture for Test Data

In `tests/test_week4_bronze.py`, create a fixture that sets up test data:

```python
import pytest

@pytest.fixture(autouse=True)
def bronze_source_data(spark):
    """Automatically create all bronze source temp views for every test.

    This fixture runs before every test in this module, creating all
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
    # ... (other temp views)
```

**Key points:**
- Uses `autouse=True` to run automatically before every test
- Takes `spark` as a parameter (the SparkSession)
- Uses SQL to create test data
- Doesn't return anything (setup-only)

### Step 3: Write the Test Function

```python
from tests.notebook_utils import find_cell

# Path to your notebook
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W4_LAB = os.path.join(_REPO_ROOT, "labs", "week4", "week4_lab.ipynb")

def test_stores_insert_overwrite(spark):
    # Find the tagged cell
    sql = find_cell(_W4_LAB, "bronze_stores_load")

    # Run it
    spark.sql(sql)

    # Validate results
    rows = spark.sql("SELECT * FROM bronze.stores").collect()
    assert len(rows) == 2

    names = {r.name for r in rows}
    assert names == {"Downtown Books", "Airport Books"}
```

**Key points:**
- Test only takes `spark` parameter - no fixture parameters needed!
- The autouse fixture runs automatically before the test
- Use `find_cell()` to extract SQL from the notebook
- Run the SQL with `spark.sql()`
- Validate results with assertions

### Step 4: Run the Test

```bash
pytest tests/test_week4_bronze.py::test_stores_insert_overwrite -v
```

---

## Tagging Cells in Notebooks

### Tag Format

Tags **must** be the first line of a code cell:

```sql
-- @test:tag_name
SELECT * FROM my_table
```

### Tag Naming Conventions

Use descriptive tags that indicate **what** the cell does:

**Good tags:**
- `-- @test:bronze_stores_load`
- `-- @test:silver_customers_merge`
- `-- @test:gold_fact_sales_merge`

**Bad tags:**
- `-- @test:query1` (not descriptive)
- `-- @test:test` (redundant)
- `--@test:my_tag` (missing space after `--`)

### Multiple Tagged Cells

You can tag multiple cells in the same notebook:

```sql
-- @test:bronze_books_load
INSERT OVERWRITE bronze.books
SELECT * FROM books_raw
```

```sql
-- @test:bronze_categories_load
INSERT OVERWRITE bronze.categories
SELECT * FROM categories_raw
```

Each tag should be unique within the notebook.

---

## Creating Test Data Fixtures

Fixtures set up data before tests run. They use SQL exclusively and **`autouse=True`** to run automatically.

### Pattern 1: Creating Temporary Views (Week 4)

Use for mocking CSV source data:

```python
@pytest.fixture(autouse=True)
def bronze_source_data(spark):
    """Automatically create all bronze source temp views for every test.

    This fixture runs before every test in this module, creating all
    source CSV temp views that bronze transformations read from.
    """
    # books_raw
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW books_raw AS
        SELECT '978-0-00-000001-1' AS isbn,
               'Test Book One' AS title,
               'Author A' AS author,
               '11' AS category_id,
               current_timestamp() AS ingestion_timestamp,
               'books.csv' AS source_filename
        UNION ALL
        SELECT '978-0-00-000002-2',
               'Test Book Two',
               'Author B',
               '11',
               current_timestamp(),
               'books.csv'
    """)
    # ... (other temp views)
```

**When to use:**
- Mocking CSV files
- Creating temporary views
- Week 4 bronze layer tests

**Key point:** Use `autouse=True` so the fixture runs automatically before every test.

### Pattern 2: Inserting Into Tables (Week 5/6)

Use for populating actual tables:

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

    spark.sql("""
        INSERT INTO bronze.books VALUES
        ('978-0-00-000001-1', 'Test Book One', 'Author A', '11',
         current_timestamp(), 'books.csv')
    """)
```

**When to use:**
- Populating bronze/silver/gold tables
- Week 5 and 6 tests
- When you need data across multiple tables

### Handling NULL Values

Use `CAST(NULL AS type)` for nullable columns:

```python
@pytest.fixture(autouse=True)
def bronze_source_data(spark):
    """Automatically create bronze source temp views for every test."""
    # instore_orders_raw with NULL email
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW instore_orders_raw AS
        SELECT
            'INS-001' AS order_id,
            CAST('2025-06-15 11:00:00' AS TIMESTAMP) AS transaction_timestamp,
            'S001' AS store_nbr,
            CAST(NULL AS STRING) AS customer_email,  -- NULL value
            'cash' AS payment_method
    """)
```

### Type Casting

Always cast types explicitly for precision:

```sql
-- Decimals (for money)
CAST(99.99 AS DECIMAL(10,2)) AS total_amount

-- Timestamps
CAST('2025-06-15 10:00:00' AS TIMESTAMP) AS order_date

-- Dates
CAST('2025-06-15' AS DATE) AS order_date

-- Use current_timestamp() for audit columns
current_timestamp() AS ingestion_timestamp
```

---

## Writing Test Functions

### Basic Test Structure

```python
def test_descriptive_name(spark):
    # Autouse fixtures have already run - data is ready!

    # 1. Find and run the tagged cell
    sql = find_cell(NOTEBOOK_PATH, "tag_name")
    spark.sql(sql)

    # 2. Query results
    rows = spark.sql("SELECT * FROM target_table").collect()

    # 3. Validate with assertions
    assert len(rows) == expected_count
    assert rows[0].column_name == expected_value
```

### Assertions

#### Check Row Count
```python
rows = spark.sql("SELECT * FROM bronze.stores").collect()
assert len(rows) == 2
```

#### Check Column Values
```python
assert rows[0].name == "Downtown Books"
assert rows[0].store_nbr == "S001"
```

#### Check Sets (Order Independent)
```python
names = {r.name for r in rows}
assert names == {"Downtown Books", "Airport Books"}
```

#### Check for NULL
```python
assert rows[0].customer_email is None
```

#### Check Decimals
```python
from decimal import Decimal
assert rows[0].total_amount == Decimal("99.99")
```

#### Check Column Existence
```python
cols = spark.sql("SELECT * FROM bronze.stores").columns
assert "ingestion_timestamp" in cols
assert "source_filename" in cols
```

#### Check No Rows Match (For Validation)
```python
bad_rows = spark.sql("""
    SELECT * FROM silver.books WHERE isbn = 'BADISBN'
""").collect()
assert len(bad_rows) == 0
```

### Testing Idempotency

Test that running the same transformation twice produces the same result:

```python
def test_merge_is_idempotent(spark):
    # Autouse fixture already created bronze source temp views

    # Run MERGE once
    sql = find_cell(_W4_LAB, "bronze_online_orders_merge")
    spark.sql(sql)
    count_first = spark.sql("SELECT COUNT(*) AS cnt FROM bronze.online_orders").collect()[0].cnt

    # Run MERGE again
    spark.sql(sql)
    count_second = spark.sql("SELECT COUNT(*) AS cnt FROM bronze.online_orders").collect()[0].cnt

    # Should be the same
    assert count_first == count_second
```

### Testing Data Quality

```python
def test_books_filters_invalid_isbn(spark):
    """Verify that books with non-ISBN-13 format are rejected."""
    # Autouse fixture already populated bronze.books with test data

    sql = find_cell(_W5_LAB, "silver_books_merge")
    spark.sql(sql)

    # No books with bad ISBN should exist
    bad_isbns = spark.sql("""
        SELECT * FROM silver.books WHERE isbn = 'BADISBN'
    """).collect()
    assert len(bad_isbns) == 0
```

### Testing Referential Integrity

```python
def test_books_category_fk_valid(spark):
    """Verify all books reference valid categories."""
    # Autouse fixture already populated bronze tables

    # Run both transformations
    spark.sql(find_cell(_W5_LAB, "silver_categories_merge"))
    spark.sql(find_cell(_W5_LAB, "silver_books_merge"))

    # Find orphaned books (no matching category)
    invalid = spark.sql("""
        SELECT b.isbn, b.category_id
        FROM silver.books b
        LEFT JOIN silver.categories c ON b.category_id = c.category_id
        WHERE c.category_id IS NULL
    """).collect()

    assert len(invalid) == 0
```

---

## Running Tests

### Run All Tests
```bash
pytest tests/
```

### Run Tests for a Specific Week
```bash
pytest tests/test_week4_bronze.py -v
pytest tests/test_week5_silver.py -v
pytest tests/test_week6_gold.py -v
```

### Run a Single Test
```bash
pytest tests/test_week4_bronze.py::test_stores_insert_overwrite -v
```

### Run Tests Matching a Pattern
```bash
# Run all tests with "merge" in the name
pytest tests/ -k merge -v

# Run all tests with "idempotent" in the name
pytest tests/ -k idempotent -v
```

### Show Detailed Output
```bash
# Show print statements
pytest tests/test_week4_bronze.py -v -s

# Show short traceback on failures
pytest tests/test_week4_bronze.py -v --tb=short

# Show full traceback on failures
pytest tests/test_week4_bronze.py -v --tb=long
```

### Stop on First Failure
```bash
pytest tests/ -x
```

---

## Common Patterns

### Pattern: Test Multiple Rows

```python
def test_categories_hierarchy_preserved(spark):
    # Autouse fixture already created all bronze source temp views
    sql = find_cell(_W4_LAB, "bronze_categories_load")
    spark.sql(sql)

    rows = spark.sql("""
        SELECT category_id, category_name, parent_category_id
        FROM bronze.categories
        ORDER BY category_id
    """).collect()

    # Verify hierarchy levels
    fiction = [r for r in rows if r.category_id == "1"][0]
    assert fiction.parent_category_id == ""

    sci_fi = [r for r in rows if r.category_id == "3"][0]
    assert sci_fi.parent_category_id == "1"

    space_opera = [r for r in rows if r.category_id == "11"][0]
    assert space_opera.parent_category_id == "3"
```

### Pattern: Test Schema/Types

```python
def test_online_orders_decimal_precision(spark):
    # Autouse fixture already created online_orders_raw temp view
    sql = find_cell(_W4_LAB, "bronze_online_orders_merge")
    spark.sql(sql)

    schema = spark.sql("SELECT * FROM bronze.online_orders").schema
    total_amount_field = [f for f in schema.fields if f.name == "total_amount"][0]
    assert "DecimalType" in str(total_amount_field.dataType)
```

### Pattern: Test Aggregates

```python
def test_customers_no_duplicates(spark):
    """Verify that each email appears exactly once."""
    # Autouse fixture already populated bronze tables with silver test data
    sql = find_cell(_W5_LAB, "silver_customers_merge")
    spark.sql(sql)

    duplicates = spark.sql("""
        SELECT email, COUNT(*) as cnt
        FROM silver.customers
        GROUP BY email
        HAVING COUNT(*) > 1
    """).collect()

    assert len(duplicates) == 0
```

### Pattern: Test Multiple Transformations in Order

```python
def test_fact_sales_joins_all_dimensions(spark):
    """Verify every fact row joins to all four dimensions."""
    # Autouse fixture already populated silver tables

    # Run all dimension loads first
    spark.sql(find_cell(_W6_LAB, "gold_dim_customer_merge"))
    spark.sql(find_cell(_W6_LAB, "gold_dim_store_merge"))
    spark.sql(find_cell(_W6_LAB, "gold_dim_book_merge"))

    # Then run fact load
    spark.sql(find_cell(_W6_LAB, "gold_fact_sales_merge"))

    # Verify all joins succeed
    result = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM gold.fact_sales f
        JOIN gold.dim_customer c ON f.customer_id = c.customer_id
        JOIN gold.dim_book b ON f.book_id = b.book_id
        JOIN gold.dim_store s ON f.store_id = s.store_id
    """).collect()[0].cnt

    fact_count = spark.sql("SELECT COUNT(*) AS cnt FROM gold.fact_sales").collect()[0].cnt
    assert result == fact_count
```

---

## Troubleshooting

### Error: "Could not find cell matching: tag_name"

**Problem:** `find_cell()` can't find the tagged cell.

**Solutions:**
1. Check the tag is exactly `-- @test:tag_name` (space after `--`)
2. Verify the tag is the **first line** of the cell
3. Make sure the tag matches what you're searching for
4. Check you're looking in the correct notebook file

### Error: "Table or view not found: my_table"

**Problem:** Your test is trying to query a table that doesn't exist.

**Solutions:**
1. Make sure your fixture has `autouse=True` so it runs automatically
2. Verify the fixture creates the table/view correctly
3. Check table names match exactly (case-sensitive)
4. Run the fixture's SQL manually to debug

### Error: "AssertionError: assert 0 == 2"

**Problem:** Your test expected 2 rows but got 0.

**Solutions:**
1. Print the actual results to see what you got: `print(rows)`
2. Run the fixture SQL manually to verify data is created
3. Run the transformation SQL manually to see what happens
4. Check if a WHERE clause is filtering out your test data

### Tests Pass Individually But Fail Together

**Problem:** Tests work alone but fail when run together.

**Solutions:**
1. Tests might be interfering with each other
2. Make sure each test uses fresh fixtures (function-scoped)
3. Check tests aren't modifying shared data
4. Verify the `spark` fixture tears down tables properly

### Slow Tests

**Problem:** Tests take a long time to run.

**Solutions:**
1. Use smaller test datasets (fewer rows)
2. Make sure you're using `spark` fixture (not creating new sessions)
3. Consider marking slow tests with `@pytest.mark.slow`
4. Run fast tests during development, all tests in CI

### Decimal Comparison Fails

**Problem:** `assert row.amount == 99.99` fails even though values look the same.

**Solution:** Use `Decimal` for exact comparisons:
```python
from decimal import Decimal
assert row.amount == Decimal("99.99")
```

---

## Tips and Best Practices

### ✅ Do

- **Tag cells clearly** with descriptive names
- **Use SQL** for all test data setup
- **Test one thing** per test function
- **Use descriptive test names** like `test_books_filters_invalid_isbn`
- **Add docstrings** to tests explaining what they verify
- **Test edge cases**: NULLs, empty strings, duplicates, invalid data
- **Test idempotency** for MERGE operations
- **Check both positive and negative cases**

### ❌ Don't

- **Don't use Python DataFrames** for test data (use SQL)
- **Don't create Row objects** or StructFields (use SQL)
- **Don't test implementation details** (test outcomes, not how)
- **Don't make tests depend on each other** (each should be independent)
- **Don't use magic numbers** (explain what values mean)
- **Don't skip assertions** (every test should assert something)

---

## Example: Complete Test File

Here's a minimal but complete test file:

```python
"""Tests for Week 4 — Bronze layer ingestion."""

import os
from decimal import Decimal
import pytest
from tests.notebook_utils import find_cell

# Path to notebook
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W4_LAB = os.path.join(_REPO_ROOT, "labs", "week4", "week4_lab.ipynb")


# --- Fixtures ---

@pytest.fixture(autouse=True)
def bronze_source_data(spark):
    """Automatically create all bronze source temp views for every test.

    This fixture runs before every test in this module, creating all
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
    """)
    # ... (other temp views)


# --- Tests ---

def test_stores_insert_overwrite(spark):
    """Verify stores are loaded correctly from temp view."""
    # Autouse fixture already created stores_raw temp view

    # Run the transformation
    sql = find_cell(_W4_LAB, "bronze_stores_load")
    spark.sql(sql)

    # Validate results
    rows = spark.sql("SELECT * FROM bronze.stores").collect()
    assert len(rows) == 1
    assert rows[0].name == "Downtown Books"
    assert rows[0].ingestion_timestamp is not None


def test_stores_has_audit_columns(spark):
    """Verify audit columns are present."""
    # Autouse fixture already created stores_raw temp view

    sql = find_cell(_W4_LAB, "bronze_stores_load")
    spark.sql(sql)

    cols = spark.sql("SELECT * FROM bronze.stores").columns
    assert "ingestion_timestamp" in cols
    assert "source_filename" in cols
```

---

## Getting Help

- Check existing tests in `tests/test_week*.py` for examples
- Read `tests/README.md` for framework overview
- Ask questions about specific errors with code snippets
- Review pytest documentation: https://docs.pytest.org/

Happy testing! 🧪
