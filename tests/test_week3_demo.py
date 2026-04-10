"""Demo test file for Week 3 — illustrates the INSERT + verify pattern.

Both patterns: run an INSERT statement, then SELECT to verify the result, then assert.
"""

import os
import pytest
from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W3_DEMO = os.path.join(_REPO_ROOT, "labs", "week3", "week3_demo.ipynb")


# ===========================================================================
# Tests — Basic SQL query validation (DO MODIFY - implement these!)
# ===========================================================================

def test_valid_email_filter(spark):
    """Assert that only employees with a valid email are inserted into filtered_employees."""
    _run_cell(spark, "demo_valid_email_filter")
    rows = spark.sql("SELECT * FROM week3_testing.filtered_employees").collect()
    # TODO: assert len(rows) equals the number of employees whose email contains '@'


def test_insert_engineering_filter(spark):
    """Assert that only Engineering employees are inserted into filtered_employees."""
    _run_cell(spark, "demo_insert_engineering_filter")
    rows = spark.sql("SELECT * FROM week3_testing.filtered_employees").collect()
    # TODO: assert len(rows) equals the number of Engineering employees


# ===========================================================================
# DO NOT MODIFY ANYTHING BELOW THIS LINE
# ===========================================================================

def _run_cell(spark, pattern):
    sql = find_cell(_W3_DEMO, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    return spark.sql(sql)


@pytest.fixture(autouse=True)
def week3_test_data(spark):
    """Create the week3_testing schema and tables for demo tests."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS week3_testing")

    spark.sql("""
        CREATE OR REPLACE TABLE week3_testing.employees (
            employee_id STRING,
            name STRING,
            email STRING,
            department STRING,
            salary DECIMAL(10,2),
            hire_date DATE
        ) USING DELTA
    """)

    spark.sql("""
        INSERT INTO week3_testing.employees VALUES
        ('EMP-001', 'John Smith', 'john.smith@company.com', 'Engineering', 85000.00, DATE('2025-01-15')),
        ('EMP-002', 'Jane Doe', 'jane.doe@company.com', 'Sales', 65000.00, DATE('2024-03-20')),
        ('EMP-003', 'Bob Wilson', 'invalid-email', 'Marketing', 55000.00, DATE('2023-06-10')),
        ('EMP-004', 'Alice Johnson', 'alice.johnson@company.com', 'Engineering', 120000.00, DATE('2025-08-01')),
        ('EMP-005', 'Charlie Brown', '', 'HR', 45000.00, DATE('2020-05-12')),
        ('EMP-006', 'Diana Prince', NULL, 'Sales', 75000.00, DATE('2026-03-15'))
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE week3_testing.filtered_employees (
            employee_id STRING,
            name STRING,
            email STRING,
            department STRING,
            salary DECIMAL(10,2),
            hire_date DATE
        ) USING DELTA
    """)

    yield

    spark.sql("DROP TABLE IF EXISTS week3_testing.employees")
    spark.sql("DROP TABLE IF EXISTS week3_testing.filtered_employees")
