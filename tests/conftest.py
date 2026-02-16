"""Shared pytest fixtures for notebook SQL tests."""

import os
import re
import shutil
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from tests.notebook_utils import get_all_sql_cells, strip_identity

# Paths to DDL notebooks (relative to repo root)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BRONZE_DDL = os.path.join(_REPO_ROOT, "labs", "week4", "create_bronze.ipynb")
_SILVER_DDL = os.path.join(_REPO_ROOT, "labs", "week5", "create_silver.ipynb")
_GOLD_DDL = os.path.join(_REPO_ROOT, "labs", "week6", "create_gold.ipynb")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Session-scoped SparkSession with Delta Lake configured."""
    warehouse_dir = str(tmp_path_factory.mktemp("warehouse"))
    derby_dir = str(tmp_path_factory.mktemp("derby"))

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("notebook-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture()
def schemas(spark):
    """Create bronze/silver/gold schemas and tables, tear down after each test.

    Extracts DDL from the create_*.ipynb notebooks and runs it. For gold
    tables, strips GENERATED ALWAYS AS IDENTITY so they work in local Spark.
    """
    # Create schemas
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

    # Run DDL from each notebook
    for ddl_path, needs_strip in [
        (_BRONZE_DDL, False),
        (_SILVER_DDL, False),
        (_GOLD_DDL, True),
    ]:
        for sql in get_all_sql_cells(ddl_path):
            sql = sql.strip()
            if not sql or sql.startswith("CREATE SCHEMA"):
                continue
            if needs_strip:
                sql = strip_identity(sql)
            # Databricks defaults to Delta, local Spark doesn't — inject USING DELTA
            # Syntax: CREATE TABLE name (cols...) USING DELTA
            if "CREATE TABLE" in sql and "USING" not in sql:
                sql = re.sub(r"\)\s*$", ") USING DELTA", sql)
            spark.sql(sql)

    yield spark

    # Tear down
    spark.sql("DROP SCHEMA IF EXISTS gold CASCADE")
    spark.sql("DROP SCHEMA IF EXISTS silver CASCADE")
    spark.sql("DROP SCHEMA IF EXISTS bronze CASCADE")
