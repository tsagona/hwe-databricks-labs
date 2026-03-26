"""Setup all test schemas and tables for CI/CD testing.

This script creates all necessary Bronze, Silver, and Gold schemas and tables
needed for running pytest tests in GitHub Actions.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main():
    builder = SparkSession.builder \
        .appName('TestSetup') \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("Creating test schemas...")
    spark.sql('CREATE SCHEMA IF NOT EXISTS week3_testing')
    spark.sql('CREATE SCHEMA IF NOT EXISTS bronze')
    spark.sql('CREATE SCHEMA IF NOT EXISTS silver')
    spark.sql('CREATE SCHEMA IF NOT EXISTS gold')

    # Week 3 - Employee/HR tables
    print("Creating Week 3 test tables...")
    spark.sql('''
        CREATE TABLE IF NOT EXISTS week3_testing.employees (
            employee_id STRING, name STRING, email STRING,
            department STRING, salary DECIMAL(10,2), hire_date DATE
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS week3_testing.departments (
            department_id STRING, department_name STRING,
            manager_name STRING, budget DECIMAL(12,2)
        ) USING DELTA
    ''')

    # Bronze tables
    print("Creating Bronze tables...")
    spark.sql('''
        CREATE TABLE IF NOT EXISTS bronze.categories (
            category_id STRING, category_name STRING, parent_category_id STRING,
            ingestion_timestamp TIMESTAMP, source_filename STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS bronze.stores (
            store_nbr STRING, name STRING, address STRING, city STRING,
            state STRING, zip STRING, ingestion_timestamp TIMESTAMP, source_filename STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS bronze.books (
            isbn STRING, title STRING, author STRING, category_id STRING,
            ingestion_timestamp TIMESTAMP, source_filename STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS bronze.online_orders (
            order_id STRING, order_timestamp TIMESTAMP, customer_email STRING,
            customer_name STRING, customer_address STRING, customer_city STRING,
            customer_state STRING, customer_zip STRING, items STRING,
            payment_method STRING, total_amount DECIMAL(10,2),
            ingestion_timestamp TIMESTAMP, source_filename STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS bronze.instore_orders (
            order_id STRING, transaction_timestamp TIMESTAMP, store_nbr STRING,
            customer_email STRING, items STRING, payment_method STRING,
            total_amount DECIMAL(10,2), cashier_name STRING,
            ingestion_timestamp TIMESTAMP, source_filename STRING
        ) USING DELTA
    ''')

    # Silver tables
    print("Creating Silver tables...")
    spark.sql('''
        CREATE TABLE IF NOT EXISTS silver.categories (
            category_id STRING, category_name STRING, parent_category_id STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS silver.stores (
            store_nbr STRING, name STRING, address STRING,
            city STRING, state STRING, zip STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS silver.books (
            isbn STRING, title STRING, author STRING, category_id STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS silver.customers (
            email STRING, name STRING, address STRING,
            city STRING, state STRING, zip STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS silver.orders (
            order_id STRING, order_channel STRING, order_timestamp TIMESTAMP,
            customer_email STRING, store_nbr STRING, payment_method STRING,
            total_amount DECIMAL(10,2), cashier_name STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS silver.order_items (
            order_id STRING, order_channel STRING, isbn STRING,
            quantity INT, unit_price DECIMAL(10,2)
        ) USING DELTA
    ''')

    # Gold dimension tables
    print("Creating Gold dimension tables...")
    spark.sql('''
        CREATE TABLE IF NOT EXISTS gold.dim_customer (
            customer_id BIGINT, email STRING, name STRING,
            address STRING, city STRING, state STRING, zip STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS gold.dim_store (
            store_id BIGINT, store_nbr STRING, name STRING,
            address STRING, city STRING, state STRING, zip STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS gold.dim_book (
            book_id BIGINT, isbn STRING, title STRING, author STRING,
            subgenre STRING, genre STRING, category STRING
        ) USING DELTA
    ''')

    spark.sql('''
        CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_id INT, full_date DATE, day_of_week INT,
            day_num_in_month INT, day_name STRING, day_abbrev STRING,
            weekday_flag STRING, week_num_in_year INT, week_begin_date DATE,
            week_begin_date_key INT, month INT, month_name STRING,
            month_abbrev STRING, quarter INT, year INT, yearmo INT,
            fiscal_month INT, fiscal_quarter INT, fiscal_year INT,
            last_day_in_month_flag STRING, same_day_year_ago_date DATE
        ) USING DELTA
    ''')

    # Gold fact table
    print("Creating Gold fact table...")
    spark.sql('''
        CREATE TABLE IF NOT EXISTS gold.fact_sales (
            customer_id BIGINT, book_id BIGINT, date_id INT, store_id BIGINT,
            order_id STRING, order_channel STRING, isbn STRING,
            payment_method STRING, quantity INT, unit_price DECIMAL(10,2),
            line_total DECIMAL(10,2)
        ) USING DELTA
    ''')

    print("✅ All test tables created successfully")
    spark.stop()


if __name__ == "__main__":
    main()
