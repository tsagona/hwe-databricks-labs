# HWE Databricks Labs

## Project Overview

This is a data engineering course with labs built on Databricks. The course covers 6 weeks.
The domain is an **online bookstore** using a medallion architecture (Bronze → Silver → Gold).

## Key Files

- `labs/curriculum.md` — Weekly curriculum overview (weeks 1-6)
- `labs/data-model.md` — Full data model specification (Bronze, Silver, Gold)
- `labs/week4/` — Bronze: week4_lab.ipynb (solution), create_bronze.ipynb, drop_bronze.ipynb
- `labs/week5/` — Silver: week5_lab.ipynb (solution), create_silver.ipynb, drop_silver.ipynb
- `labs/week6/` — Gold: week6_lab.ipynb (solution), create_gold.ipynb, drop_gold.ipynb
- Data generation scripts live in a separate infrastructure repo (deterministic bronze CSV test data + DBFS upload)

## Table Naming Convention

All tables use `schema.table` naming, where the schema is the medallion layer: `bronze.*`, `silver.*`, `gold.*`. The catalog is `student_name` (not specified in DDL/lab files).

## Data Model Summary

### Bronze (Week 4) — Raw CSV Ingestion
4 source tables, each with `ingestion_timestamp` and `source_filename` appended. MERGE INTO using natural keys for idempotency.

- **bronze.books** (PK: isbn)
- **bronze.stores** (PK: store_nbr)
- **bronze.online_orders** (PK: order_id) — includes customer name/address fields
- **bronze.instore_orders** (PK: order_id) — customer_email nullable

Both order tables have an `items` column containing a JSON array of line items:
`[{"isbn": "...", "title": "...", "quantity": N, "unit_price": N.NN}, ...]`

### Silver (Week 5) — Normalized 3NF
5 tables. All tables use natural keys as PKs.

- **silver.customers** (PK: email) — derived from bronze.online_orders, takes most recent order's customer fields
- **silver.books** (PK: isbn)
- **silver.stores** (PK: store_nbr)
- **silver.orders** (PK: order_id + order_channel) — unified from online + instore orders
  - MERGE key: (order_id, order_channel)
  - online orders: store_nbr COALESCEd to 'online'
  - instore orders: customer_email COALESCEd to 'in-store' when null
- **silver.order_items** (PK: order_id + order_channel + isbn) — exploded from items JSON, drops redundant title

### Gold (Week 6) — Star Schema, Surrogate Keys
5 tables. All dimensions have surrogate keys. Denormalized for query performance.

- **gold.dim_customer** (SCD Type 1)
- **gold.dim_book**
- **gold.dim_store**
- **gold.dim_date** — calendar dimension (date, day_of_week, month, quarter, year)
- **gold.fact_sales** — grain: one row per line item
  - MERGE key: (order_id, order_channel, isbn)
  - FKs: customer_id, book_id, date_id, store_id
  - Measures: quantity, unit_price, line_total (quantity * unit_price)
  - Degenerate dimensions: order_id, order_channel, isbn, payment_method

## Design Decisions

- Customer data lives on online orders in bronze (no separate customers source). `silver.customers` is extracted/normalized from `bronze.online_orders` by grouping on email and taking the most recent order's fields.
- The unified silver.orders table uses COALESCE sentinel values ('in-store', 'online') to handle nullable composite key parts from the two order sources.
- Both bronze order tables carry `order_id` from the source CSV. Silver uses `order_id + order_channel` as a natural composite PK (no surrogate needed). Surrogate keys are assigned only in gold (all dimensions + fact).
- Tables are organized into schemas by layer: `bronze.*`, `silver.*`, `gold.*`.
- `total_amount` is kept on silver.orders even though it's derivable from order_items — useful for data quality checks.
- `title` in the items JSON is dropped during the silver explode since it's redundant with silver.books.
- DDL (CREATE/DROP scripts) lives alongside the lab notebook in each week's folder. Lab notebooks contain only transformations.
- Surrogate keys use `_id` suffix (e.g., `customer_id`, `book_id`). The store natural key is `store_nbr` across all layers; `store_id` is the surrogate in gold only.
- Gold dimensions use `BIGINT GENERATED ALWAYS AS IDENTITY` for surrogate keys.
- Gold fact_sales uses MERGE on the natural key `(order_id, order_channel, isbn)` carried as degenerate dimensions, making the load idempotent and incremental.
- Sentinel dimension rows are added in the gold layer for `'in-store'` (dim_customer) and `'online'` (dim_store) to ensure fact table JOINs don't drop rows.
- All notebooks use SQL kernel (.ipynb with `"language": "sql"`).

## Course Week Mapping

- **Week 1**: SQL basics, Databricks UI
- **Week 2**: File formats, Parquet, Unity Catalog, CREATE TABLE, time travel
- **Week 3**: Git workflows, pytest, GitHub Actions
- **Week 4**: Bronze layer — PySpark, DataFrames, MERGE INTO, idempotent ingestion
- **Week 5**: Silver layer — 3NF normalization, JOINs, MERGE for upserts
- **Week 6**: Gold layer — Dimensional modeling, star schema, dims and facts
