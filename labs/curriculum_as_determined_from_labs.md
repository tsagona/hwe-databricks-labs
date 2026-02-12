# Curriculum as Determined from Labs

Topics covered in each week, derived purely from the lab notebooks.

## Week 4: Bronze Layer

- Reading CSV files with `read_files` (single file vs. directory)
- Creating temporary views (`CREATE OR REPLACE TEMPORARY VIEW`)
- Accessing file metadata (`_metadata.file_path`) for lineage tracking
- Adding audit columns (`current_timestamp()`, `source_filename`)
- `MERGE INTO` with `WHEN MATCHED THEN UPDATE` / `WHEN NOT MATCHED THEN INSERT`
- Natural keys as merge keys (`isbn`, `store_nbr`, `order_id`)
- Idempotent pipelines — safe to re-run without creating duplicates
- Verifying idempotency with row counts and duplicate key checks (`GROUP BY ... HAVING COUNT(*) > 1`)
- `UNION ALL` to combine verification queries
- Static reference data vs. transactional data that grows over time

## Week 5: Silver Layer

- Data quality checks: rejecting nulls/empty strings, regex validation with `RLIKE`, `TRIM` whitespace
- Column renaming to normalize naming conventions (dropping source-system prefixes)
- Deriving a new entity from existing data (extracting `silver.customers` from `bronze.online_orders`)
- Window functions: `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC)` to pick the most recent record per group
- Unifying multiple source tables with `UNION ALL` and a discriminator column (`order_channel`)
- `COALESCE` for sentinel/default values on nullable foreign key columns
- `CAST(NULL AS STRING)` to align column types across a `UNION ALL`
- Parsing JSON with `FROM_JSON` and a typed schema (`ARRAY<STRUCT<...>>`)
- Flattening nested arrays with `LATERAL VIEW EXPLODE`
- `CAST` to `DECIMAL(10, 2)` for type conversion
- Composite merge keys (`order_id + order_channel`, `order_id + order_channel + isbn`)
- Dropping redundant columns during normalization (`title` removed from order items since it lives on `silver.books`)
- Referential integrity checks (`LEFT JOIN ... WHERE ... IS NULL`)
- Cross-check validation (comparing `SUM(quantity * unit_price)` against stored `total_amount`)

## Week 6: Gold Layer

- Star schema design: central fact table surrounded by dimension tables
- Surrogate keys with `BIGINT GENERATED ALWAYS AS IDENTITY`
- Why identity columns require explicit column lists in MERGE (no `INSERT *` / `UPDATE SET *`)
- SCD Type 1: overwrite dimension rows on update, no history kept
- Sentinel dimension rows for special cases (`'in-store'` customer, `'online'` store) to prevent dropped rows on JOINs
- Generating a date dimension with `SEQUENCE`, `EXPLODE`, and date functions (`DATE_FORMAT`, `MONTH`, `QUARTER`, `YEAR`)
- Truncate-and-reload strategy (`TRUNCATE TABLE` + `INSERT INTO`) as an alternative to MERGE when no natural key is preserved
- Building a fact table via multi-table JOINs: joining silver tables to dimension tables to look up surrogate keys
- Degenerate dimensions: attributes stored directly on the fact table (`order_channel`, `payment_method`)
- Computed measures (`line_total = quantity * unit_price`)
- `CAST(... AS DATE)` to extract date from timestamp for dimension lookup
- Verifying fact table: row count comparison with source, null foreign key detection
- Writing analytical queries against a star schema: joining fact to dimensions, grouping by dimension attributes, aggregating measures
