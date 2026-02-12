# Week 4: Bronze Layer

Build the Bronze layer of a medallion architecture for an online bookstore. You will ingest raw CSV files into Delta tables using Spark SQL, adding audit columns and using MERGE INTO for idempotent loads.

## Learning Objectives

- Understand the medallion architecture (Bronze / Silver / Gold)
- Understand idempotent pipelines and why they matter
- Read CSV files using Spark SQL
- Use MERGE INTO to load data without creating duplicates

## Key Concepts

**Medallion architecture** organizes a lakehouse into three layers:
- **Bronze** — raw data, as close to the source as possible
- **Silver** — cleaned, normalized, conformed
- **Gold** — aggregated / denormalized for analytics

**Idempotent pipelines** produce the same result whether you run them once or many times. We achieve this with MERGE INTO — if a row with the same natural key already exists, we update it; otherwise we insert it.

## Lab Instructions

### Step 1: Create Target Tables

Create each bronze Delta table if it doesn't already exist. Example:

```sql
CREATE TABLE IF NOT EXISTS bronze.books (
  isbn STRING,
  title STRING,
  author STRING,
  genre STRING,
  ingestion_timestamp TIMESTAMP,
  source_filename STRING
);
```

### Step 2: Create Temporary Views from CSV Files

Use `read_files` to load each CSV into a temporary view with the audit columns added. There are four source files:
- `books.csv`
- `stores.csv`
- `online_orders.csv`
- `instore_orders.csv`

```sql
CREATE OR REPLACE TEMPORARY VIEW books_raw AS
SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_filename
FROM read_files(
  '/path/to/books.csv',
  format => 'csv',
  header => true
);
```

The `_metadata.file_path` column is automatically available when reading files and contains the source filename. `current_timestamp()` captures when the row was ingested.

### Step 3: MERGE INTO Bronze Tables

For each table, use MERGE INTO to upsert from the temporary view into the target table, matching on the natural key. If the row exists, update all columns. If not, insert it.

```sql
MERGE INTO bronze.books AS target
USING books_raw AS source
ON target.isbn = source.isbn
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Repeat for each table:

#### 3a. bronze.books

| Column | Notes |
|---|---|
| `isbn` PK | Natural key |
| `title` | |
| `author` | |
| `genre` | |
| `ingestion_timestamp` | Added on load |
| `source_filename` | Added on load |

MERGE key: `isbn`

#### 3b. bronze.stores

| Column | Notes |
|---|---|
| `store_nbr` PK | Natural key |
| `store_name` | |
| `store_address` | |
| `store_city` | |
| `store_state` | |
| `store_zip` | |
| `ingestion_timestamp` | Added on load |
| `source_filename` | Added on load |

MERGE key: `store_nbr`

#### 3c. bronze.online_orders

| Column | Notes |
|---|---|
| `order_id` PK | Natural key |
| `order_timestamp` | |
| `customer_email` | |
| `customer_name` | |
| `customer_address` | Street address |
| `customer_city` | |
| `customer_state` | |
| `customer_zip` | |
| `items` | JSON string |
| `payment_method` | |
| `total_amount` | |
| `ingestion_timestamp` | Added on load |
| `source_filename` | Added on load |

MERGE key: `order_id`

#### 3d. bronze.instore_orders

| Column | Notes |
|---|---|
| `order_id` PK | Natural key |
| `transaction_timestamp` | |
| `store_nbr` | |
| `customer_email` | Nullable |
| `items` | JSON string |
| `payment_method` | |
| `total_amount` | |
| `cashier_name` | |
| `ingestion_timestamp` | Added on load |
| `source_filename` | Added on load |

MERGE key: `order_id`

### Step 4: Verify Idempotency

Run your entire pipeline a second time against the same CSV files. Then verify:

1. **Row counts haven't changed** — each table should have the same number of rows as after the first run.
   ```sql
   SELECT 'bronze.books' AS table_name, COUNT(*) AS row_count FROM bronze.books
   UNION ALL
   SELECT 'bronze.stores', COUNT(*) FROM bronze.stores
   UNION ALL
   SELECT 'bronze.online_orders', COUNT(*) FROM bronze.online_orders
   UNION ALL
   SELECT 'bronze.instore_orders', COUNT(*) FROM bronze.instore_orders
   ```
2. **No duplicate keys** — confirm with:
   ```sql
   SELECT isbn, COUNT(*) FROM bronze.books GROUP BY isbn HAVING COUNT(*) > 1
   ```
3. **Audit columns updated** — `ingestion_timestamp` should reflect the second run for matched rows.
