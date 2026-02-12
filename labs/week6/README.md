# Week 6: Gold Layer

Build the Gold layer by transforming Silver 3NF tables into a star schema optimized for analytics. You will create dimension tables with surrogate keys, generate a date dimension, and build a fact table joining everything together.

## Learning Objectives

- Understand dimensional modeling and star schemas
- Understand the difference between dimension tables and fact tables
- Understand surrogate keys and why they are used in a star schema
- Understand SCD Type 1 (overwrite, no history)
- Understand degenerate dimensions
- Build a complete star schema from normalized source tables

## Key Concepts

**Star schema** organizes data into a central **fact table** surrounded by **dimension tables**. Facts contain measures (numbers you aggregate) and foreign keys to dimensions. Dimensions contain descriptive attributes you filter and group by.

**Surrogate keys** are system-generated identifiers (e.g., auto-increment integers) that replace natural keys in the gold layer. They provide a stable, compact join key that is independent of source system changes.

**SCD Type 1** (Slowly Changing Dimension, Type 1) means we overwrite dimension rows in place when source data changes. No history is kept — the dimension always reflects the current state.

**Degenerate dimensions** are dimension values stored directly on the fact table (no separate dimension table). Used when the attribute doesn't warrant its own table — like `order_channel` or `payment_method`.

## Lab Instructions

### Step 1: Build gold.dim_customer (SCD Type 1)

Source: `silver.customers`

| Column | Notes |
|---|---|
| `customer_id` PK | Surrogate key |
| `email` | Natural key |
| `name` | |
| `address` | |
| `city` | |
| `state` | |
| `zip` | |

**Transformations:**
- Add `customer_id` surrogate using `GENERATED ALWAYS AS IDENTITY` (for new rows)
- MERGE on `email` — if the customer exists, update all fields (SCD Type 1). If new, insert with a new surrogate key.

### Step 2: Build gold.dim_book

Source: `silver.books`

| Column | Notes |
|---|---|
| `book_id` PK | Surrogate key |
| `isbn` | Natural key |
| `title` | |
| `author` | |
| `genre` | |

**Transformations:**
- Add `book_id` surrogate
- MERGE on `isbn`

### Step 3: Build gold.dim_store

Source: `silver.stores`

| Column | Notes |
|---|---|
| `store_id` PK | Surrogate key |
| `store_nbr` | Natural key |
| `name` | |
| `address` | |
| `city` | |
| `state` | |
| `zip` | |

**Transformations:**
- Add `store_id` surrogate
- MERGE on `store_nbr`

### Step 4: Build gold.dim_date

Source: generated (not from a silver table)

| Column | Notes |
|---|---|
| `date_id` PK | Surrogate key |
| `date` | |
| `day_of_week` | |
| `month` | |
| `quarter` | |
| `year` | |

**Transformations:**
- Generate a sequence of dates covering the range of `order_datetime` values in `silver.orders`
- For each date, derive: `day_of_week`, `month`, `quarter`, `year`
- Assign `date_id` surrogate

### Step 5: Build gold.fact_sales

Sources: `silver.orders` + `silver.order_items` + all dimension tables (for surrogate key lookups)

| Column | Notes |
|---|---|
| `sales_id` PK | Surrogate key |
| `customer_id` FK | Lookup from gold.dim_customer |
| `book_id` FK | Lookup from gold.dim_book |
| `date_id` FK | Lookup from gold.dim_date |
| `store_id` FK | Lookup from gold.dim_store |
| `order_channel` | Degenerate dimension |
| `quantity` | Measure |
| `unit_price` | Measure |
| `line_total` | quantity * unit_price |
| `payment_method` | Degenerate dimension |

**Transformations:**
1. Join `silver.orders` to `silver.order_items` on `(order_id, order_channel)`
2. Join to `gold.dim_customer` on `email` = `customer_email` to get `customer_id`
3. Join to `gold.dim_book` on `isbn` to get `book_id`
4. Join to `gold.dim_date` on `date` = `order_datetime` cast to date to get `date_id`
5. Join to `gold.dim_store` on `store_nbr` to get surrogate `store_id`
6. Compute `line_total` = `quantity * unit_price`
7. Keep `order_channel` and `payment_method` as degenerate dimensions
8. Add `sales_id` surrogate

### Step 6: Verify

1. **Fact row count** — `gold.fact_sales` should have the same number of rows as `silver.order_items`.
2. **No null FKs** — every row in `gold.fact_sales` should have non-null values for `customer_id`, `book_id`, `date_id`, and `store_id`.
3. **line_total check** — verify `line_total` = `quantity * unit_price` for all rows.
4. **Sample queries** — confirm the star schema works for analytics:
   ```sql
   -- Total sales by genre
   SELECT b.genre, SUM(f.line_total) AS total_sales
   FROM gold.fact_sales f
   JOIN gold.dim_book b ON f.book_id = b.book_id
   GROUP BY b.genre
   ORDER BY total_sales DESC

   -- Monthly sales by store
   SELECT s.name, d.month, d.year, SUM(f.line_total) AS total_sales
   FROM gold.fact_sales f
   JOIN gold.dim_store s ON f.store_id = s.store_id
   JOIN gold.dim_date d ON f.date_id = d.date_id
   GROUP BY s.name, d.month, d.year
   ORDER BY d.year, d.month
   ```
