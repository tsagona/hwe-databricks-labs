# Week 5: Silver Layer

Build the Silver layer by cleaning, normalizing, and conforming Bronze data into 3NF tables. You will apply data quality checks, extract a customers table, unify two order sources, and explode JSON arrays into rows.

## Learning Objectives

- Understand normalization and Third Normal Form (3NF)
- Apply data quality checks during ingestion
- Use JOINs to combine and relate tables
- Use MERGE INTO for upserts (insert or update)
- Explode JSON arrays into individual rows

## Key Concepts

**Third Normal Form (3NF)** eliminates redundancy by ensuring every non-key column depends on the key, the whole key, and nothing but the key. In practice this means:
- Customer data gets its own table (instead of being repeated on every order)
- Order line items get their own table (instead of being a JSON blob)
- Redundant columns (like `title` on line items) are dropped since they can be looked up via a JOIN

**Data quality checks** are applied during the Bronze-to-Silver transition — this is where we reject bad data and clean up the rest.

## Lab Instructions

### Step 1: Build silver.books

Source: `bronze.books`

| Column | Notes |
|---|---|
| `isbn` PK | Natural key |
| `title` | |
| `author` | |
| `genre` | |

**Transformations:**
- Drop `ingestion_timestamp` and `source_filename`

**Data quality checks:**
1. **Null/empty** — Reject rows where `isbn` or `title` is null or empty string.
2. **ISBN format** — Validate that `isbn` matches ISBN-13 pattern (`978-X-XX-XXXXXX-X`).
3. **Whitespace** — Trim leading/trailing whitespace from all string fields.

Use MERGE INTO on `isbn`.

### Step 2: Build silver.stores

Source: `bronze.stores`

| Column | Notes |
|---|---|
| `store_nbr` PK | Natural key |
| `name` | Renamed from `store_name` |
| `address` | Renamed from `store_address` |
| `city` | Renamed from `store_city` |
| `state` | Renamed from `store_state` |
| `zip` | Renamed from `store_zip` |

**Transformations:**
- Drop `ingestion_timestamp` and `source_filename`
- Drop the `store_` prefix from all columns except `store_nbr`

Use MERGE INTO on `store_nbr`.

### Step 3: Build silver.customers

Source: `bronze.online_orders`

| Column | Notes |
|---|---|
| `email` PK | Natural key |
| `name` | From most recent order |
| `address` | From most recent order |
| `city` | From most recent order |
| `state` | From most recent order |
| `zip` | From most recent order |

**Transformations:**
- Group `bronze.online_orders` by `customer_email`
- For each email, take the customer fields (`customer_name`, `customer_address`, `customer_city`, `customer_state`, `customer_zip`) from the row with the most recent `order_timestamp`
- Rename columns: drop the `customer_` prefix

This table does not exist in the source data — it is *derived* by extracting and deduplicating customer information from online orders.

Use MERGE INTO on `email`.

### Step 4: Build silver.orders

Sources: `bronze.online_orders` + `bronze.instore_orders`

| Column | Notes |
|---|---|
| `order_id` PK | From source bronze tables |
| `order_channel` PK | `'online'` or `'in-store'` |
| `order_datetime` | From `order_timestamp` or `transaction_timestamp` |
| `customer_email` FK | COALESCE to `'in-store'` for anonymous instore |
| `store_nbr` FK | COALESCE to `'online'` for online orders |
| `payment_method` | |
| `total_amount` | Kept for data quality cross-checks |
| `cashier_name` | Nullable (instore only) |

**Transformations:**

For online orders:
- Set `order_channel` = `'online'`
- Rename `order_timestamp` → `order_datetime`
- Set `store_nbr` = `'online'` (no physical store)
- Set `cashier_name` = `NULL`
- Drop customer name/address fields (now in silver.customers)

For instore orders:
- Set `order_channel` = `'in-store'`
- Rename `transaction_timestamp` → `order_datetime`
- COALESCE `customer_email` to `'in-store'` when null

Union the two DataFrames, then MERGE INTO on `(order_id, order_channel)`.

### Step 5: Build silver.order_items

Sources: `bronze.online_orders` + `bronze.instore_orders` (the `items` JSON column)

| Column | Notes |
|---|---|
| `order_id` PK, FK | |
| `order_channel` PK, FK | |
| `isbn` PK, FK | |
| `quantity` | |
| `unit_price` | |

**Transformations:**
- Parse the `items` JSON string into a struct array
- Explode the array so each line item becomes its own row
- Add `order_channel` (`'online'` or `'in-store'`)
- Drop `title` (redundant — can be looked up via silver.books)

Use MERGE INTO on `(order_id, order_channel, isbn)`.

### Step 6: Verify

1. **Row counts** — `silver.books` should equal `bronze.books` minus any rejected rows. `silver.stores` should equal `bronze.stores`.
2. **Customer count** — `silver.customers` should equal the number of distinct `customer_email` values in `bronze.online_orders`.
3. **Order count** — `silver.orders` should equal the combined count of `bronze.online_orders` + `bronze.instore_orders`.
4. **Referential integrity** — every `customer_email` in `silver.orders` should exist in `silver.customers` (or be `'in-store'`). Every `isbn` in `silver.order_items` should exist in `silver.books`.
5. **Total amount cross-check** — for a sample of orders, verify that the sum of `quantity * unit_price` across `silver.order_items` matches `total_amount` in `silver.orders`.
