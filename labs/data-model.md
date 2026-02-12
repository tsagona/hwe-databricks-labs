# Bookstore Data Model — Bronze / Silver / Gold

All tables follow the naming convention `schema.table`, where the schema is the medallion layer: `bronze.*`, `silver.*`, `gold.*`.

## Bronze (Week 4) — Raw CSV Ingestion

All tables add `ingestion_timestamp` and `source_filename`. MERGE INTO using natural keys for idempotency.

### bronze.books

| Column | Notes |
|---|---|
| `isbn` PK | Natural key |
| `title` | |
| `author` | |
| `genre` | |
| `ingestion_timestamp` | Added on load |
| `source_filename` | Added on load |

### bronze.stores

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

### bronze.online_orders

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

### bronze.instore_orders

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

### Items JSON Structure

```json
[
  {"isbn": "978-0-13-468599-1", "title": "The Pragmatic Programmer", "quantity": 1, "unit_price": 49.99},
  {"isbn": "978-0-59-651798-1", "title": "JavaScript: The Good Parts", "quantity": 2, "unit_price": 29.99}
]
```

---

## Silver (Week 5) — Normalized 3NF

### silver.customers — derived from bronze.online_orders

| Column | Notes |
|---|---|
| `email` PK | Natural key |
| `name` | From most recent order |
| `address` | From most recent order |
| `city` | From most recent order |
| `state` | From most recent order |
| `zip` | From most recent order |

Extracted from `bronze.online_orders` by grouping on `customer_email` and taking the most recent order's customer fields.

### silver.books

| Column | Notes |
|---|---|
| `isbn` PK | Natural key |
| `title` | |
| `author` | |
| `genre` | |

**Data quality checks (bronze → silver):**
1. **Null/empty** — Reject rows where `isbn` or `title` is null or empty string.
2. **ISBN format** — Validate `isbn` matches ISBN-13 pattern (`978-X-XX-XXXXXX-X`).
3. **Whitespace** — Trim leading/trailing whitespace from all string fields.

### silver.stores

| Column | Notes |
|---|---|
| `store_nbr` PK | Natural key |
| `name` | |
| `address` | |
| `city` | |
| `state` | |
| `zip` | |

### silver.orders — unified from online + instore

| Column | Notes |
|---|---|
| `order_id` PK | From source bronze tables |
| `order_channel` PK | `'online'` or `'in-store'` |
| `order_datetime` | From `order_timestamp` or `transaction_timestamp` |
| `customer_email` FK → silver.customers | COALESCE to `'in-store'` for anonymous instore |
| `store_nbr` FK → silver.stores | COALESCE to `'online'` for online orders |
| `payment_method` | |
| `total_amount` | Kept for data quality cross-checks |
| `cashier_name` | Nullable (instore only) |

MERGE key: `(order_id, order_channel)`

### silver.order_items — exploded from items JSON

| Column | Notes |
|---|---|
| `order_id` PK, FK → silver.orders | |
| `order_channel` PK, FK → silver.orders | |
| `isbn` PK, FK → silver.books | |
| `quantity` | |
| `unit_price` | |

---

## Gold (Week 6) — Star Schema, Surrogate Keys

### gold.dim_customer — SCD Type 1

| Column | Notes |
|---|---|
| `customer_id` PK | Surrogate key |
| `email` | Natural key |
| `name` | |
| `address` | |
| `city` | |
| `state` | |
| `zip` | |

### gold.dim_book

| Column | Notes |
|---|---|
| `book_id` PK | Surrogate key |
| `isbn` | Natural key |
| `title` | |
| `author` | |
| `genre` | |

### gold.dim_store

| Column | Notes |
|---|---|
| `store_id` PK | Surrogate key |
| `store_nbr` | Natural key |
| `name` | |
| `address` | |
| `city` | |
| `state` | |
| `zip` | |

### gold.dim_date

| Column | Notes |
|---|---|
| `date_id` PK | Surrogate key |
| `date` | |
| `day_of_week` | |
| `month` | |
| `quarter` | |
| `year` | |

### gold.fact_sales — grain: one row per line item

| Column | Notes |
|---|---|
| `sales_id` PK | Surrogate key |
| `customer_id` FK → gold.dim_customer | |
| `book_id` FK → gold.dim_book | |
| `date_id` FK → gold.dim_date | |
| `store_id` FK → gold.dim_store | |
| `order_id` | Degenerate dimension; part of MERGE key |
| `order_channel` | Degenerate dimension; part of MERGE key |
| `isbn` | Degenerate dimension; part of MERGE key |
| `quantity` | Measure |
| `unit_price` | Measure |
| `line_total` | quantity * unit_price |
| `payment_method` | Degenerate dimension |
