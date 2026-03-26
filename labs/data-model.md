# Bookstore Data Model — Bronze / Silver / Gold

All tables follow the naming convention `schema.table`, where the schema is the medallion layer: `bronze.*`, `silver.*`, `gold.*`.

## Bronze (Week 4) — Raw CSV Ingestion

All tables add `ingestion_timestamp` and `source_filename`. MERGE INTO using natural keys for idempotency.

### bronze.categories

| Column | Type | Notes |
|---|---|---|
| `category_id` PK | STRING | Natural key |
| `category_name` | STRING | |
| `parent_category_id` | STRING | Empty string for top-level |
| `ingestion_timestamp` | TIMESTAMP | Added on load |
| `source_filename` | STRING | Added on load |

### bronze.books

| Column | Type | Notes |
|---|---|---|
| `isbn` PK | STRING | Natural key |
| `title` | STRING | |
| `author` | STRING | |
| `category_id` | STRING | FK to categories (leaf only) |
| `ingestion_timestamp` | TIMESTAMP | Added on load |
| `source_filename` | STRING | Added on load |

### bronze.stores

| Column | Type | Notes |
|---|---|---|
| `store_nbr` PK | STRING | Natural key |
| `name` | STRING | |
| `address` | STRING | |
| `city` | STRING | |
| `state` | STRING | |
| `zip` | STRING | |
| `ingestion_timestamp` | TIMESTAMP | Added on load |
| `source_filename` | STRING | Added on load |

### bronze.online_orders

| Column | Type | Notes |
|---|---|---|
| `order_id` PK | STRING | Natural key |
| `order_timestamp` | TIMESTAMP | |
| `customer_email` | STRING | |
| `customer_name` | STRING | |
| `customer_address` | STRING | Street address |
| `customer_city` | STRING | |
| `customer_state` | STRING | |
| `customer_zip` | STRING | |
| `items` | STRING | JSON array |
| `payment_method` | STRING | |
| `total_amount` | DECIMAL(10,2) | |
| `ingestion_timestamp` | TIMESTAMP | Added on load |
| `source_filename` | STRING | Added on load |

### bronze.instore_orders

| Column | Type | Notes |
|---|---|---|
| `order_id` PK | STRING | Natural key |
| `transaction_timestamp` | TIMESTAMP | |
| `store_nbr` | STRING | |
| `customer_email` | STRING | Nullable |
| `items` | STRING | JSON array |
| `payment_method` | STRING | |
| `total_amount` | DECIMAL(10,2) | |
| `cashier_name` | STRING | |
| `ingestion_timestamp` | TIMESTAMP | Added on load |
| `source_filename` | STRING | Added on load |

### Items JSON Structure

```json
[
  {"isbn": "978-0-13-468599-1", "title": "The Pragmatic Programmer", "quantity": 1, "unit_price": 49.99},
  {"isbn": "978-0-59-651798-1", "title": "JavaScript: The Good Parts", "quantity": 2, "unit_price": 29.99}
]
```

---

## Silver (Week 5) — Normalized 3NF

Silver layer is a **normalized 3NF (Third Normal Form)** data model with natural keys and referential integrity.

### Entity-Relationship Diagram

```
┌─────────────────┐
│   categories    │ ◄─┐ Self-referential hierarchy
├─────────────────┤   │ (parent_category_id → category_id)
│ category_id PK  │───┘
│ category_name   │
│ parent_cat_id   │
└─────────────────┘
        ▲
        │
        │ FK: category_id
        │
┌─────────────────┐         ┌──────────────────┐
│     books       │         │    customers     │
├─────────────────┤         ├──────────────────┤
│ isbn PK         │         │ email PK         │
│ title           │         │ name             │
│ author          │         │ address          │
│ category_id FK  │         │ city             │
└─────────────────┘         │ state            │
        ▲                   │ zip              │
        │                   └──────────────────┘
        │                           ▲
        │ FK: isbn                  │ FK: customer_email
        │                           │
┌─────────────────────────────────────┐         ┌─────────────────┐
│         order_items                 │         │     stores      │
├─────────────────────────────────────┤         ├─────────────────┤
│ order_id PK, FK ────────┐           │         │ store_nbr PK    │
│ order_channel PK, FK ───┤           │         │ name            │
│ isbn PK, FK             │           │         │ address         │
│ quantity                │           │         │ city            │
│ unit_price              │           │         │ state           │
└─────────────────────────┼───────────┘         │ zip             │
                          │                     └─────────────────┘
                          │                              ▲
                          │                              │ FK: store_nbr
                          ▼                              │
                    ┌──────────────────────────┐         │
                    │        orders            │─────────┘
                    ├──────────────────────────┤
                    │ order_id PK              │
                    │ order_channel PK         │
                    │ order_datetime           │
                    │ customer_email FK        │
                    │ store_nbr FK             │
                    │ payment_method           │
                    │ total_amount             │
                    │ cashier_name (nullable)  │
                    └──────────────────────────┘
```

### silver.categories — self-referential hierarchy

| Column | Type | Notes |
|---|---|---|
| `category_id` PK | STRING | Natural key |
| `category_name` | STRING | |
| `parent_category_id` FK | STRING | FK to self; empty string for top-level |

Three levels: Category (2 rows) → Genre (8 rows) → Subgenre (16 rows). 26 total rows. Books reference leaf-level (subgenre) category_id only.

### silver.customers — derived from bronze.online_orders

| Column | Type | Notes |
|---|---|---|
| `email` PK | STRING | Natural key |
| `name` | STRING | From most recent order |
| `address` | STRING | From most recent order |
| `city` | STRING | From most recent order |
| `state` | STRING | From most recent order |
| `zip` | STRING | From most recent order |

Extracted from `bronze.online_orders` by grouping on `customer_email` and taking the most recent order's customer fields.

### silver.books

| Column | Type | Notes |
|---|---|---|
| `isbn` PK | STRING | Natural key |
| `title` | STRING | |
| `author` | STRING | |
| `category_id` FK | STRING | FK → silver.categories (leaf only) |

**Data quality checks (bronze → silver):**
1. **Null/empty** — Reject rows where `isbn` or `title` is null or empty string.
2. **ISBN format** — Validate `isbn` matches ISBN-13 pattern (`978-X-XX-XXXXXX-X`).
3. **Whitespace** — Trim leading/trailing whitespace from all string fields.

### silver.stores

| Column | Type | Notes |
|---|---|---|
| `store_nbr` PK | STRING | Natural key |
| `name` | STRING | |
| `address` | STRING | |
| `city` | STRING | |
| `state` | STRING | |
| `zip` | STRING | |

### silver.orders — unified from online + instore

| Column | Type | Notes |
|---|---|---|
| `order_id` PK | STRING | From source bronze tables |
| `order_channel` PK | STRING | `'online'` or `'in-store'` |
| `order_datetime` | TIMESTAMP | From `order_timestamp` or `transaction_timestamp` |
| `customer_email` FK | STRING | COALESCE to `'in-store'` for anonymous instore |
| `store_nbr` FK | STRING | COALESCE to `'online'` for online orders |
| `payment_method` | STRING | |
| `total_amount` | DECIMAL(10,2) | Kept for data quality cross-checks |
| `cashier_name` | STRING | Nullable (instore only) |

MERGE key: `(order_id, order_channel)`

### silver.order_items — exploded from items JSON

| Column | Type | Notes |
|---|---|---|
| `order_id` PK, FK | STRING | FK → silver.orders |
| `order_channel` PK, FK | STRING | FK → silver.orders |
| `isbn` PK, FK | STRING | FK → silver.books |
| `quantity` | INT | |
| `unit_price` | DECIMAL(10,2) | |

---

## Gold (Week 6) — Star Schema, Surrogate Keys

Gold layer is a **dimensional model (star schema)** optimized for analytics. All dimensions have surrogate keys. Category hierarchy is denormalized onto `dim_book`.

### Star Schema Diagram

```
                         ┌──────────────────┐
                         │  dim_customer    │
                         ├──────────────────┤
                         │ customer_id PK   │
                         │ email            │
                         │ name             │
                         │ address          │
                         │ city, state, zip │
                         └──────────────────┘
                                  │
                                  │ customer_id (FK)
                                  │
┌──────────────────┐              │              ┌──────────────────┐
│    dim_book      │              │              │    dim_date      │
├──────────────────┤              │              ├──────────────────┤
│ book_id PK       │              │              │ date_id PK       │
│ isbn             │              ▼              │ full_date        │
│ title            │    ┌──────────────────┐    │ day_of_week      │
│ author           │───▶│   fact_sales     │◀───│ month            │
│ subgenre         │    ├──────────────────┤    │ quarter          │
│ genre            │    │ sales_id PK      │    │ year             │
│ category         │    │ customer_id FK   │    │ fiscal_quarter   │
└──────────────────┘    │ book_id FK       │    │ ...              │
         │              │ store_id FK      │    └──────────────────┘
    book_id (FK)        │ date_id FK       │              │
                        │ order_id         │         date_id (FK)
                        │ order_channel    │
                        │ isbn             │
                        │ quantity         │
                        │ unit_price       │
                        │ line_total       │
                        │ payment_method   │
                        └──────────────────┘
┌──────────────────┐              ▲
│   dim_store      │              │
├──────────────────┤              │
│ store_id PK      │              │ store_id (FK)
│ store_nbr        │              │
│ name             │              │
│ address          │              │
│ city, state, zip │──────────────┘
└──────────────────┘

         ★ Star Schema: Fact table in center, dimensions radiate outward ★
```

### gold.dim_customer — SCD Type 1

| Column | Type | Notes |
|---|---|---|
| `customer_id` PK | BIGINT | Surrogate key (IDENTITY) |
| `email` | STRING | Natural key |
| `name` | STRING | |
| `address` | STRING | |
| `city` | STRING | |
| `state` | STRING | |
| `zip` | STRING | |

**Sentinel row:** `'in-store'` customer for anonymous purchases.

### gold.dim_book — flattened category hierarchy

| Column | Type | Notes |
|---|---|---|
| `book_id` PK | BIGINT | Surrogate key (IDENTITY) |
| `isbn` | STRING | Natural key |
| `title` | STRING | |
| `author` | STRING | |
| `subgenre` | STRING | Leaf category name (from silver.categories self-join) |
| `genre` | STRING | Mid-level category name |
| `category` | STRING | Top-level category name |

The 3-level category hierarchy from `silver.categories` is **flattened** onto `dim_book` via two self-joins during the gold load. This demonstrates the difference between 3NF (self-joins required) and star schema (pre-joined for analysts). No separate gold categories dimension is needed.

### gold.dim_store

| Column | Type | Notes |
|---|---|---|
| `store_id` PK | BIGINT | Surrogate key (IDENTITY) |
| `store_nbr` | STRING | Natural key |
| `name` | STRING | |
| `address` | STRING | |
| `city` | STRING | |
| `state` | STRING | |
| `zip` | STRING | |

**Sentinel row:** `'online'` store for online orders.

### gold.dim_date — loaded from CSV

| Column | Type | Notes |
|---|---|---|
| `date_id` PK | INT | Surrogate key (from CSV, not IDENTITY) |
| `full_date` | DATE | |
| `day_of_week` | INT | Numeric (1-7) |
| `day_num_in_month` | INT | |
| `day_name` | STRING | e.g., "Monday" |
| `day_abbrev` | STRING | e.g., "Mon" |
| `weekday_flag` | STRING | 'Y' or 'N' |
| `week_num_in_year` | INT | |
| `week_begin_date` | DATE | |
| `week_begin_date_key` | INT | |
| `month` | INT | |
| `month_name` | STRING | e.g., "January" |
| `month_abbrev` | STRING | e.g., "Jan" |
| `quarter` | INT | |
| `year` | INT | |
| `yearmo` | INT | YYYYMM format |
| `fiscal_month` | INT | |
| `fiscal_quarter` | INT | |
| `fiscal_year` | INT | |
| `last_day_in_month_flag` | STRING | 'Y' or 'N' |
| `same_day_year_ago_date` | DATE | |

### gold.fact_sales — grain: one row per line item

| Column | Type | Notes |
|---|---|---|
| `sales_id` PK | BIGINT | Surrogate key (IDENTITY) |
| `customer_id` FK | BIGINT | FK → gold.dim_customer |
| `book_id` FK | BIGINT | FK → gold.dim_book |
| `date_id` FK | INT | FK → gold.dim_date |
| `store_id` FK | BIGINT | FK → gold.dim_store |
| `order_id` | STRING | Degenerate dimension; part of MERGE key |
| `order_channel` | STRING | Degenerate dimension; part of MERGE key |
| `isbn` | STRING | Degenerate dimension; part of MERGE key |
| `quantity` | INT | Measure |
| `unit_price` | DECIMAL(10,2) | Measure |
| `line_total` | DECIMAL(10,2) | Measure: quantity × unit_price |
| `payment_method` | STRING | Degenerate dimension |

**MERGE key (natural key):** `(order_id, order_channel, isbn)` — ensures idempotent loads.

**Degenerate dimensions:** Attributes from the transaction (order_id, isbn, etc.) that don't warrant their own dimension table. Carried on the fact table for drill-through queries.

---

## Key Differences Between Layers

| Aspect | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **Purpose** | Raw historical data | Cleaned, normalized data | Analytics-ready |
| **Schema** | CSV structure + audit | 3NF (Third Normal Form) | Star schema |
| **Keys** | Natural keys | Natural keys | Surrogate keys |
| **Normalization** | Denormalized (JSON arrays) | Normalized (exploded) | Denormalized (pre-joined) |
| **Data Quality** | None (as-is from source) | Validated, cleaned | Aggregated, enriched |
| **Query Pattern** | Historical audit | Operational queries | Analytical queries |
| **Category Hierarchy** | Flat (1 table) | Self-referential (JOINs) | Flattened (pre-joined) |

---

## Design Decisions

- **Customer data** lives on online orders in bronze (no separate customers source). `silver.customers` is extracted from `bronze.online_orders` by grouping on email and taking the most recent order's fields.
- **Unified silver.orders** uses COALESCE sentinel values (`'in-store'`, `'online'`) to handle nullable composite key parts from the two order sources.
- **Surrogate keys** use `BIGINT GENERATED ALWAYS AS IDENTITY` in gold dimensions only.
- **Sentinel dimension rows** (`'in-store'` customer, `'online'` store) are added in gold to ensure fact table JOINs don't drop rows.
- **Category hierarchy** demonstrates the key difference between 3NF and star schema:
  - Silver: Self-referential table (requires 2 JOINs to get full hierarchy)
  - Gold: Flattened onto `dim_book` (no JOINs needed for analysts)
- **Degenerate dimensions** (order_id, isbn, payment_method) are transaction attributes carried on the fact table rather than creating tiny dimensions.
