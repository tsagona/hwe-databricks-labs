# Week 6 Test Data

This document shows the test data that is automatically created by the test fixtures before each test runs. Data is inserted into silver tables, and gold dimensions are pre-populated with known surrogate keys.

## Silver Layer Test Data

### silver.categories

3-level category hierarchy:

| category_id | category_name | parent_category_id |
|-------------|---------------|--------------------|
| 1 | Fiction | *(empty string)* |
| 3 | Science Fiction | 1 |
| 11 | Space Opera | 3 |

**Hierarchy:**
- Fiction (top-level category)
  - Science Fiction (genre)
    - Space Opera (subgenre)

### silver.stores

| store_nbr | name | address | city | state | zip |
|-----------|------|---------|------|-------|-----|
| S001 | Downtown Books | 100 Main St | Springfield | IL | 62701 |

### silver.books

| isbn | title | author | category_id |
|------|-------|--------|-------------|
| 978-0-00-000001-1 | Test Book One | Author A | 11 |
| 978-0-00-000002-2 | Test Book Two | Author B | 11 |

### silver.customers

| email | name | address | city | state | zip |
|-------|------|---------|------|-------|-----|
| alice@example.com | Alice Smith | 123 Elm St | Springfield | IL | 62701 |
| bob@example.com | Bob Jones | 456 Oak Ave | Springfield | IL | 62702 |

### silver.orders

| order_id | order_channel | order_datetime | customer_email | store_nbr | payment_method | total_amount | cashier_name |
|----------|---------------|----------------|----------------|-----------|----------------|--------------|--------------|
| ONL-001 | online | 2025-06-15 10:00:00 | alice@example.com | online | credit_card | 39.98 | NULL |
| ONL-002 | online | 2025-06-15 14:00:00 | bob@example.com | online | debit_card | 24.99 | NULL |
| INS-001 | in-store | 2025-06-15 11:00:00 | **in-store** | S001 | cash | 19.99 | Bob Jones |
| INS-002 | in-store | 2025-06-16 12:00:00 | bob@example.com | S001 | credit_card | 84.96 | Jane Doe |

### silver.order_items

| order_id | order_channel | isbn | quantity | unit_price |
|----------|---------------|------|----------|------------|
| ONL-001 | online | 978-0-00-000001-1 | 2 | 19.99 |
| ONL-002 | online | 978-0-00-000002-2 | 1 | 24.99 |
| INS-001 | in-store | 978-0-00-000001-1 | 1 | 19.99 |
| INS-002 | in-store | 978-0-00-000001-1 | 3 | 19.99 |
| INS-002 | in-store | 978-0-00-000002-2 | 1 | 24.99 |

---

## Gold Layer Test Data (Pre-populated Dimensions)

For fact table tests, dimensions are pre-populated with **known surrogate key values** so FK lookups work correctly.

### gold.dim_customer

| customer_id | email | name | address | city | state | zip |
|-------------|-------|------|---------|------|-------|-----|
| **1** | alice@example.com | Alice Smith | 123 Elm St | Springfield | IL | 62701 |
| **2** | bob@example.com | Bob Jones | 456 Oak Ave | Springfield | IL | 62702 |
| **3** | **in-store** | In-Store Customer | NULL | NULL | NULL | NULL |

**Sentinel row:** customer_id=3 for anonymous in-store purchases

### gold.dim_store

| store_id | store_nbr | name | address | city | state | zip |
|----------|-----------|------|---------|------|-------|-----|
| **1** | S001 | Downtown Books | 100 Main St | Springfield | IL | 62701 |
| **2** | **online** | Online | NULL | NULL | NULL | NULL |

**Sentinel row:** store_id=2 for online orders

### gold.dim_book

| book_id | isbn | title | author | subgenre | genre | category |
|---------|------|-------|--------|----------|-------|----------|
| **1** | 978-0-00-000001-1 | Test Book One | Author A | Space Opera | Science Fiction | Fiction |
| **2** | 978-0-00-000002-2 | Test Book Two | Author B | Space Opera | Science Fiction | Fiction |

**Note:** The category hierarchy is **flattened** onto dim_book via self-joins on silver.categories

### gold.dim_date

| date_id | full_date | day_of_week | day_name | month | month_name | quarter | year |
|---------|-----------|-------------|----------|-------|------------|---------|------|
| **20250615** | 2025-06-15 | 1 | Sunday | 6 | June | 2 | 2025 |
| **20250616** | 2025-06-16 | 2 | Monday | 6 | June | 2 | 2025 |

*(Additional columns exist but are omitted for brevity)*

---

## Expected Gold Results

After running gold transformations:

### gold.fact_sales

5 rows (one per line item):

| sales_id | customer_id | book_id | store_id | date_id | order_id | order_channel | isbn | quantity | unit_price | line_total | payment_method |
|----------|-------------|---------|----------|---------|----------|---------------|------|----------|------------|------------|----------------|
| (auto) | 1 | 1 | 2 | 20250615 | ONL-001 | online | 978-0-00-000001-1 | 2 | 19.99 | 39.98 | credit_card |
| (auto) | 2 | 2 | 2 | 20250615 | ONL-002 | online | 978-0-00-000002-2 | 1 | 24.99 | 24.99 | debit_card |
| (auto) | **3** | 1 | **1** | 20250615 | INS-001 | in-store | 978-0-00-000001-1 | 1 | 19.99 | 19.99 | cash |
| (auto) | 2 | 1 | 1 | 20250616 | INS-002 | in-store | 978-0-00-000001-1 | 3 | 19.99 | 59.97 | credit_card |
| (auto) | 2 | 2 | 1 | 20250616 | INS-002 | in-store | 978-0-00-000002-2 | 1 | 24.99 | 24.99 | credit_card |

**Key testing points:**
- **Surrogate key lookups**: customer_id, book_id, store_id, date_id are FKs to dimensions
- **Sentinel FK usage**: INS-001 uses customer_id=3 (in-store sentinel), ONL orders use store_id=2 (online sentinel)
- **Line total calculation**: `quantity × unit_price`
- **Degenerate dimensions**: order_id, order_channel, isbn, payment_method carried on fact table
- **MERGE key**: (order_id, order_channel, isbn) ensures idempotent loads

## Testing Focus

- **Dimension creation**: Verify dimensions are populated from silver tables
- **Sentinel rows**: Both dim_customer and dim_store should have sentinel rows
- **Hierarchy flattening**: dim_book should have subgenre/genre/category columns from self-joins
- **Fact table FK lookups**: All foreign keys should resolve correctly to dimension surrogate keys
- **Calculated measures**: line_total = quantity × unit_price
- **Degenerate dimensions**: Verify order_id, order_channel, isbn, payment_method exist on fact table
