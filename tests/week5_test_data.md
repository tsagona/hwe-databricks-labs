# Week 5 Test Data

This document shows the test data that is automatically created by the test fixtures before each test runs. All data is inserted into bronze tables.

## bronze.categories

3-level category hierarchy:

| category_id | category_name | parent_category_id | ingestion_timestamp | source_filename |
|-------------|---------------|-------------------|---------------------|-----------------|
| 1 | Fiction | *(empty string)* | current_timestamp() | categories.csv |
| 3 | Science Fiction | 1 | current_timestamp() | categories.csv |
| 11 | Space Opera | 3 | current_timestamp() | categories.csv |

## bronze.stores

1 store location:

| store_nbr | name | address | city | state | zip | ingestion_timestamp | source_filename |
|-----------|------|---------|------|-------|-----|---------------------|-----------------|
| S001 | Downtown Books | 100 Main St | Springfield | IL | 62701 | current_timestamp() | stores.csv |

## bronze.books

5 books with data quality issues — **only 2 should pass validation**:

| isbn | title | author | category_id | ingestion_timestamp | source_filename | **Valid?** |
|------|-------|--------|-------------|---------------------|-----------------|------------|
| 978-0-00-000001-1 | Test Book One | Author A | 11 | current_timestamp() | books.csv | ✅ Valid |
| 978-0-00-000002-2 | Test Book Two | Author B | 11 | current_timestamp() | books.csv | ✅ Valid |
| BADISBN | Bad ISBN Book | Author C | 11 | current_timestamp() | books.csv | ❌ Invalid ISBN |
| 978-0-00-000004-4 | *(empty string)* | Author D | 11 | current_timestamp() | books.csv | ❌ Empty title |
| 978-0-00-000005-5 | `   ` (3 spaces) | Author E | 11 | current_timestamp() | books.csv | ❌ Whitespace-only title |

**Data quality rules:**
- Must have valid ISBN-13 format: `978-X-XX-XXXXXX-X`
- Must have non-empty, non-whitespace title
- Only 2 books should pass → silver.books

## bronze.online_orders

2 orders from the **same customer** (alice@example.com) at different times:

| order_id | order_timestamp | customer_email | customer_name | customer_address | customer_city | customer_state | customer_zip | items (JSON) | payment_method | total_amount | ingestion_timestamp | source_filename |
|----------|----------------|----------------|---------------|------------------|---------------|----------------|--------------|--------------|----------------|--------------|---------------------|-----------------|
| ONL-001 | 2025-06-01 10:00:00 | alice@example.com | Alice Old | 100 Old St | OldCity | IL | 60001 | `[{"isbn":"978-0-00-000001-1",...}]` | credit_card | 39.98 | current_timestamp() | online_orders_1.csv |
| ONL-002 | **2025-07-15 14:00:00** | alice@example.com | **Alice New** | **200 New Ave** | **NewCity** | IL | **60002** | `[{"isbn":"978-0-00-000002-2",...}]` | debit_card | 24.99 | current_timestamp() | online_orders_2.csv |

**Customer extraction rule:**
- For `silver.customers`, take the **most recent** order's customer fields
- alice@example.com should get: "Alice New", "200 New Ave", "NewCity", "60002" (from ONL-002)

## bronze.instore_orders

2 in-store orders:

| order_id | transaction_timestamp | store_nbr | customer_email | items (JSON) | payment_method | total_amount | cashier_name | ingestion_timestamp | source_filename | **Notes** |
|----------|----------------------|-----------|----------------|--------------|----------------|--------------|--------------|---------------------|-----------------|-----------|
| INS-001 | 2025-06-15 11:00:00 | S001 | **NULL** | 1 item | cash | 19.99 | Bob Jones | current_timestamp() | instore_orders_1.csv | Anonymous → 'in-store' sentinel |
| INS-002 | 2025-06-16 12:00:00 | S001 | bob@example.com | 2 items | credit_card | 84.96 | Jane Doe | current_timestamp() | instore_orders_2.csv | Customer provided email |

**Items details:**
- **INS-001**: `[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":1,"unit_price":19.99}]`
- **INS-002**: `[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":3,"unit_price":19.99},{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]`

## Expected Silver Results

After running silver transformations:

- **silver.categories**: 3 rows (all pass through)
- **silver.stores**: 1 row (all pass through)
- **silver.books**: **2 rows** (only valid ISBNs with non-empty titles)
- **silver.customers**: **2 rows** (alice@example.com with "Alice New" data, bob@example.com)
- **silver.orders**: **4 rows** (2 online + 2 in-store, with sentinel values)
  - ONL orders: `store_nbr = 'online'`
  - INS-001: `customer_email = 'in-store'`
  - INS-002: `customer_email = 'bob@example.com'`
- **silver.order_items**: **5 rows** (exploded from JSON, title dropped)
  - ONL-001/online: 1 line item
  - ONL-002/online: 1 line item
  - INS-001/in-store: 1 line item
  - INS-002/in-store: 2 line items

## Testing Focus

- **Data quality filtering**: Books with invalid ISBN or empty title are rejected
- **Whitespace trimming**: Leading/trailing spaces are removed
- **Most recent logic**: Customer data comes from the latest order
- **Sentinel values**: `'in-store'` and `'online'` fill nullable composite key parts
- **JSON explosion**: Items array becomes separate rows, title field is dropped
