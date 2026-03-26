# Week 4 Test Data

This document shows the test data that is automatically created by the test fixtures before each test runs. All data is created as temporary views that simulate CSV files.

## stores_raw

2 bookstore locations:

| store_nbr | name | address | city | state | zip | ingestion_timestamp | source_filename |
|-----------|------|---------|------|-------|-----|---------------------|-----------------|
| S001 | Downtown Books | 100 Main St | Springfield | IL | 62701 | current_timestamp() | stores.csv |
| S002 | Airport Books | 200 Terminal Dr | Springfield | IL | 62702 | current_timestamp() | stores.csv |

## categories_raw

3-level category hierarchy (Category → Genre → Subgenre):

| category_id | category_name | parent_category_id | ingestion_timestamp | source_filename |
|-------------|---------------|-------------------|---------------------|-----------------|
| 1 | Fiction | *(empty string)* | current_timestamp() | categories.csv |
| 3 | Science Fiction | 1 | current_timestamp() | categories.csv |
| 11 | Space Opera | 3 | current_timestamp() | categories.csv |

**Hierarchy:**
- Fiction (top-level category)
  - Science Fiction (genre)
    - Space Opera (subgenre)

## books_raw

2 books in the Space Opera subgenre:

| isbn | title | author | category_id | ingestion_timestamp | source_filename |
|------|-------|--------|-------------|---------------------|-----------------|
| 978-0-00-000001-1 | Test Book One | Author A | 11 | current_timestamp() | books.csv |
| 978-0-00-000002-2 | Test Book Two | Author B | 11 | current_timestamp() | books.csv |

## online_orders_raw

1 online order with customer information:

| order_id | order_timestamp | customer_email | customer_name | customer_address | customer_city | customer_state | customer_zip | items (JSON) | payment_method | total_amount | ingestion_timestamp | source_filename |
|----------|----------------|----------------|---------------|------------------|---------------|----------------|--------------|--------------|----------------|--------------|---------------------|-----------------|
| ONL-001 | 2025-06-15 10:00:00 | alice@example.com | Alice Smith | 123 Elm St | Springfield | IL | 62701 | `[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":2,"unit_price":19.99}]` | credit_card | 39.98 | current_timestamp() | online_orders_1.csv |

**Items JSON structure:**
```json
[
  {
    "isbn": "978-0-00-000001-1",
    "title": "Test Book One",
    "quantity": 2,
    "unit_price": 19.99
  }
]
```

## instore_orders_raw

1 in-store order (anonymous customer):

| order_id | transaction_timestamp | store_nbr | customer_email | items (JSON) | payment_method | total_amount | cashier_name | ingestion_timestamp | source_filename |
|----------|----------------------|-----------|----------------|--------------|----------------|--------------|--------------|---------------------|-----------------|
| INS-001 | 2025-06-15 11:00:00 | S001 | NULL | `[{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]` | cash | 24.99 | Bob Jones | current_timestamp() | instore_orders_1.csv |

**Items JSON structure:**
```json
[
  {
    "isbn": "978-0-00-000002-2",
    "title": "Test Book Two",
    "quantity": 1,
    "unit_price": 24.99
  }
]
```

## Testing Notes

- **INSERT OVERWRITE tests**: Use stores, categories, and books (static reference data)
- **MERGE INTO tests**: Use online_orders and instore_orders (transactional data)
- **Idempotency tests**: Run MERGE twice to verify no duplicates
- **Audit columns**: All tables should add `ingestion_timestamp` and `source_filename`
