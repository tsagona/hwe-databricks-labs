# Data Generation Scripts

Generate CSV test data for the bronze layer. All scripts are deterministic — the same inputs always produce the same output.

Books and stores are standalone. Order scripts depend on the books CSV (and stores CSV for in-store) to ensure referential integrity — every ISBN and store_nbr in the generated orders exists in the corresponding reference data.

## Scripts

### gen_bronze_books.py

Generates books with a mix of valid and invalid rows. Invalid rows fail one or more of the silver data quality checks (null/empty ISBN, bad ISBN format, null/empty title).

```
python gen_bronze_books.py --valid <N> --invalid <N> [-o OUTPUT]
```

| Argument | Description |
|---|---|
| `--valid` | Number of rows that pass silver DQ checks |
| `--invalid` | Number of rows that fail at least one check |
| `-o` | Output file path (default: `bronze_books.csv`) |

### gen_bronze_stores.py

Generates store reference data.

```
python gen_bronze_stores.py --count <N> [-o OUTPUT]
```

| Argument | Description |
|---|---|
| `--count` | Number of stores to generate |
| `-o` | Output file path (default: `bronze_stores.csv`) |

### gen_bronze_online_orders.py

Generates online orders. Each order has 1-5 line items, a customer (from a static pool of 100), and a computed `total_amount`. Only valid ISBNs from the books CSV are referenced.

```
python gen_bronze_online_orders.py --count <N> --books <books.csv> [--seed <N>] [-o OUTPUT]
```

| Argument | Description |
|---|---|
| `--count` | Number of orders to generate |
| `--books` | Path to the books CSV (only valid ISBNs are used) |
| `--seed` | Random seed (default: 42) |
| `-o` | Output file path (default: `bronze_online_orders.csv`) |

### gen_bronze_instore_orders.py

Generates in-store orders. Each order has 1-5 line items, a store_nbr from the stores CSV, a cashier name, and optionally a customer email (~30% of orders). Only valid ISBNs from the books CSV are referenced.

```
python gen_bronze_instore_orders.py --count <N> --books <books.csv> --stores <stores.csv> [--seed <N>] [-o OUTPUT]
```

| Argument | Description |
|---|---|
| `--count` | Number of orders to generate |
| `--books` | Path to the books CSV (only valid ISBNs are used) |
| `--stores` | Path to the stores CSV (store_nbr values are sampled) |
| `--seed` | Random seed (default: 42) |
| `-o` | Output file path (default: `bronze_instore_orders.csv`) |

## Example: generate a full dataset

```bash
cd data_generation_scripts

python gen_bronze_books.py --valid 100 --invalid 10 -o bronze_books.csv
python gen_bronze_stores.py --count 8 -o bronze_stores.csv
python gen_bronze_online_orders.py --count 500 --books bronze_books.csv -o bronze_online_orders.csv
python gen_bronze_instore_orders.py --count 300 --books bronze_books.csv --stores bronze_stores.csv -o bronze_instore_orders.csv
```

## Upload to Databricks

`generate-and-upload-data.sh` generates a full dataset and uploads it to DBFS in one step. Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) to be installed and configured with a profile (`databricks configure --profile <name>`).

```bash
./generate-and-upload-data.sh --profile <name> [--dbfs-path <path>] [--seed <N>]
```

| Argument | Description |
|---|---|
| `--profile` | **(required)** Databricks CLI profile to use |
| `--dbfs-path` | DBFS destination (default: `dbfs:/FileStore/hwe-data`) |
| `--seed` | Random seed for order scripts (default: 42) |

Files are uploaded to subdirectories matching the week 4 lab `read_files()` pattern:

```
<dbfs-path>/books/books.csv
<dbfs-path>/stores/stores.csv
<dbfs-path>/online_orders/online_orders.csv
<dbfs-path>/instore_orders/instore_orders.csv
```

## Supporting files

- **customers.py** — Static pool of 100 customers (email, name, address) shared by both order scripts. Not run directly.
