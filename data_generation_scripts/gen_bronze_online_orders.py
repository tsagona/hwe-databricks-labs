"""Generate bronze online orders CSV test data.

Each order references only valid ISBNs from the provided books CSV (rows that
pass the silver DQ checks). This ensures referential integrity through the
pipeline.

Usage:
    python gen_bronze_online_orders.py --count <N> --books <books.csv> [--start <TS>] [--end <TS>] [--seed <N>] [-o OUTPUT]
"""

import argparse
import csv
import hashlib
import json
import random
import re
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from customers import CUSTOMERS

ISBN_REGEX = re.compile(r"^978-[0-9]-[0-9]{2}-[0-9]{6}-[0-9]$")

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gift_card"]


def load_valid_books(books_path):
    """Read books CSV and return only rows that pass silver DQ checks."""
    books = []
    with open(books_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            isbn = (row["isbn"] or "").strip()
            title = (row["title"] or "").strip()
            if isbn and title and ISBN_REGEX.match(isbn):
                books.append({"isbn": isbn, "title": title})
    if not books:
        raise SystemExit(f"No valid books found in {books_path}")
    return books


def book_price(isbn):
    """Deterministic price for a given ISBN, derived from its hash."""
    h = int(hashlib.sha256(isbn.encode()).hexdigest(), 16)
    return round(5.99 + (h % 4400) / 100, 2)  # $5.99 - $49.99


def random_timestamp(start, end):
    """Random datetime between start and end."""
    delta = end - start
    offset = random.random() * delta.total_seconds()
    dt = start + timedelta(seconds=offset)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def gen_order(order_num, customers, books, ts_start, ts_end):
    """Generate one online order row."""
    customer = random.choice(customers)
    num_items = random.randint(1, 5)
    selected_books = random.sample(books, min(num_items, len(books)))

    items = []
    total = 0.0
    for book in selected_books:
        qty = random.randint(1, 3)
        price = book_price(book["isbn"])
        items.append({
            "isbn": book["isbn"],
            "title": book["title"],
            "quantity": qty,
            "unit_price": price,
        })
        total += qty * price

    return {
        "order_id": f"ONL-{order_num:06d}",
        "order_timestamp": random_timestamp(ts_start, ts_end),
        "customer_email": customer["email"],
        "customer_name": customer["name"],
        "customer_address": customer["address"],
        "customer_city": customer["city"],
        "customer_state": customer["state"],
        "customer_zip": customer["zip"],
        "items": json.dumps(items),
        "payment_method": random.choice(PAYMENT_METHODS),
        "total_amount": f"{total:.2f}",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate bronze online orders CSV."
    )
    parser.add_argument("--count", type=int, required=True, help="Number of orders to generate")
    parser.add_argument(
        "--books", required=True,
        help="Path to books CSV (ISBNs will be sampled from valid rows)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--start",
        help="Start timestamp for orders (YYYY-MM-DD HH:MM:SS). Default: 5 minutes ago.",
    )
    parser.add_argument(
        "--end",
        help="End timestamp for orders (YYYY-MM-DD HH:MM:SS). Default: now.",
    )
    parser.add_argument(
        "-o", "--output", default="bronze_online_orders.csv",
        help="Output CSV file path (default: bronze_online_orders.csv)",
    )
    args = parser.parse_args()

    if args.count < 0:
        parser.error("Count must be non-negative")

    random.seed(args.seed)

    books = load_valid_books(args.books)

    # Sample a subset of customers (~1 per 3 orders, min 1)
    num_customers = max(1, args.count // 3)
    customers = random.sample(CUSTOMERS, min(num_customers, len(CUSTOMERS)))

    now = datetime.now()
    ts_start = datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S") if args.start else now - timedelta(minutes=5)
    ts_end = datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S") if args.end else now

    fieldnames = [
        "order_id", "order_timestamp", "customer_email", "customer_name",
        "customer_address", "customer_city", "customer_state", "customer_zip",
        "items", "payment_method", "total_amount",
    ]

    rows = [gen_order(i + 1, customers, books, ts_start, ts_end)
            for i in range(args.count)]

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {args.count} orders ({len(customers)} distinct customers, "
          f"{len(books)} valid books) to {args.output}")


if __name__ == "__main__":
    main()
