"""Generate bronze stores CSV test data.

Usage:
    python gen_bronze_stores.py --count <N> [-o OUTPUT]
"""

import argparse
import csv
import random

STORE_TYPES = ["Main Street", "Downtown", "Westside", "Eastside", "North",
               "South", "Central", "Plaza", "Mall", "Village", "Harbor",
               "Lakeside", "Uptown", "Midtown", "Garden", "Park", "Campus"]

STREET_NAMES = ["Main St", "Oak Ave", "Elm St", "Maple Dr", "Cedar Ln",
                "Pine Rd", "Broadway", "Market St", "Park Ave", "Lake Blvd",
                "River Rd", "Hill St", "Sunset Blvd", "First Ave", "Second St"]

CITIES_BY_STATE = {
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento", "San Jose"],
    "NY": ["New York", "Buffalo", "Rochester", "Albany", "Syracuse"],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio", "Fort Worth"],
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville", "St Petersburg"],
    "IL": ["Chicago", "Springfield", "Naperville", "Evanston", "Peoria"],
    "WA": ["Seattle", "Tacoma", "Spokane", "Bellevue", "Olympia"],
    "MA": ["Boston", "Cambridge", "Worcester", "Salem", "Springfield"],
    "CO": ["Denver", "Boulder", "Colorado Springs", "Fort Collins", "Aurora"],
    "OR": ["Portland", "Eugene", "Salem", "Bend", "Medford"],
    "PA": ["Philadelphia", "Pittsburgh", "Harrisburg", "Allentown", "Erie"],
}


def random_zip():
    return f"{random.randint(10000, 99999)}"


def gen_store(store_num):
    state = random.choice(list(CITIES_BY_STATE.keys()))
    city = random.choice(CITIES_BY_STATE[state])
    return {
        "store_nbr": f"STR-{store_num:04d}",
        "store_name": f"{random.choice(STORE_TYPES)} Books",
        "store_address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
        "store_city": city,
        "store_state": state,
        "store_zip": random_zip(),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate bronze stores CSV."
    )
    parser.add_argument("--count", type=int, required=True, help="Number of stores to generate")
    parser.add_argument(
        "-o", "--output", default="bronze_stores.csv",
        help="Output CSV file path (default: bronze_stores.csv)",
    )
    args = parser.parse_args()

    if args.count < 0:
        parser.error("Count must be non-negative")

    random.seed(args.count)

    rows = [gen_store(i + 1) for i in range(args.count)]

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["store_nbr", "store_name", "store_address",
                           "store_city", "store_state", "store_zip"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {args.count} rows to {args.output}")


if __name__ == "__main__":
    main()
