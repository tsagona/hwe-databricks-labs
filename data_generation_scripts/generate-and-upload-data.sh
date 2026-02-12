#!/bin/bash
#
# Generate bronze layer CSV test data and upload it to a Databricks workspace.
#
# Prerequisites:
#   - Databricks CLI installed and configured with a profile
#     (via 'databricks configure --profile <name>')
#   - Python 3 available
#
# Usage:
#   ./upload-data.sh --profile <name> [--dbfs-path <path>] [--seed <N>]
#
# Defaults:
#   --dbfs-path   dbfs:/FileStore/hwe-data
#   --seed        42

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Defaults ──────────────────────────────────────────────────────────────────

DBFS_PATH="dbfs:/FileStore/hwe-data"
SEED=42
PROFILE=""

# ── Parse args ────────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)   PROFILE="$2";   shift 2 ;;
        --dbfs-path) DBFS_PATH="$2"; shift 2 ;;
        --seed)      SEED="$2";      shift 2 ;;
        *)           echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [ -z "$PROFILE" ]; then
    echo "Error: --profile is required (e.g. --profile hwe)"
    echo "Configure one with: databricks configure --profile <name>"
    exit 1
fi

DB="databricks --profile $PROFILE"

# ── Check for Databricks CLI ──────────────────────────────────────────────────

if ! command -v databricks &>/dev/null; then
    echo "Error: 'databricks' CLI not found. Install it first:"
    echo "  https://docs.databricks.com/dev-tools/cli/install.html"
    exit 1
fi

# ── Generate data ─────────────────────────────────────────────────────────────

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

TS_START=$(date -v-365d "+%Y-%m-%d %H:%M:%S")
TS_END=$(date -v-1d "+%Y-%m-%d %H:%M:%S")

echo "Generating data (seed=$SEED, start=$TS_START, end=$TS_END)..."

python3 "$SCRIPT_DIR/gen_bronze_books.py" \
    --valid 100 --invalid 10 \
    -o "$TMPDIR/books.csv"

python3 "$SCRIPT_DIR/gen_bronze_stores.py" \
    --count 8 \
    -o "$TMPDIR/stores.csv"

python3 "$SCRIPT_DIR/gen_bronze_online_orders.py" \
    --count 500000 --seed "$SEED" \
    --books "$TMPDIR/books.csv" \
    --start "$TS_START" --end "$TS_END" \
    -o "$TMPDIR/online_orders.csv"

python3 "$SCRIPT_DIR/gen_bronze_instore_orders.py" \
    --count 300000 --seed "$SEED" \
    --books "$TMPDIR/books.csv" \
    --stores "$TMPDIR/stores.csv" \
    --start "$TS_START" --end "$TS_END" \
    -o "$TMPDIR/instore_orders.csv"

# ── Upload to DBFS ────────────────────────────────────────────────────────────

FILES=(
    "books.csv|books|books.csv"
    "stores.csv|stores|stores.csv"
    "online_orders.csv|online_orders|online_orders.csv"
    "instore_orders.csv|instore_orders|instore_orders.csv"
)
TOTAL=${#FILES[@]}

echo ""
echo "Uploading $TOTAL files to $DBFS_PATH ..."
echo ""

for i in "${!FILES[@]}"; do
    IFS='|' read -r LOCAL_NAME SUBDIR REMOTE_NAME <<< "${FILES[$i]}"
    STEP=$((i + 1))
    FILE_SIZE=$(du -h "$TMPDIR/$LOCAL_NAME" | cut -f1 | xargs)

    FILLED=$((STEP * 20 / TOTAL))
    EMPTY=$((20 - FILLED))
    BAR=$(printf '#%.0s' $(seq 1 $FILLED))$(printf '.%.0s' $(seq 1 $EMPTY))

    printf "\r  [%-20s] %d/%d  %-25s (%s)" "$BAR" "$STEP" "$TOTAL" "$REMOTE_NAME" "$FILE_SIZE"

    $DB fs mkdirs "$DBFS_PATH/$SUBDIR"
    $DB fs cp "$TMPDIR/$LOCAL_NAME" "$DBFS_PATH/$SUBDIR/$REMOTE_NAME" --overwrite
done

echo ""
echo ""
echo "Done. Files uploaded to:"
for f in "${FILES[@]}"; do
    IFS='|' read -r _ SUBDIR REMOTE_NAME <<< "$f"
    echo "  $DBFS_PATH/$SUBDIR/$REMOTE_NAME"
done
echo ""
echo "Use these paths in the week 4 lab read_files() calls."
