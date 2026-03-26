# GitHub Actions CI/CD Setup

This repository uses GitHub Actions to automatically run tests whenever you push code or create pull requests.

## Available Workflows

We have **5 separate workflow files** that run different test suites:

### 1. **All Tests** (`ci.yml`)
- **Triggers:** Push to main, PRs to main, or manual trigger
- **What it does:** Runs the complete test suite (all weeks)
- **Use case:** Final validation before merging to main

### 2. **Week 3 Tests** (`week3-tests.yml`)
- **Triggers:** Changes to `labs/week3/**` or `tests/test_week3_sql.py`
- **What it does:** Tests SQL queries and pytest basics (Employee/HR domain)
- **Tables created:** `week3_testing.employees`, `week3_testing.departments`
- **Use case:** Fast feedback when working on Week 3 exercises

### 3. **Week 4 Tests** (`week4-tests.yml`)
- **Triggers:** Changes to `labs/week4/**` or `tests/test_week4_bronze.py`
- **What it does:** Tests Bronze layer ingestion (CSV → Delta)
- **Tables created:** All 5 bronze tables (categories, stores, books, online_orders, instore_orders)
- **Use case:** Validate Bronze layer transformations and MERGE logic

### 4. **Week 5 Tests** (`week5-tests.yml`)
- **Triggers:** Changes to `labs/week5/**` or `tests/test_week5_silver.py`
- **What it does:** Tests Silver layer normalization (3NF)
- **Tables created:** All Bronze + all 6 Silver tables
- **Use case:** Validate data cleaning, normalization, and quality checks

### 5. **Week 6 Tests** (`week6-tests.yml`)
- **Triggers:** Changes to `labs/week6/**` or `tests/test_week6_gold.py`
- **What it does:** Tests Gold layer star schema (dimensions + fact)
- **Tables created:** All Silver + all 5 Gold tables
- **Use case:** Validate dimensional modeling and FK lookups

---

## How It Works

### Automatic Triggers

Tests run automatically when:
- ✅ You **push** to the `main` branch
- ✅ You **open a PR** to the `main` branch
- ✅ You **update a PR** by pushing new commits

**Path-based triggering:** Week-specific workflows only run when you change files in that week's directory. This saves CI minutes and gives you faster feedback.

**Example:**
```bash
# If you only change labs/week3/week3_lab.ipynb
# → Only week3-tests.yml runs (not weeks 4-6)

# If you change tests/notebook_utils.py
# → All week-specific workflows run (they all depend on it)
```

### Manual Triggers

You can also run the **full test suite** manually:

1. Go to **Actions** tab in GitHub
2. Select **"All Tests (Full Suite)"** workflow
3. Click **"Run workflow"** → Choose branch → **"Run workflow"**

---

## Setup Instructions

### Prerequisites

✅ You've forked/cloned this repository
✅ GitHub Actions is enabled (it's on by default for public repos)

### Step 1: Ensure Required Files Exist

Check that these files are in your repo:

```bash
.github/workflows/
├── ci.yml              # All tests
├── week3-tests.yml     # Week 3 only
├── week4-tests.yml     # Week 4 only
├── week5-tests.yml     # Week 5 only
└── week6-tests.yml     # Week 6 only

requirements-test.txt   # Python dependencies
scripts/setup_test_tables.py  # Table setup script
```

All files are already included in this repository. ✅

### Step 2: Push Your Work

```bash
git add .
git commit -m "Complete Week 3 exercises"
git push origin main
```

### Step 3: Check Test Results

1. Go to your GitHub repository
2. Click the **Actions** tab at the top
3. You'll see your workflow runs with ✅ (passed) or ❌ (failed)
4. Click on a run to see detailed logs

**Example output:**
```
✅ Week 3 Tests (SQL & Testing)
   Run Week 3 tests
   ===== test session starts =====
   tests/test_week3_sql.py::test_valid_email_filter PASSED
   tests/test_week3_sql.py::test_count_employees PASSED
   ===== 6 passed in 12.34s =====
```

---

## Workflow Architecture

Each workflow follows this structure:

```yaml
1. Checkout code         # Get your latest code
2. Set up Python 3.11   # Install Python
3. Install dependencies  # Install pytest, pyspark, delta-spark
4. Set up Spark         # Configure local Spark environment
5. Create tables        # Create Delta tables for testing
6. Run tests            # Execute pytest
7. Test summary         # Report results
```

### Local Spark in GitHub Actions

Tests run using **PySpark in local mode** inside the GitHub Actions runner (Ubuntu VM). This means:

- ✅ No Databricks cluster needed
- ✅ No cloud credentials needed
- ✅ Tests run in ~2-3 minutes
- ✅ Delta tables are created in-memory
- ✅ Tables are destroyed after tests finish (fresh start each run)

**Trade-off:** GitHub Actions tests can't access your actual Databricks catalog or DBFS data. They use synthetic test data from fixtures instead.

---

## Debugging Failed Tests

### Reading Test Output

When a test fails, click on the red ❌ in the Actions tab, then click "Run tests" to see:

```
FAILED tests/test_week4_bronze.py::test_books_insert_overwrite - AssertionError: assert 0 == 2
```

This tells you:
- **File:** `tests/test_week4_bronze.py`
- **Test:** `test_books_insert_overwrite`
- **Error:** Expected 2 rows but got 0

### Common Issues

#### Issue: "Could not find cell matching: bronze_books_load"

**Cause:** Your SQL cell is missing the `-- @test:bronze_books_load` tag

**Fix:** Add the tag to your SQL cell:
```sql
-- @test:bronze_books_load
INSERT OVERWRITE bronze.books
SELECT * FROM books_raw
```

#### Issue: "AssertionError: assert 0 == 2"

**Cause:** Your query returned no rows (expected 2)

**Fix:** Check your SQL logic. Did you forget to implement the transformation?

#### Issue: "Table or view not found: bronze.books"

**Cause:** Test is trying to read a table before it's created

**Fix:** Tests should create temp views or read from fixtures, not expect pre-existing data

---

## Running Tests Locally

You can run the same tests locally before pushing:

### Setup (one time)

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements-test.txt
```

### Run tests

```bash
# All tests
pytest tests/ -v

# Single week
pytest tests/test_week3_sql.py -v

# Single test
pytest tests/test_week3_sql.py::test_valid_email_filter -v
```

---

## CI/CD Best Practices

### ✅ DO

- **Commit often** — small commits are easier to debug if tests fail
- **Check test output** — read the error messages, they're usually clear
- **Run locally first** — catch errors before pushing
- **Use descriptive commit messages** — helps you find when tests broke

### ❌ DON'T

- **Don't push untested code** — run `pytest` locally first
- **Don't ignore failing tests** — fix them before moving on
- **Don't commit to main directly** — use feature branches + PRs (best practice)
- **Don't remove test tags** — `-- @test:tag_name` comments are required

---

## Advanced: Feature Branch Workflow

Professional workflow (optional but recommended):

```bash
# 1. Create feature branch
git checkout -b week3-exercises

# 2. Work on your exercises
# ... edit files ...

# 3. Commit and push
git add labs/week3/week3_lab.ipynb
git commit -m "Complete Week 3 exercises"
git push origin week3-exercises

# 4. Open Pull Request on GitHub
# → Tests run automatically
# → If tests pass ✅, merge to main
# → If tests fail ❌, fix and push again
```

This workflow:
- ✅ Keeps `main` branch always working
- ✅ Tests run before merge (gate)
- ✅ Easy to see what changed
- ✅ Can get feedback on PRs

---

## Troubleshooting

### Tests pass locally but fail in GitHub Actions

**Possible causes:**
1. You committed a file but forgot to push it
2. Your local test depends on environment-specific data
3. File paths are case-sensitive in Linux (GitHub Actions) but not on Mac/Windows

**Fix:** Double-check what's in your remote repo matches your local files

### Workflow doesn't trigger

**Possible causes:**
1. You pushed to a branch other than `main`
2. The changed files don't match the `paths:` filter
3. GitHub Actions is disabled for your repo

**Fix:** Check the "Actions" tab — if it says "Workflows disabled", click "Enable"

### "No space left on device" error

**Cause:** CI runner ran out of disk space (rare)

**Fix:** Re-run the workflow (GitHub will use a fresh VM)

---

## Cost and Limits

### Free Tier

GitHub Actions is **free** for public repositories with generous limits:
- ✅ 2,000 minutes/month (private repos)
- ✅ **Unlimited** minutes for public repos
- ✅ 20 concurrent jobs

### Typical Usage

- Week 3 tests: ~1 minute
- Week 4 tests: ~2 minutes
- Week 5 tests: ~2.5 minutes
- Week 6 tests: ~3 minutes
- Full suite: ~3.5 minutes

**Total per push:** ~3-4 minutes (only changed weeks run)

---

## Questions?

### Where are the test assertions?

**Students branch:** TODOs in test files
**Solution branch:** Complete assertions

### Why do tests use temp views instead of reading CSV files?

Tests use **synthetic data** created in fixtures, not real CSV files. This makes tests:
- ✅ Faster (no file I/O)
- ✅ Deterministic (same data every time)
- ✅ Portable (no dependencies on external files)

### Can I test against real Databricks?

Not in GitHub Actions (requires credentials and cluster access). For real Databricks testing, you'll run notebooks manually in the workspace.

GitHub Actions tests validate **your SQL logic**, not Databricks infrastructure.

---

## Summary

| Workflow | What It Tests | When It Runs | Duration |
|----------|---------------|--------------|----------|
| `ci.yml` | Everything | Push/PR to main, manual | ~4 min |
| `week3-tests.yml` | Week 3 SQL | Changes to week3 files | ~1 min |
| `week4-tests.yml` | Bronze layer | Changes to week4 files | ~2 min |
| `week5-tests.yml` | Silver layer | Changes to week5 files | ~2.5 min |
| `week6-tests.yml` | Gold layer | Changes to week6 files | ~3 min |

**Next step:** Push your code and watch the tests run! 🚀
