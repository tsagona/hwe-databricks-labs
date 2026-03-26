# HWE Databricks Labs - Student Edition

Welcome to the Databricks Data Engineering course! This repository contains hands-on labs for learning data engineering fundamentals using Databricks, Spark SQL, and the medallion architecture.

## Course Structure

This is a 6-week course covering:

- **Week 1**: Introduction to Databricks, Spark, and SQL
- **Week 2**: File formats, Delta Lake, and time travel
- **Week 3**: Git workflows, testing with pytest, and CI/CD
- **Week 4**: Bronze layer — raw data ingestion
- **Week 5**: Silver layer — data cleaning and normalization
- **Week 6**: Gold layer — dimensional modeling

## Repository Structure

```
├── labs/
│   ├── data-model.md          # Reference: complete data model spec
│   ├── week2/                 # Delta Lake and time travel
│   ├── week3/                 # Git, testing, and CI/CD
│   ├── week4/                 # Bronze layer exercises
│   ├── week5/                 # Silver layer exercises
│   └── week6/                 # Gold layer exercises
├── tests/
│   ├── test_week3_sql.py      # Week 3 SQL tests
│   ├── test_week4_bronze.py   # Week 4 bronze tests
│   ├── test_week5_silver.py   # Week 5 silver tests
│   ├── test_week6_gold.py     # Week 6 gold tests
│   ├── README.md              # Testing framework overview
│   └── WRITING_TESTS.md       # Complete guide to writing tests
└── .github/workflows/
    └── ci.yml                 # GitHub Actions for automated testing
```

## Getting Started

### Prerequisites

- A Databricks workspace account
- A GitHub account
- Basic SQL knowledge

### Setup

1. **Fork this repository** to your GitHub account
2. **Clone into Databricks**:
   - In Databricks, click Workspace → Repos → Add Repo
   - Paste your forked repo URL
   - Click Create

3. **Install test dependencies** (for running tests locally):
   ```bash
   pip install -r requirements-test.txt
   ```

## How to Complete the Labs

Each week follows the same pattern:

### 1. Complete the Lab Notebook

Each week has a `weekN_lab.ipynb` notebook with exercises marked as:
```sql
-- TODO: Write SQL to...
```

Fill in the TODOs with working SQL queries.

### 2. Create Required Tables

Before running your lab queries, run the `create_*.ipynb` notebook to set up tables:
- Week 3: `create_week3_tables.ipynb`
- Week 4: `create_bronze.ipynb`
- Week 5: `create_silver.ipynb`
- Week 6: `create_gold.ipynb`

These notebooks also have TODOs for you to complete the DDL (table definitions).

### 3. Write Tests

Each week has a test file in `tests/test_weekN_*.py`. Complete the test functions:

```python
def test_something(spark):
    """Verify something works correctly."""
    # TODO: Implement this test
    pass
```

See `tests/WRITING_TESTS.md` for a complete guide on writing tests.

### 4. Run Tests Locally

```bash
# Run all tests
pytest tests/ -v

# Run specific week
pytest tests/test_week4_bronze.py -v

# Run single test
pytest tests/test_week4_bronze.py::test_stores_insert_overwrite -v
```

### 5. Submit via Pull Request

1. Create a feature branch: `week4-yourname`
2. Commit your changes
3. Push to GitHub
4. Create a pull request
5. GitHub Actions will automatically run your tests
6. Tests must pass before merging ✅

## Git Workflow

**Always work in feature branches, never commit directly to main:**

```bash
# Create a branch
git checkout -b week4-myname

# Make changes, then:
git add labs/week4/week4_lab.ipynb tests/test_week4_bronze.py
git commit -m "Complete Week 4 bronze layer exercises"
git push -u origin week4-myname

# Then create a pull request on GitHub
```

## Data Model

The labs build a bookstore data pipeline:
- **Bronze**: Raw CSV data (stores, books, categories, orders)
- **Silver**: Cleaned, normalized 3NF tables
- **Gold**: Star schema for analytics (dimensions + fact tables)

See `labs/data-model.md` for complete table schemas and relationships.

## Testing

This course emphasizes testing as a core data engineering practice:

- **Week 3**: Learn pytest basics, test Python functions
- **Week 4-6**: Test SQL transformations using the tagging framework

All test setup (fixtures, test data) is provided. You only need to write the test assertions.

See `tests/WRITING_TESTS.md` for detailed guidance.

## Automated Testing

**Tests run automatically when you push code!**

Push your work to GitHub and tests will run in the cloud:
- ✅ Green check = tests pass, ready to merge
- ❌ Red X = tests failed, click to see why

**Quick guide:** See [TESTING.md](TESTING.md) for how to use automated testing

**Complete documentation:** See [GITHUB_ACTIONS.md](GITHUB_ACTIONS.md) for detailed setup, troubleshooting, and advanced usage

## Getting Help

- **Test failures**: Read the error message carefully — it shows exactly what failed
- **SQL errors**: Check the cell tag matches what your test expects
- **Setup issues**: Make sure you ran the `create_*.ipynb` notebook first
- **General questions**: Check `tests/README.md` and `tests/WRITING_TESTS.md`

## Tips for Success

1. **Read the docstrings** — test function docstrings tell you what to verify
2. **Run tests often** — don't wait until you've finished everything
3. **Check the data model** — `labs/data-model.md` has all table schemas
4. **Start simple** — get one test passing before moving to the next
5. **Use Git branches** — one branch per week, submit via PR

Good luck! 🚀
