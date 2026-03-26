# Week 3 Lab: Git Workflows, Testing, and CI/CD

This week focuses on software engineering practices for data engineering:
- Git branching and pull requests
- Writing tests with pytest
- Automated testing with GitHub Actions

## Lab Structure

### SQL Testing with pytest

**File:** `week3_lab.ipynb`
SQL notebook with tagged queries demonstrating basic testing concepts:
- Filter employees by valid email format
- Count records
- Filter by salary range
- Find recent hires (date filtering)
- Calculate averages (salary by department)
- Validate non-null/non-empty fields

**Domain:** Employee/HR data (different from the bookstore domain used in weeks 4-6)

Each query is tagged with `-- @test:tag_name` so tests can extract and run them.

**Setup:** `create_week3_tables.ipynb`
Creates the `week3_testing` schema with simple employee and department tables.

**Tests:** `tests/test_week3_sql.py`
9 tests that validate the SQL queries work correctly:
- Extract SQL from tagged cells using `find_cell()`
- Run queries with `spark.sql()`
- Assert results are correct

**How to run:**
```bash
pytest tests/test_week3_sql.py -v
```

## Test Coverage

9 SQL tests demonstrating the testing framework students will use in weeks 4-6.

**Focus areas:**
- Query correctness (filtering, aggregations, sorting)
- Data validation (email format, non-null checks)
- Date filtering (recent hires)
- Numeric filtering (salary ranges)
- Aggregations by group (average salary by department)

**Note:** Week 3 uses Employee/HR domain to learn testing fundamentals. The bookstore domain is introduced starting in Week 4.

## Learning Objectives

By the end of this lab, students can:
1. Create feature branches and submit pull requests
2. Write pytest tests with assertions
3. Test SQL queries using the tagging framework
4. Read test failures and debug issues
5. Set up GitHub Actions for automated testing
6. Understand the value of CI/CD in preventing bugs

## CI/CD Setup

**File:** `ci_template.yml`
GitHub Actions workflow template. Copy to `.github/workflows/ci.yml` to enable automated testing on every pull request.

## Next Steps

In weeks 4-6, students will:
- Use the same tagging framework for bronze/silver/gold transformations
- Write more complex data validation tests
- Practice the Git workflow for all lab submissions
