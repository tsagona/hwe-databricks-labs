# Testing Your Code

This repository has automated tests to check your work. Tests run automatically when you push code.

## Quick Start

### 1. Push Your Code
```bash
git add labs/week3/week3_lab.ipynb
git commit -m "Complete Week 3"
git push
```

### 2. Check Results
1. Go to your GitHub repository
2. Click the **Actions** tab at the top
3. See your test results:
   - ✅ **Green checkmark** = All tests passed! 🎉
   - ❌ **Red X** = Tests failed - click to see why

## What Gets Tested

Each week has its own test suite:

| Week | What's Tested | Time |
|------|---------------|------|
| **Week 3** | SQL queries (employees/HR) | ~1 min |
| **Week 4** | Bronze layer ingestion | ~2 min |
| **Week 5** | Silver layer normalization | ~2.5 min |
| **Week 6** | Gold layer star schema | ~3 min |

**Smart testing:** Only the weeks you changed will run! If you only edit Week 3, only Week 3 tests run.

## Running Tests Locally

You can run tests on your computer before pushing:

```bash
# One-time setup
pip install -r requirements-test.txt

# Run all tests
pytest tests/ -v

# Run just Week 3
pytest tests/test_week3_sql.py -v

# Run a single test
pytest tests/test_week3_sql.py::test_valid_email_filter -v
```

## Reading Test Results

### When Tests Pass ✅
```
✅ Week 3 Tests
   test_valid_email_filter PASSED
   test_count_employees PASSED
   ===== 6 passed in 12.34s =====
```

You're done! Move on to the next exercise.

### When Tests Fail ❌
```
❌ Week 3 Tests
   FAILED test_valid_email_filter
   AssertionError: assert 0 == 3
```

Click on the failed test to see:
- **What failed:** Which test didn't pass
- **Why it failed:** What the error message says
- **Where to look:** The line number in your notebook

### Common Errors

#### "Could not find cell matching: valid_email_filter"
**Problem:** Missing the `-- @test:valid_email_filter` tag in your SQL cell

**Fix:** Add the tag to your SQL:
```sql
-- @test:valid_email_filter
SELECT * FROM week3_testing.employees
WHERE email LIKE '%@%'
```

#### "AssertionError: assert 0 == 3"
**Problem:** Your query returned 0 rows but should return 3

**Fix:** Check your SQL logic. Did you:
- Use the right table?
- Use the right WHERE clause?
- Actually write the SQL (not just a TODO)?

#### "Table or view not found"
**Problem:** Tests expect you to use specific table/view names

**Fix:** Check the lab instructions for the exact names to use

## Getting Help

1. **Read the error message** - it usually tells you exactly what's wrong
2. **Check the lab instructions** - make sure you're using the right table/view names
3. **Compare with examples** - look at the test data in `tests/weekN_test_data.md`
4. **Ask for help** - show your instructor the test output

## Advanced: Feature Branches

Professional workflow (optional):

```bash
# Create a branch for your work
git checkout -b week3-solution

# Work on exercises, commit, push
git push origin week3-solution

# Open a Pull Request on GitHub
# Tests run automatically before merge!
```

This keeps your `main` branch clean and lets you see test results before merging.

---

## More Information

- **Complete guide:** See [GITHUB_ACTIONS.md](GITHUB_ACTIONS.md)
- **Workflow details:** See [.github/workflows/README.md](.github/workflows/README.md)
- **Test writing guide:** See [tests/README.md](tests/README.md) (for instructors)
