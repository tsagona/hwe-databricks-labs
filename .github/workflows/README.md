# GitHub Actions Workflows

This directory contains 5 CI/CD workflow files for automated testing:

## Workflows

| File | Purpose | Trigger |
|------|---------|---------|
| `ci.yml` | Run all tests | Push/PR to main, manual |
| `week3-tests.yml` | Week 3 SQL tests | Changes to `labs/week3/**` |
| `week4-tests.yml` | Week 4 Bronze tests | Changes to `labs/week4/**` |
| `week5-tests.yml` | Week 5 Silver tests | Changes to `labs/week5/**` |
| `week6-tests.yml` | Week 6 Gold tests | Changes to `labs/week6/**` |

## Quick Start

1. **Push your code:**
   ```bash
   git add .
   git commit -m "Your message"
   git push
   ```

2. **Check results:**
   - Go to **Actions** tab in GitHub
   - See ✅ (passed) or ❌ (failed)
   - Click for detailed logs

## Documentation

See [GITHUB_ACTIONS.md](../../GITHUB_ACTIONS.md) in the root directory for complete documentation including:
- How workflows work
- Debugging failed tests
- Running tests locally
- Best practices
- Troubleshooting

## Test Requirements

All workflows require:
- `requirements-test.txt` (pytest, pyspark, delta-spark)
- `scripts/setup_test_tables.py` (table creation)
- Test files in `tests/` directory

These files are already included in this repository.
