# Week 3 Test Data

This document shows the test data that is automatically created by the test fixtures before each test runs.

**Domain:** Employee/HR data (different from the bookstore domain used in weeks 4-6)

## week3_testing.employees

6 employees with a mix of valid and invalid email formats, various salaries, and hire dates:

| employee_id | name | email | department | salary | hire_date |
|-------------|------|-------|------------|--------|-----------|
| EMP-001 | John Smith | john.smith@company.com | Engineering | $85,000.00 | CURRENT_DATE - 365 days (~1 year ago) |
| EMP-002 | Jane Doe | jane.doe@company.com | Sales | $65,000.00 | CURRENT_DATE - 730 days (~2 years ago) |
| EMP-003 | Bob Wilson | invalid-email | Marketing | $55,000.00 | CURRENT_DATE - 1095 days (~3 years ago) |
| EMP-004 | Alice Johnson | alice.johnson@company.com | Engineering | $120,000.00 | CURRENT_DATE - 180 days (~6 months ago) |
| EMP-005 | Charlie Brown | *(empty string)* | HR | $45,000.00 | CURRENT_DATE - 1825 days (~5 years ago) |
| EMP-006 | Diana Prince | NULL | Sales | $75,000.00 | CURRENT_DATE - 10 days |

**Data notes:**
- **Valid emails:** 3 employees (EMP-001, EMP-002, EMP-004)
- **Invalid emails:** 1 missing @ symbol (EMP-003), 1 empty string (EMP-005), 1 NULL (EMP-006)
- **Salary range:** $45,000 to $120,000
- **Recent hires (< 30 days):** 1 employee (EMP-006)
- **Departments:** Engineering (2), Sales (2), Marketing (1), HR (1)

**Used for testing:**
- Email validation (contains @)
- Email non-empty checks (exclude NULL and empty string)
- Salary range filtering ($50k-$100k should return 3 employees)
- Date filtering (recent hires in last 30 days)
- Aggregations (average salary by department)
- Counting queries

## week3_testing.departments

4 departments with managers and budgets:

| dept_id | dept_name | manager_name | budget |
|---------|-----------|--------------|--------|
| DEPT-001 | Engineering | Sarah Connor | $500,000.00 |
| DEPT-002 | Sales | Michael Scott | $300,000.00 |
| DEPT-003 | Marketing | Don Draper | $250,000.00 |
| DEPT-004 | HR | Toby Flenderson | $150,000.00 |

**Data notes:**
- Can be used for JOIN exercises (though not required for Week 3)
- Demonstrates department reference data
- Budget range: $150k to $500k

## Expected Query Results

### Valid Email Filter
- Should return: EMP-001, EMP-002, EMP-004 (3 rows)
- Filters for emails containing `@`

### Count Employees
- Should return: 6

### Salary Range ($50k - $100k)
- Should return: EMP-001 ($85k), EMP-002 ($65k), EMP-003 ($55k) (3 rows)

### Recent Hires (last 30 days)
- Should return: EMP-006 (1 row)

### Average Salary by Department
- Engineering: $102,500 (average of $85k and $120k)
- Sales: $70,000 (average of $65k and $75k)
- Marketing: $55,000
- HR: $45,000

### Employees with Valid Email (non-NULL, non-empty)
- Should return: EMP-001, EMP-002, EMP-003, EMP-004 (4 rows)
- Excludes EMP-005 (empty) and EMP-006 (NULL)
