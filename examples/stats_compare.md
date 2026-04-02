# Stats Compare

Compare readable statistics between two CSV files.

## Command

```bash
sqlcompare run stats tests/datasets/stats_compare/previous.csv tests/datasets/stats_compare/current.csv
```

To run only specific checks:

```bash
sqlcompare run stats tests/datasets/stats_compare/previous.csv tests/datasets/stats_compare/current.csv --checks row_count,schema
```

## Output

```text
📊 Stats comparison: previous -> current
Inputs:
  Previous: previous
  Current: current

Row counts:
 Previous  Current  Diff % Diff
        5        6     1  20.0%

Schema differences:
Schemas match on column names (4 common columns).

Column comparison:
    COL         NULL             DUP  MATCH
  notes  2 -> 1 (-50%)  3 -> 5 (66.7%)   61.1
segment   0 (no change)    4 -> 6 (50%)   72.2
 amount   0 (no change)   2 (no change)   94.4
     id   0 (no change)   0 (no change)   94.4

Summary: 3 of 4 selected check(s) found differences.
```
