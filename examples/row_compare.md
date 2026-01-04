# Row Compare

Compare rows using a dataset YAML with file inputs

## Command

```bash
sqlcompare dataset tests/datasets/row_compare/dataset.yaml
```

## Output

```text
Prepared dataset tables: sqlcompare.dataset_dataset_998aa249_previous, sqlcompare.dataset_dataset_998aa249_new
✅ Ensured schema 'sqlcompare' exists
Joining tables: sqlcompare.dataset_dataset_998aa249_previous and sqlcompare.dataset_dataset_998aa249_new
Rows only in current dataset: 1
+----+-------+--------+--------+
| id | name  | amount | status |
+----+-------+--------+--------+
| 4  | delta |   40   |  open  |
+----+-------+--------+--------+
Rows only in previous dataset: 1
+----+-------+--------+--------+
| id | name  | amount | status |
+----+-------+--------+--------+
| 2  | bravo |   20   |  open  |
+----+-------+--------+--------+
🚨 Differences in values for common rows: 2 rows in total

📊 Difference statistics by column:
+-------------+------------+
| COLUMN_NAME | diff_count |
+-------------+------------+
|   amount    |     1      |
|    name     |     1      |
+-------------+------------+

✅ Columns with no differences: status

📋 Sample differences (first 10 rows):
+----+--------+--------+---------+
| id | COLUMN | BEFORE | CURRENT |
+----+--------+--------+---------+
| 1  |  name  | alpha  |  alfa   |
| 1  | amount |   10   |   11    |
+----+--------+--------+---------+
🔎 To review the diff, run: sqlcompare inspect compare_sqlcompare_dataset_dataset_998aa249_previous_sqlcompare_dataset_dataset_998aa249_new_20260104_025829_b28946b5
💡 Tips: --stats for per-column counts, --missing-current/--missing-previous for row-only, --column <name> to filter, --list-columns to inspect available fields.
```
