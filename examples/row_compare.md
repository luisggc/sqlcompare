# Row Compare

Compare rows using a dataset YAML with file inputs

```text
Prepared dataset tables: examples.dataset_dataset_1f232b87_previous, examples.dataset_dataset_1f232b87_new
✅ Ensured schema 'examples' exists
Joining tables: examples.dataset_dataset_1f232b87_previous and examples.dataset_dataset_1f232b87_new
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
|    name     |     1      |
|   amount    |     1      |
+-------------+------------+

✅ Columns with no differences: status

📋 Sample differences (first 10 rows):
+----+--------+--------+---------+
| id | COLUMN | BEFORE | CURRENT |
+----+--------+--------+---------+
| 1  |  name  | alpha  |  alfa   |
| 1  | amount |   10   |   11    |
+----+--------+--------+---------+
🔎 To review the diff, run: sqlcompare analyze-diff compare_examples_dataset_dataset_1f232b87_previous_examples_dataset_dataset_1f232b87_new_20260103_234457_9180044d
💡 Tips: --stats for per-column counts, --missing-current/--missing-previous for row-only, --column <name> to filter, --list-columns to inspect available fields.
```
