---
name: sqlcompare-output-analysis
description: Analyze SQLCompare outputs after a core run. Use when the user has a diff_id and needs to inspect stats, missing rows, column-level diffs, exports, or AI-friendly diff-queries.
---

# SQLCompare Output Analysis

## Overview
Interpret and drill into a SQLCompare diff_id using inspect, list-diffs, and diff-queries. Produce summaries, targeted column reviews, and exportable reports.

## Start From A diff_id
1. If you already have a diff_id, proceed to inspection commands.
2. If you do not, list recent diffs and pick the correct one.
`sqlcompare list-diffs`
`sqlcompare list-diffs users`

## Core Inspection Commands
1. Overall statistics summary.
`sqlcompare inspect <diff_id> --stats`
2. Inspect a specific column with row samples.
`sqlcompare inspect <diff_id> --column revenue --limit 100`
3. Show rows missing on either side.
`sqlcompare inspect <diff_id> --missing-current`
`sqlcompare inspect <diff_id> --missing-previous`
4. List available columns.
`sqlcompare inspect <diff_id> --list-columns`

## Export Reports (XLSX)
1. Summary report with capped row samples per column.
`sqlcompare inspect <diff_id> --save summary`
2. Full report without per-column row caps.
`sqlcompare inspect <diff_id> --save complete --file-path ./reports/full_diff.xlsx`
3. Single-column summary report.
`sqlcompare inspect <diff_id> --column revenue --save summary --file-path ./reports/revenue_diff.xlsx`

Notes:
1. `--save summary|complete` is for the standard diff view.
2. Do not combine `--save summary|complete` with `--stats`, `--missing-current`, `--missing-previous`, or `--list-columns`.

## AI-Friendly Diff Queries
Use diff-queries to get structured metadata and SQL templates.
`sqlcompare diff-queries <diff_id>`

## Interpretation Hints
1. Start with `--stats` to see which columns change most.
2. Use `--missing-current` and `--missing-previous` to find dropped or new rows.
3. Use `--column` to inspect high-impact fields.
4. Export a summary report for sharing, then refine analysis with targeted column checks.
