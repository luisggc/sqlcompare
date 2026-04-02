---
name: sqlcompare-output-analysis
description: Analyze SQLCompare outputs after a core run. Use when the user has a diff_id and needs to review stats, missing rows, column-level diffs, exports, or AI-friendly meta output.
---

# SQLCompare Output Analysis

## Overview
Interpret and drill into a SQLCompare diff_id using review, history, and review meta. Produce summaries, targeted column reviews, and exportable reports.

## Start From A diff_id
1. If you already have a diff_id, proceed to inspection commands.
2. If you do not, list recent diffs and pick the correct one.
`sqlcompare history`
`sqlcompare history users`

## Core Inspection Commands
1. Overall statistics summary.
`sqlcompare review stats <diff_id>`
2. Review a specific column with row samples.
`sqlcompare review diff <diff_id> --column revenue --limit 100`
3. Show rows missing on either side.
`sqlcompare review missing <diff_id> --side current`
`sqlcompare review missing <diff_id> --side previous`
4. List available columns.
`sqlcompare review columns <diff_id>`

## Export Reports (XLSX)
1. Summary report with capped row samples per column.
`sqlcompare review export <diff_id> --mode summary`
2. Full report without per-column row caps.
`sqlcompare review export <diff_id> --mode complete --output ./reports/full_diff.xlsx`
3. Single-column summary report.
`sqlcompare review export <diff_id> --column revenue --mode summary --output ./reports/revenue_diff.xlsx`

Notes:
1. `review export --mode summary|complete` is for the standard diff view.
2. Do not combine `review export` with `review stats`, `review missing`, or `review columns`.

## AI-Friendly Diff Queries
Use review meta to get structured metadata and SQL templates.
`sqlcompare review meta <diff_id>`

## Interpretation Hints
1. Start with `--stats` to see which columns change most.
2. Use `--missing-current` and `--missing-previous` to find dropped or new rows.
3. Use `--column` to review high-impact fields.
4. Export a summary report for sharing, then refine analysis with targeted column checks.
