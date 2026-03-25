# Q4 & Q5 — Silver Anomalies Report

This document validates and explains the “Cleaning” conventions from the challenge and documents the anomalies found in the Silver layer that feed `Q4` and `Q5`.

## Cleaning conventions required by the challenge

1. **Fallback merchant name**
   - Rule: Use `merchant_id` as `merchant_name` where there is no corresponding merchant name for the merchant IDs in the historical table.
2. **Do not drop null categories**
   - Rule: Do not filter out records where `category` is null.
   - Allowed behavior: replace null categories with the text **`"Unknown category"`**.

## Where the cleaning happens

Silver is produced by `pipelines/silver_builder.py`, which applies `src/application/services/cleaning_service.py`.

In `CleaningService.join_and_clean(...)`:
- `merchant_name` is set with `coalesce(merchant_name_lookup, merchant_id)` (fallback)
- `category` is set with `coalesce(category, "Unknown category")` (no category rows are dropped)

## Validations (Silver: `data/silver/enriched_transactions`)

### A) Merchant identity anomalies

- `merchant_id IS NULL` rows: **34,570**
- For the rows where `merchant_id IS NOT NULL`:
  - `merchant_name IS NULL` violations: **0**
  - `merchant_name == ""` violations: **0**

**Interpretation**: the fallback rule is correct. The only rows that still end up with missing `merchant_name` are those where `merchant_id` itself is missing (because there is nothing to fallback to).

### B) Category null handling

- `category IS NULL` rows: **0**
- `category == ""` rows: **0**
- `category == "Unknown category"` rows: **44,625**

**Interpretation**: category nulls are replaced exactly as allowed by the challenge.

## How the dashboard/report should handle anomalies

For “report” visuals (Q4/Q5):
- **Exclude** rows where `merchant_id IS NULL` (since merchant identity is missing and those rows create `City=None` / `merchant_id=None` artifacts in derived top-merchants outputs).
- **Do NOT exclude** category `"Unknown category"`; it is intentionally part of the cleaned data and is required to reflect real demand in null-category records.

## Evidence in Q4 Gold outputs (derived from Silver)

- `data/gold/q4/top_merchants_global` contains **1** row with `merchant_id = NULL` / `merchant_name = NULL`.
- `data/gold/q4/top_merchants_distribution_by_city` contains **1** row with `City = None` (caused by join behavior on null merchant identity).

## Summary

The implementation respects the challenge Cleaning rules. The only anomalies remaining are a small set of transaction rows where `merchant_id` is missing in the original dataset. Those rows should be excluded only from *report visualizations*, while `"Unknown category"` should remain.

