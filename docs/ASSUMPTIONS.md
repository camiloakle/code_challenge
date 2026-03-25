# Assumptions

## For reviewers (exam / evaluation map)

| Topic | Document | Code |
|-------|----------|------|
| Medallion rules, Q1–Q5 logic | This file + `promt.md` | `pipelines/`, `src/application/strategies/` |
| Raw paths (directory, glob, legacy file) | *Dataset discrepancies* below | `pipelines/bronze_ingestion.py` → `resolve_raw_transactions_path` |
| Source column names vs challenge contract | *Raw data schema* + column naming below | `CleaningService.align_to_challenge_schema` in `src/application/services/cleaning_service.py` |
| Duplicate `merchant_id` in merchants | *Merchant resolution* below | `MerchantResolutionService` in `src/application/services/merchant_resolution_service.py` |
| Operational run order | `RUNBOOK.md` | `Makefile`, `README.md` |

**Invariant for grading:** Gold (Q1–Q5) only consumes **Silver** output with **canonical** names (`purchase_ts`, `amount`, …). Any rename from `purchase_date` / `purchase_amount` happens in **Silver**, never in Gold.

## Cleaning rules (from the PDF)

The PDF explicitly requires two cleaning behaviors:
- **Merchant naming:** use `merchant_id` as the merchant name when there is no corresponding merchant name for that id in `merchants.csv`.
- **Category handling:** do not filter out records where `category` is null; replace null categories with the text `Unknown category` where applicable.

In this repo, these rules are enforced at (or before) Silver time so Gold remains contract-stable.

## Raw data schema

- **Transactions** (Parquet): **Bronze** stores columns as in the extract (e.g. `purchase_date`, `purchase_amount`). **Silver** outputs the challenge contract: `merchant_id`, `amount`, `category` (nulls coalesced), `purchase_ts`, `city_id`, `state_id`, `installments`, plus join metadata. Mapping from alternate names is in `CleaningService.align_to_challenge_schema` (Silver only). Ingestion layout is covered under *Dataset discrepancies*.
- **Merchants** (`merchants.csv`): at minimum `merchant_id`, `merchant_name`; extra columns pass through Bronze and Silver.

### Merchant resolution (duplicate `merchant_id`)

The Data Dictionary describes `merchant_id` as a **unique** merchant identifier, but the raw file can contain **multiple rows per `merchant_id`** (snapshots / inconsistent master data). Joining transactions to that table without resolution **multiplies rows** and biases Gold aggregates.

**Silver policy:** before `CleaningService.join_and_clean`, `MerchantResolutionService` keeps **one row per `merchant_id`** using a deterministic ranking (aligned with the Data Dictionary ordinals **A > B > C > D > E** for the two `most_recent_*_range` fields):

1. `most_recent_sales_range` (best-first: A best, E worst; unknown/nulls last)
2. `most_recent_purchases_range` (same)
3. `active_months_lag12` (desc, nulls last)
4. `active_months_lag6` (desc)
5. `active_months_lag3` (desc)
6. `avg_purchases_lag12` (desc)
7. `merchant_name` (asc, stable tie-break)

If either range column is absent from the input schema, that criterion is skipped (tests / minimal frames).

**Outputs:**

- `silver/merchants_resolved/` — golden record per id (used for the join).
- `silver/merchants_duplicates_audit/` — only ids with >1 row, with `resolution_rank` and `is_resolved_winner`.

### Column naming (source vs contract)

| Contract (Silver onward, Gold) | Common alternate in Parquet extracts |
|-------------------------------|----------------------------------------|
| `purchase_ts` | `purchase_date` |
| `amount` | `purchase_amount` |

If both canonical and alternate names were present, the implementation prefers the canonical column and does not duplicate. Re-run **Silver** (and downstream Gold) after changing this logic so Delta tables refresh.

## Dataset discrepancies identified

### historical_transactions file format

| Source | Stated filename | Actual filename | Decision |
|--------|-----------------|-----------------|----------|
| Data Dictionary (PDF) | `historical_transactions.csv` | N/A | Documentation outdated |
| Download link | `historical_transactions.parquet` | N/A | Format is Parquet; often a folder, not one file |
| Downloaded extract | N/A | `part-00000-tid-*.snappy.parquet` | **Used for implementation** |

**Technical explanation:** The `part-00000-tid-*.snappy.parquet` naming is standard Spark output when writing Parquet: `part-*` is the partition index, `tid-*` is the task id, `snappy` is the codec. Multiple files may exist in one folder.

**Implementation decision:**

- Use `spark.read.parquet(directory_path)` without pinning a single filename so all parts load correctly.
- If the download is a **single** `part-*.snappy.parquet` sitting directly in `data/raw/` (not in a subfolder), the pipeline resolves `data/raw/part-*.parquet` as a glob so Spark still reads it.
- This matches production behaviour when file names change between runs.

**Impact:** None on data quality or business logic—only how raw paths are resolved.

### merchants.csv file format

| Source | Stated filename | Actual filename | Decision |
|--------|-----------------|-----------------|----------|
| Data Dictionary | `merchants.csv` | `merchants.csv` | Consistent |

**Implementation:** `spark.read.csv(..., header=True, inferSchema=True)`.

## Questions

| ID | Logic |
|----|--------|
| Q1 | `Month` = `yyyy-MM` from `purchase_ts`; `City` = `City_{city_id}`; top **5** merchants by `sum(amount)` per (Month, City); tie-break `merchant_name` asc. |
| Q2 | Average `amount` per (`merchant_name`, `state_id`); columns Merchant, State ID, Average Amount; sort by Average Amount desc. |
| Q3 | Per `category`, top **3** hours by `sum(amount)`; `Hour` = `hour_of_day * 100` (HH00). |
| Q4 | All outputs under `data/gold/q4/`. `results/`: per `city_id` city metrics. `city_category_association/`: one row, Cramér's V global (city×category, cities with ≥50 tx). `top_merchants_global/`: top **100** merchants by global tx count + **primary city** (city with most tx per merchant). `top_merchants_distribution_by_city/`: counts of those 100 by primary city. `merchant_popularity_by_city/`: complement — top **5** per city by local tx count. |
| Q5 | `q5_results`: per transaction, `installments` + Q5e columns. `q5_advisory_summary`: aggregates for Q5a–d plus installments EV comparisons: `q5_installments_impact` (overall) and `q5e_installments_by_category` / `q5e_installments_by_city` (segmented). Narrative answers: `docs/Q5_BUSINESS_REPORT.md`. |

### Q5e (Installments) expected-value assumptions

This model follows the PDF assumptions:
- **Gross profit margin:** `25%` (profit margin applied to expected revenue).
- **Credit default rate:** `22.9% per month` (used as the risk factor in the EV model).
- **Installments structure:** equal installments are assumed.
- **Default timing & recovery:** everyone who defaulted did so after paying half of the transaction, so recovery under default is `50%` of the transaction amount.

With these assumptions, for a transaction `amount`:
- **With installments (expected revenue):**
  - `expected_revenue = (1 - 0.229) * amount + 0.229 * 0.5 * amount`
- **With installments (expected profit):**
  - `expected_profit = expected_revenue * 0.25`
- **Without installments (expected profit):**
  - `expected_profit = amount * 0.25`

Interpretation note (simplification aligned to the repo implementation):
- The EV uses `0.229` as the model risk factor associated with the “default happens after half payment” scenario.
- This is a simplification of temporal default dynamics into a single EV term, matching the formulas implemented in `src/application/strategies/q5_strategy.py`.

In the Gold outputs, the comparison groups are aligned to the pipeline meaning of installments:
- `with_installments`: `installments > 0`
- `without_installments`: `installments <= 0` or `installments` is null

Installments recommendation rule (`installments_recommended`):
- The final decision is not only based on whether installments are better than the `25%` baseline.
- It also applies a conservative EV threshold: `installments_recommended = True` when `expected_profit` is at least `80%` of the ideal single-payment profit (ideal single-payment profit = `amount * 0.25`), as implemented in `src/application/services/recommendation_engine.py` (see `THRESHOLD_TOLERANCE = 0.8`).

## Environment

- **Java 11+** required for local Spark.
- **Databricks**: swap filesystem paths for DBFS and use cluster packages `delta-spark`, `pyspark`.
