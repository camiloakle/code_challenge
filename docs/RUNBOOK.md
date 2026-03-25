# Runbook

## Local execution

1. Install **Java 11+** (recommended **17** for PySpark 3.5.x) and set `JAVA_HOME` if Spark fails to start.
2. `export PYTHONPATH=.` (or use `make` targets, which set it).
3. Create venv and deps: `make init` (see `README.md`).
4. Place raw data under `data/raw/` (see `README.md` and `docs/ASSUMPTIONS.md` for paths and column notes).
5. Run `make run-all` or `make run-bronze` → `make run-silver` → `make run-q1` (or `pipelines.runner --all`). Silver writes `merchants_resolved` and `merchants_duplicates_audit` under `data/silver/` (see `docs/ASSUMPTIONS.md`).

After **changing Silver schema logic** (e.g. column renames), rebuild Silver and Gold: `make clean` (removes generated `data/bronze|silver|gold`) or delete `data/silver` and `data/gold` only, then re-run from Bronze or Silver as appropriate.

## Failure modes

| Symptom | Action |
|---------|--------|
| `Java gateway process exited` | Install JRE/JDK; verify `java -version` and `JAVA_HOME`. |
| `DeltaCatalog` / `ClassNotFoundException` (Delta) | Use `delta-spark` with `configure_spark_with_delta_pip` (`config/spark_config.py`); reinstall deps. |
| `Failed to read Silver` | Run `silver_builder` after `bronze_ingestion`. |
| `Transactions not found` | Set `RAW_TRANSACTIONS_PATH`, or use `data/raw/historical_transactions/`, legacy `historical_transactions.parquet`, or loose `data/raw/part-*.parquet` (see `ASSUMPTIONS.md`). |
| `purchase_ts` / `amount` cannot be resolved | Raw uses `purchase_date` / `purchase_amount`; ensure **Silver** ran with current `CleaningService` (re-run `make run-silver` and Gold). |

## Airflow

- Set `AIRFLOW_HOME`, install `apache-airflow`, copy `airflow/dags` into the Airflow DAGs folder or point `dags_folder` at this repo’s `airflow/dags`.
- Ensure the worker can run `python` with the same `PYTHONPATH` and Spark/JVM as development.
