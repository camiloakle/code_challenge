#!/usr/bin/env python3
"""Validate `Data Dictionary.xlsx` against files under `data/` (EDA + consistency).

Does not require PySpark: uses pandas + pyarrow. Intended to run before pipelines.

Usage:
  python scripts/validate_data_dictionary.py
  python scripts/validate_data_dictionary.py --data-root data --dictionary "Data Dictionary.xlsx"
  python scripts/validate_data_dictionary.py --deep   # referential check tx merchant_id vs merchants
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent

# Silver contract aliases (see docs/ASSUMPTIONS.md)
TX_CANONICAL_TO_DICTIONARY = {
    "purchase_ts": "purchase_date",
    "amount": "purchase_amount",
}


@dataclass
class SheetSpec:
    """Parsed sheet: logical name and column definitions."""

    sheet_name: str
    logical_name: str
    columns: list[tuple[str, str]]  # (name, description)


@dataclass
class ValidationReport:
    """Collects messages for exit code."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    infos: list[str] = field(default_factory=list)

    def ok(self) -> bool:
        return not self.errors


def _load_openpyxl_engine():
    try:
        import pandas as pd  # noqa: F401

        return pd
    except ImportError as exc:
        raise SystemExit(
            "pandas is required. Install project deps: pip install -r requirements.txt"
        ) from exc


def parse_data_dictionary(excel_path: Path) -> list[SheetSpec]:
    """Read Excel: each sheet lists Columns | Description starting after header row."""
    pd = _load_openpyxl_engine()
    try:
        xl = pd.ExcelFile(excel_path)
    except ImportError as exc:
        if "openpyxl" in str(exc).lower():
            raise SystemExit("Reading .xlsx requires openpyxl: pip install openpyxl") from exc
        raise

    specs: list[SheetSpec] = []
    for sheet in xl.sheet_names:
        raw = pd.read_excel(xl, sheet_name=sheet, header=None)
        # Find row where first cell is "Columns"
        start_idx = None
        for i in range(len(raw)):
            c0 = raw.iloc[i, 0]
            if pd.notna(c0) and str(c0).strip().lower() == "columns":
                start_idx = i + 1
                break
        if start_idx is None:
            specs.append(
                SheetSpec(
                    sheet_name=sheet,
                    logical_name=_logical_name(sheet),
                    columns=[],
                )
            )
            continue
        pairs: list[tuple[str, str]] = []
        for j in range(start_idx, len(raw)):
            name = raw.iloc[j, 0]
            desc = raw.iloc[j, 1] if raw.shape[1] > 1 else ""
            if pd.isna(name) or str(name).strip() == "":
                continue
            pairs.append((str(name).strip(), _clean_desc(desc)))
        specs.append(
            SheetSpec(
                sheet_name=sheet,
                logical_name=_logical_name(sheet),
                columns=pairs,
            )
        )
    return specs


def _clean_desc(desc) -> str:
    pd = _load_openpyxl_engine()
    if pd.isna(desc):
        return ""
    return str(desc).strip()


def _logical_name(sheet: str) -> str:
    s = sheet.lower().replace(" ", "_").replace(".csv", "")
    if s.startswith("historical"):
        return "historical_transactions"
    if "merchant" in s:
        return "merchants"
    return s


def discover_transaction_parquet_paths(data_raw: Path) -> list[Path]:
    """Mirror pipeline expectations: folder, legacy file, or loose part files."""
    paths: list[Path] = []
    ht_dir = data_raw / "historical_transactions"
    if ht_dir.is_dir():
        paths.extend(sorted(ht_dir.glob("*.parquet")))
    legacy = data_raw / "historical_transactions.parquet"
    if legacy.is_file():
        paths.append(legacy)
    paths.extend(sorted(data_raw.glob("part-*.parquet")))
    # de-dupe preserving order
    seen = set()
    out: list[Path] = []
    for p in paths:
        rp = p.resolve()
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out


def parquet_schema_and_rows(path: Path) -> tuple[list[str], int]:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    names = list(pf.schema_arrow.names)
    md = pf.metadata
    num_rows = int(md.num_rows) if md is not None else -1
    return names, num_rows


def read_merchants_csv(path: Path):
    pd = _load_openpyxl_engine()
    return pd.read_csv(path)


def compare_columns(
    expected: list[str],
    actual: list[str],
    label: str,
    report: ValidationReport,
) -> None:
    exp_set = set(expected)
    act_set = set(actual)
    missing = sorted(exp_set - act_set)
    extra = sorted(act_set - exp_set)
    if not missing and not extra:
        report.infos.append(f"{label}: column names match dictionary ({len(actual)} cols).")
        return
    if missing:
        report.warnings.append(f"{label}: columns in dictionary but missing in data: {missing}")
    if extra:
        report.infos.append(f"{label}: extra columns in data (not in dictionary): {extra}")
    if missing:
        # Map canonical Silver names to dictionary names for hint
        inv = {v: k for k, v in TX_CANONICAL_TO_DICTIONARY.items()}
        hints = [inv[m] for m in missing if m in inv]
        if hints:
            report.infos.append(
                f"{label}: note — pipeline may expose {hints} under Silver "
                f"canonical names; see docs/ASSUMPTIONS.md."
            )


def profile_merchants(csv_path: Path, report: ValidationReport) -> None:
    df = read_merchants_csv(csv_path)
    n = len(df)
    dups = df["merchant_id"].duplicated().sum() if "merchant_id" in df.columns else 0
    report.infos.append(f"merchants: rows={n}, duplicate merchant_id rows={int(dups)}")
    if dups > 0:
        report.warnings.append(
            f"merchants: found {dups} duplicate merchant_id rows (expect unique)."
        )
    nulls = df.isnull().sum()
    high = nulls[nulls > 0]
    if len(high):
        report.infos.append(
            "merchants: null counts (non-zero):\n  " + high.to_string().replace("\n", "\n  ")
        )


def profile_transactions(
    paths: list[Path],
    expected_cols: list[str],
    report: ValidationReport,
) -> tuple[list[str], int]:
    if not paths:
        report.errors.append("transactions: no Parquet files found under data/raw/.")
        return [], 0
    all_cols: list[str] = []
    total_rows = 0
    for p in paths:
        cols, n = parquet_schema_and_rows(p)
        if not all_cols:
            all_cols = cols
        elif set(cols) != set(all_cols):
            report.warnings.append(f"transactions: schema differs from first file — {p.name}")
        if n >= 0:
            total_rows += n
    report.infos.append(
        f"transactions: {len(paths)} parquet file(s), ~rows={total_rows}, "
        f"columns={len(all_cols)}"
    )
    compare_columns(expected_cols, all_cols, "transactions", report)
    return all_cols, total_rows


def deep_merchant_join_check(
    merchants_path: Path,
    tx_paths: list[Path],
    report: ValidationReport,
) -> None:
    """Sample distinct merchant_ids from Parquet vs merchants reference."""
    import pyarrow.dataset as ds
    import pyarrow.compute as pc

    mer = read_merchants_csv(merchants_path)
    if "merchant_id" not in mer.columns:
        report.warnings.append("deep check: merchants.csv has no merchant_id")
        return
    valid = set(mer["merchant_id"].astype(str))

    dataset = ds.dataset([str(p) for p in tx_paths], format="parquet")
    if "merchant_id" not in dataset.schema.names:
        report.warnings.append("deep check: transactions have no merchant_id column")
        return
    tbl = dataset.to_table(columns=["merchant_id"])
    # distinct merchant_id in tx
    uniq = pc.unique(tbl.column(0)).to_pylist()
    tx_ids = {str(x) for x in uniq if x is not None}
    orphans = sorted(tx_ids - valid)
    if orphans:
        sample = orphans[:15]
        report.warnings.append(
            f"deep check: {len(orphans)} distinct merchant_id values in "
            f"transactions not present in merchants (sample: {sample})"
        )
    else:
        report.infos.append(
            "deep check: every distinct transaction merchant_id exists in merchants.csv."
        )


def print_report(report: ValidationReport) -> None:
    for line in report.infos:
        print(f"[INFO] {line}")
    for line in report.warnings:
        print(f"[WARN] {line}")
    for line in report.errors:
        print(f"[ERROR] {line}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Data Dictionary.xlsx vs data/raw (EDA + consistency)."
    )
    parser.add_argument(
        "--dictionary",
        type=Path,
        default=ROOT / "Data Dictionary.xlsx",
        help="Path to Data Dictionary.xlsx",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=ROOT / "data",
        help="Data root (expects raw/ inside)",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Referential check: tx merchant_id vs merchants (reads Parquet)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable summary on stdout (last line)",
    )
    args = parser.parse_args()

    report = ValidationReport()
    excel_path = args.dictionary.expanduser().resolve()
    data_root = args.data_root.expanduser().resolve()
    data_raw = data_root / "raw"

    if not excel_path.is_file():
        report.errors.append(f"Dictionary not found: {excel_path}")
        print_report(report)
        return 1

    specs = parse_data_dictionary(excel_path)
    merchants_spec = next((s for s in specs if s.logical_name == "merchants"), None)
    tx_spec = next((s for s in specs if s.logical_name == "historical_transactions"), None)

    if merchants_spec:
        report.infos.append(
            f"Dictionary: sheet '{merchants_spec.sheet_name}' → "
            f"{len(merchants_spec.columns)} merchant columns"
        )
    if tx_spec:
        report.infos.append(
            f"Dictionary: sheet '{tx_spec.sheet_name}' → "
            f"{len(tx_spec.columns)} transaction columns"
        )

    merchants_csv = data_raw / "merchants.csv"
    if not merchants_csv.is_file():
        report.errors.append(f"Missing {merchants_csv}")
    else:
        exp_mer = [c for c, _ in merchants_spec.columns] if merchants_spec else []
        df = read_merchants_csv(merchants_csv)
        compare_columns(exp_mer, list(df.columns), "merchants.csv", report)
        profile_merchants(merchants_csv, report)

    tx_paths = discover_transaction_parquet_paths(data_raw)
    exp_tx = [c for c, _ in tx_spec.columns] if tx_spec else []
    if tx_paths:
        profile_transactions(tx_paths, exp_tx, report)
        if args.deep and merchants_csv.is_file():
            try:
                deep_merchant_join_check(merchants_csv, tx_paths, report)
            except Exception as exc:
                report.warnings.append(f"deep check failed: {exc}")
    else:
        report.errors.append(
            "No transaction Parquet under data/raw/ "
            "(expected historical_transactions/, historical_transactions.parquet, "
            "or part-*.parquet)."
        )

    print_report(report)

    payload = {
        "ok": report.ok(),
        "errors": report.errors,
        "warnings": report.warnings,
        "info_count": len(report.infos),
    }
    if args.json:
        print("JSON_SUMMARY:", json.dumps(payload, ensure_ascii=False))

    return 0 if report.ok() else 1


if __name__ == "__main__":
    sys.exit(main())
