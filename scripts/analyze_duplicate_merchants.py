#!/usr/bin/env python3
"""Analyze duplicate merchant_id rows and transaction impact."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parent.parent


def parse_data_dictionary(excel_path: Path) -> list[dict[str, object]]:
    """Parse sheets that contain a Columns/Description section."""
    xl = pd.ExcelFile(excel_path)
    out: list[dict[str, object]] = []
    for sheet in xl.sheet_names:
        raw = pd.read_excel(xl, sheet_name=sheet, header=None)
        start = None
        for i in range(len(raw)):
            c0 = raw.iloc[i, 0]
            if pd.notna(c0) and str(c0).strip().lower() == "columns":
                start = i + 1
                break
        cols: list[tuple[str, str]] = []
        if start is not None:
            for j in range(start, len(raw)):
                name = raw.iloc[j, 0]
                desc = raw.iloc[j, 1] if raw.shape[1] > 1 else ""
                if pd.isna(name) or str(name).strip() == "":
                    continue
                d = "" if pd.isna(desc) else str(desc).strip()
                cols.append((str(name).strip(), d))
        out.append({"sheet_name": sheet, "columns": cols})
    return out


def discover_tx_paths(data_raw: Path) -> list[Path]:
    """Mirror Bronze ingestion input discovery."""
    paths: list[Path] = []
    tx_dir = data_raw / "historical_transactions"
    if tx_dir.is_dir():
        paths.extend(sorted(tx_dir.glob("*.parquet")))
    legacy = data_raw / "historical_transactions.parquet"
    if legacy.is_file():
        paths.append(legacy)
    paths.extend(sorted(data_raw.glob("part-*.parquet")))
    dedup: list[Path] = []
    seen: set[Path] = set()
    for p in paths:
        rp = p.resolve()
        if rp not in seen:
            seen.add(rp)
            dedup.append(p)
    return dedup


def dict_merchant_id_defs(dictionary: Path) -> list[dict[str, str]]:
    """Return merchant_id definitions across sheets."""
    defs: list[dict[str, str]] = []
    for s in parse_data_dictionary(dictionary):
        for col, desc in s["columns"]:  # type: ignore[index]
            if col == "merchant_id":
                defs.append(
                    {
                        "sheet": str(s["sheet_name"]),
                        "column": col,
                        "description": desc,
                    }
                )
    return defs


def profile_duplicates(
    df_mer: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Profile duplicate merchant_id groups and varying columns."""
    dup_mask = df_mer["merchant_id"].duplicated(keep=False)
    dup_rows = df_mer[dup_mask].copy()

    metrics = {
        "merchant_rows_total": int(len(df_mer)),
        "merchant_id_distinct": int(df_mer["merchant_id"].nunique()),
        "duplicate_rows": int(dup_mask.sum()),
        "duplicate_ids": int(dup_rows["merchant_id"].nunique()),
    }
    if len(dup_rows) == 0:
        return dup_rows, metrics

    cols = [c for c in df_mer.columns if c != "merchant_id"]
    groups: list[dict[str, object]] = []
    counter: Counter[str] = Counter()
    for mid, g in dup_rows.groupby("merchant_id", sort=False):
        unique_rows = int(g.drop_duplicates().shape[0])
        var_cols = [c for c in cols if g[c].nunique(dropna=False) > 1]
        counter.update(var_cols)
        groups.append(
            {
                "merchant_id": mid,
                "rows_for_id": int(len(g)),
                "unique_rows_for_id": unique_rows,
                "varying_columns_count": int(len(var_cols)),
                "varying_columns": "|".join(var_cols),
            }
        )

    grp = pd.DataFrame(groups).sort_values(
        ["unique_rows_for_id", "varying_columns_count", "rows_for_id"],
        ascending=False,
    )
    metrics["groups_with_differences"] = int((grp["unique_rows_for_id"] > 1).sum())
    metrics["max_rows_same_id"] = int(grp["rows_for_id"].max())
    metrics["top_varying_columns_count"] = int(len(counter))
    return grp, metrics


def tx_impact(tx_paths: list[Path], dup_ids: set[str]) -> dict[str, object]:
    """Measure how much transaction volume belongs to duplicate IDs."""
    dataset = ds.dataset([str(p) for p in tx_paths], format="parquet")
    col_mid = "merchant_id"
    col_amt = "purchase_amount"
    tbl = dataset.to_table(columns=[col_mid, col_amt])
    tx = tbl.to_pandas()

    rows_total = int(len(tx))
    amount_total = float(tx[col_amt].fillna(0).sum())
    mask = tx[col_mid].astype(str).isin(dup_ids)
    rows_dup = int(mask.sum())
    amount_dup = float(tx.loc[mask, col_amt].fillna(0).sum())
    counts = tx.loc[mask, col_mid].astype(str).value_counts().to_dict()

    return {
        "tx_rows_total": rows_total,
        "tx_rows_with_duplicate_merchant_id": rows_dup,
        "tx_rows_impact_ratio": (rows_dup / rows_total) if rows_total else 0.0,
        "tx_amount_total": amount_total,
        "tx_amount_with_duplicate_merchant_id": amount_dup,
        "tx_amount_impact_ratio": ((amount_dup / amount_total) if amount_total else 0.0),
        "tx_counts_for_dup_ids": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze duplicate merchants and transaction impact."
    )
    parser.add_argument(
        "--dictionary",
        type=Path,
        default=ROOT / "Data Dictionary.xlsx",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=ROOT / "data",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "data" / "gold" / "qa",
    )
    args = parser.parse_args()

    dictionary = args.dictionary.resolve()
    data_root = args.data_root.resolve()
    data_raw = data_root / "raw"
    merchants_path = data_raw / "merchants.csv"
    tx_paths = discover_tx_paths(data_raw)

    if not dictionary.exists():
        raise SystemExit(f"Dictionary not found: {dictionary}")
    if not merchants_path.exists():
        raise SystemExit(f"Merchants file not found: {merchants_path}")
    if not tx_paths:
        raise SystemExit("No transaction parquet found under data/raw/")

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    defs = dict_merchant_id_defs(dictionary)
    mer = pd.read_csv(merchants_path)
    dup_group_df, metrics = profile_duplicates(mer)
    dup_ids = set(dup_group_df["merchant_id"].astype(str))
    dup_rows_df = mer[mer["merchant_id"].astype(str).isin(dup_ids)].copy()
    impact = tx_impact(tx_paths, dup_ids)

    # Potential join explosion if merchants is not deduplicated before join
    versions = dup_group_df.set_index("merchant_id")["rows_for_id"].to_dict()
    join_rows = 0
    for mid, tx_n in impact["tx_counts_for_dup_ids"].items():
        join_rows += int(tx_n) * int(versions.get(mid, 1))
    impact["possible_join_multiplication_rows"] = int(join_rows)

    dup_group_df.to_csv(out_dir / "duplicate_merchant_groups.csv", index=False)
    dup_rows_df.to_csv(out_dir / "duplicate_merchant_rows.csv", index=False)

    summary = {
        "dictionary_merchant_id_definitions": defs,
        "merchants_duplicate_metrics": metrics,
        "transactions_impact": {k: v for k, v in impact.items() if k != "tx_counts_for_dup_ids"},
    }
    (out_dir / "duplicate_merchants_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("=== Data Dictionary: merchant_id ===")
    for d in defs:
        print(f"- {d['sheet']}: {d['description']}")

    print("\n=== Merchants duplicates ===")
    print(
        "rows_total={merchant_rows_total} distinct_ids={merchant_id_distinct} "
        "duplicate_rows={duplicate_rows} duplicate_ids={duplicate_ids} "
        "groups_with_differences={groups_with_differences}".format(**metrics)
    )

    print("\n=== Transaction impact ===")
    print(
        "tx_rows_total={tx_rows_total} "
        "tx_rows_with_duplicate_id={tx_rows_with_duplicate_merchant_id} "
        "rows_impact_ratio={tx_rows_impact_ratio:.4f} "
        "tx_amount_total={tx_amount_total:.2f} "
        "tx_amount_with_duplicate_id="
        "{tx_amount_with_duplicate_merchant_id:.2f} "
        "amount_impact_ratio={tx_amount_impact_ratio:.4f} "
        "possible_join_multiplication_rows="
        "{possible_join_multiplication_rows}".format(**impact)
    )
    print(f"\nArtifacts written to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
