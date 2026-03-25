#!/usr/bin/env python3
"""Contrast Billups challenge PDF requirements against Gold Parquet outputs.

Reads `Data Engineer Challenge (2) (1) (2).pdf` (optional, via pdftotext) for traceability
and validates schemas + business rules implied by the brief vs files under data/gold/.

Usage:
  python scripts/validate_challenge_results.py
  python scripts/validate_challenge_results.py --gold-root data/gold --strict
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent

DEFAULT_PDF = ROOT / "Data Engineer Challenge (2) (1) (2).pdf"

# Expected coefficients from PDF / src.shared.constants (Q5e)
try:
    from src.shared.constants import (
        DEFAULT_RATE_MONTHLY,
        EXPECTED_PROFIT_MARGIN,
        GROSS_PROFIT_MARGIN,
    )
except ImportError:
    sys.path.insert(0, str(ROOT))
    from src.shared.constants import (
        DEFAULT_RATE_MONTHLY,
        EXPECTED_PROFIT_MARGIN,
        GROSS_PROFIT_MARGIN,
    )


@dataclass
class Finding:
    """Single check outcome."""

    code: str
    ok: bool
    message: str
    severity: str = "info"  # info | warn | error


@dataclass
class SectionReport:
    question: str
    pdf_expectation: str
    findings: list[Finding] = field(default_factory=list)

    @property
    def has_error(self) -> bool:
        return any(not f.ok and f.severity == "error" for f in self.findings)

    @property
    def has_warn(self) -> bool:
        return any(not f.ok for f in self.findings)


def _pdf_text(pdf_path: Path) -> str | None:
    """Return plain text from PDF using pdftotext (poppler-utils), or None."""
    try:
        r = subprocess.run(
            ["pdftotext", str(pdf_path), "-"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    return None


def _load_gold_parquet(gold_dir: Path) -> pd.DataFrame:
    paths = sorted(gold_dir.glob("part-*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No part-*.parquet under {gold_dir}")
    frames = [pd.read_parquet(p) for p in paths]
    return pd.concat(frames, ignore_index=True)


def _validate_q1(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    required = ["Month", "City", "Merchant", "Purchase Total", "No of sales"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        findings.append(
            Finding(
                "q1_schema",
                False,
                f"Faltan columnas: {missing}",
                "error",
            )
        )
        return findings

    findings.append(Finding("q1_schema", True, "Columnas alineadas con el ejemplo del PDF (Month, City, Merchant, …)."))

    bad = df.groupby(["Month", "City"], observed=True).size()
    over = int((bad > 5).sum())
    mx = int(bad.max()) if len(bad) else 0
    pdf_top5 = over == 0
    findings.append(
        Finding(
            "q1_top5_per_month_city",
            pdf_top5,
            f"PDF: top 5 comercios por mes y ciudad. Máximo observado por (Month, City): {mx}; "
            f"grupos con >5 filas: {over}.",
            "error" if not pdf_top5 else "info",
        )
    )

    if (df["Purchase Total"] < 0).any():
        findings.append(
            Finding("q1_amounts_non_negative", False, "Hay Purchase Total < 0.", "warn")
        )
    else:
        findings.append(Finding("q1_amounts_non_negative", True, "Purchase Total sin negativos."))

    return findings


def _validate_q2(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    required = {"Merchant", "State ID", "Average Amount"}
    if not required.issubset(df.columns):
        findings.append(
            Finding(
                "q2_schema",
                False,
                f"Se esperaban {sorted(required)}; columnas: {list(df.columns)}",
                "error",
            )
        )
        return findings

    findings.append(
        Finding(
            "q2_schema_pdf",
            True,
            "Columnas Merchant, State ID, Average Amount (promedio por comercio y estado).",
            "info",
        )
    )
    s = df["Average Amount"].reset_index(drop=True)
    if len(s) > 1 and not s.is_monotonic_decreasing:
        findings.append(
            Finding(
                "q2_order_desc",
                False,
                "PDF: mayores promedios primero. Las filas no están ordenadas desc por Average Amount.",
                "warn",
            )
        )
    else:
        findings.append(
            Finding(
                "q2_order_desc",
                True,
                "Filas ordenadas no creciente por Average Amount.",
                "info",
            )
        )
    return findings


def _validate_q3(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = {"category", "Hour", "total_amount"}
    if not need.issubset(df.columns):
        findings.append(
            Finding("q3_schema", False, f"Se esperaban {sorted(need)}; columnas: {list(df.columns)}", "error")
        )
        return findings

    findings.append(Finding("q3_schema", True, "category + Hour + total_amount presentes."))

    sizes = df.groupby("category", observed=True).size()
    over = int((sizes > 3).sum())
    mx = int(sizes.max()) if len(sizes) else 0
    ok_top3 = over == 0
    findings.append(
        Finding(
            "q3_top3_per_category",
            ok_top3,
            f"PDF: máximo 3 horas por categoría. Máximo filas por category: {mx}; categorías con >3: {over}.",
            "error" if not ok_top3 else "info",
        )
    )

    if "hour_of_day" in df.columns:
        hours = set(df["hour_of_day"].dropna().unique().tolist())
        if hours and not hours.issubset(set(range(24))):
            findings.append(Finding("q3_hours_range", False, "hour_of_day fuera de 0–23.", "warn"))
        else:
            findings.append(Finding("q3_hours_range", True, "hour_of_day en 0–23 (si está presente)."))
    return findings


def _validate_q4(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = {
        "city_id",
        "City",
        "total_transactions",
        "distinct_merchants",
        "distinct_categories",
    }
    if not need.issubset(df.columns):
        findings.append(Finding("q4_schema", False, f"Columnas: {list(df.columns)}", "error"))
        return findings

    findings.append(
        Finding(
            "q4_city_metrics",
            True,
            "Métricas por ciudad (volumen). Cramér's V está en `q4/city_category_association/`.",
            "info",
        )
    )
    return findings


def _validate_q4_city_category_association(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = {
        "city_category_cramers_v",
        "metric_description",
        "min_city_tx_for_contingency",
    }
    if not need.issubset(df.columns):
        findings.append(
            Finding("q4_assoc_schema", False, f"Columnas: {list(df.columns)}", "error")
        )
        return findings
    if len(df) != 1:
        findings.append(
            Finding(
                "q4_assoc_rows",
                False,
                f"Se esperaba 1 fila (métrica global); hay {len(df)}.",
                "warn",
            )
        )
    v = df["city_category_cramers_v"].iloc[0]
    vv = float(v)
    ok = math.isnan(vv) or (0.0 <= vv <= 1.0)
    findings.append(
        Finding(
            "q4_cramers_v_bounds",
            ok,
            f"Cramér's V global (ciudad×categoría) = {vv}.",
            "warn" if not ok else "info",
        )
    )
    return findings


def _validate_q4_top_merchants_global(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = {
        "global_rank",
        "merchant_id",
        "merchant_name",
        "total_transactions",
        "primary_city_id",
        "City",
        "tx_in_primary_city",
        "n_cities_active",
    }
    if not need.issubset(df.columns):
        findings.append(
            Finding("q4_global_schema", False, f"Columnas: {list(df.columns)}", "error")
        )
        return findings
    mx = int(df["global_rank"].max()) if len(df) else 0
    findings.append(
        Finding(
            "q4_global_top_k",
            mx <= 100 and len(df) <= 100,
            f"Top K global: {len(df)} filas; max rank {mx} (K=100).",
            "warn" if mx > 100 or len(df) > 100 else "info",
        )
    )
    return findings


def _validate_q4_top_distribution(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = {
        "primary_city_id",
        "City",
        "n_top_merchants_in_top_k",
        "pct_of_top_k",
    }
    if not need.issubset(df.columns):
        findings.append(
            Finding("q4_dist_schema", False, f"Columnas: {list(df.columns)}", "error")
        )
        return findings
    s = int(df["n_top_merchants_in_top_k"].sum()) if len(df) else 0
    ok = s == 100
    findings.append(
        Finding(
            "q4_dist_sum",
            ok,
            f"Suma de merchants en distribución = {s} (esperado 100 si K=100).",
            "warn" if not ok else "info",
        )
    )
    return findings


def _validate_q5(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = [
        "merchant_name",
        "category",
        "amount",
        "installments",
        "installments_recommended",
        "expected_profit_margin",
        "risk_score",
        "assumptions",
    ]
    missing = [c for c in need if c not in df.columns]
    if missing:
        findings.append(Finding("q5_schema", False, f"Faltan: {missing}", "error"))
        return findings

    findings.append(Finding("q5_schema", True, "Columnas Q5e + dimensiones presentes."))

    ep = df["expected_profit_margin"].dropna().unique()
    if len(ep) == 1 and math.isclose(
        float(ep[0]),
        float(EXPECTED_PROFIT_MARGIN),
        rel_tol=0.0,
        abs_tol=1e-5,  # Parquet puede ser float32 (~1e-7 vs float64)
    ):
        findings.append(
            Finding(
                "q5_ev_margin",
                True,
                f"expected_profit_margin fijo = {EXPECTED_PROFIT_MARGIN} (constante del challenge).",
            )
        )
    else:
        findings.append(
            Finding(
                "q5_ev_margin",
                False,
                f"Valores únicos expected_profit_margin: {ep[:5]}… (esperado ~{EXPECTED_PROFIT_MARGIN}).",
                "warn",
            )
        )

    rs = df["risk_score"].dropna().unique()
    if len(rs) == 1 and math.isclose(
        float(rs[0]),
        float(DEFAULT_RATE_MONTHLY),
        rel_tol=0.0,
        abs_tol=1e-5,
    ):
        findings.append(
            Finding("q5_default_rate", True, f"risk_score = {DEFAULT_RATE_MONTHLY} (22.9%).")
        )
    else:
        findings.append(
            Finding(
                "q5_default_rate",
                False,
                f"risk_score esperado {DEFAULT_RATE_MONTHLY}; visto: {rs[:5]}",
                "warn",
            )
        )

    findings.append(
        Finding(
            "q5_advisory_tables",
            True,
            "Q5a–d e impacto de `installments`: ver `data/gold/q5_advisory_summary/` y `docs/CHALLENGE_REPORT.md`.",
            "info",
        )
    )
    findings.append(
        Finding(
            "q5_gross_assumption",
            True,
            f"Modelo asume ~{int(GROSS_PROFIT_MARGIN * 100)}% margen bruto (ver constants / PDF).",
            "info",
        )
    )
    return findings


def _validate_q4_merchant_popularity(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = {
        "City",
        "city_id",
        "merchant_id",
        "merchant_name",
        "transaction_count",
        "popularity_rank",
    }
    if not need.issubset(df.columns):
        findings.append(
            Finding(
                "q4_pop_schema",
                False,
                f"Columnas esperadas {sorted(need)}; visto: {list(df.columns)}",
                "error",
            )
        )
        return findings
    mx = df.groupby("city_id", observed=True)["popularity_rank"].max()
    bad = int((mx > 5).sum())
    ok = bad == 0
    findings.append(
        Finding(
            "q4_pop_top5",
            ok,
            f"Top 5 merchants por ciudad por # transacciones. Ciudades con rank>5: {bad}.",
            "error" if not ok else "info",
        )
    )
    return findings


def _validate_q5_advisory_summary(df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    need = {
        "section",
        "detail_key",
        "detail_label",
        "transaction_count",
        "total_amount",
        "extra_note",
    }
    if not need.issubset(df.columns):
        findings.append(
            Finding(
                "q5_adv_schema",
                False,
                f"Columnas: {list(df.columns)}",
                "error",
            )
        )
        return findings
    want = {
        "q5a_cities",
        "q5b_categories",
        "q5c_months",
        "q5d_hours",
        "q5_installments_impact",
    }
    have = set(df["section"].dropna().unique().tolist())
    missing = want - have
    findings.append(
        Finding(
            "q5_adv_sections",
            len(missing) == 0,
            f"Secciones presentes: {sorted(have)}; faltan: {sorted(missing)}.",
            "error" if missing else "info",
        )
    )
    return findings


PDF_BLURBS = {
    "q1": "Top 5 merchants by purchase_amount for each month and each city (Month, City, Merchant, Purchase Total, No of sales).",
    "q2": "Average sale amount per merchant in each state; largest sales first (Merchant, State ID, Average Amount).",
    "q3": "Top 3 hours with largest sales amount per product category (category × hour).",
    "q4": "Cities with most popular merchants; correlation between city_id and categories (popularity = transaction count).",
    "q5": "Strategic advice a–e; 5e uses installments, 25% gross profit, 22.9% default, equal installments, half payment before default.",
}


def run_validation(gold_root: Path, pdf_path: Path) -> tuple[list[SectionReport], str | None]:
    pdf_excerpt = _pdf_text(pdf_path)
    sections: list[SectionReport] = []
    base_result_paths = {
        "q1": "q1_results",
        "q2": "q2_results",
        "q3": "q3_results",
        "q4": "q4/results",
        "q5": "q5_results",
    }

    for q in ("q1", "q2", "q3", "q4", "q5"):
        sub = gold_root / base_result_paths[q]
        header = SectionReport(question=q.upper(), pdf_expectation=PDF_BLURBS[q])
        if not sub.is_dir():
            header.findings.append(
                Finding(f"{q}_path", False, f"No existe {sub}", "error")
            )
            sections.append(header)
            continue
        try:
            df = _load_gold_parquet(sub)
        except FileNotFoundError as e:
            header.findings.append(Finding(f"{q}_data", False, str(e), "error"))
            sections.append(header)
            continue

        if q == "q1":
            header.findings.extend(_validate_q1(df))
        elif q == "q2":
            header.findings.extend(_validate_q2(df))
        elif q == "q3":
            header.findings.extend(_validate_q3(df))
        elif q == "q4":
            header.findings.extend(_validate_q4(df))
        else:
            header.findings.extend(_validate_q5(df))

        header.findings.insert(
            0,
            Finding(
                f"{q}_rows",
                True,
                f"Filas cargadas: {len(df):,}; columnas: {list(df.columns)}",
                "info",
            ),
        )
        sections.append(header)

    # Salidas Gold extra (Q4 global + asociación + local; Q5 a–d + installments)
    extras: list[tuple[str, str, str, Callable[[pd.DataFrame], list[Finding]]]] = [
        (
            "q4/city_category_association",
            "Q4a",
            "Cramér's V global (una fila) — ciudad×categoría.",
            _validate_q4_city_category_association,
        ),
        (
            "q4/top_merchants_global",
            "Q4b",
            "Top K merchants globales + ciudad principal.",
            _validate_q4_top_merchants_global,
        ),
        (
            "q4/top_merchants_distribution_by_city",
            "Q4c",
            "Distribución del top K por ciudad principal.",
            _validate_q4_top_distribution,
        ),
        (
            "q4/merchant_popularity_by_city",
            "Q4+",
            "Popularidad local por ciudad (complemento).",
            _validate_q4_merchant_popularity,
        ),
        (
            "q5_advisory_summary",
            "Q5+",
            "Agregados para Q5a–d y cruce installments × monto.",
            _validate_q5_advisory_summary,
        ),
    ]
    for folder, q_label, blurb, validator in extras:
        sub = gold_root / folder
        header = SectionReport(question=q_label, pdf_expectation=blurb)
        if not sub.is_dir():
            header.findings.append(
                Finding(f"{folder}_path", False, f"No existe {sub}", "warn")
            )
            sections.append(header)
            continue
        try:
            df = _load_gold_parquet(sub)
        except FileNotFoundError as e:
            header.findings.append(Finding(f"{folder}_data", False, str(e), "warn"))
            sections.append(header)
            continue
        header.findings.extend(validator(df))
        header.findings.insert(
            0,
            Finding(
                f"{folder}_rows",
                True,
                f"Filas: {len(df):,}; columnas: {list(df.columns)}",
                "info",
            ),
        )
        sections.append(header)

    return sections, pdf_excerpt


def _print_report(sections: list[SectionReport], pdf_excerpt: str | None) -> None:
    print("=" * 72)
    print("Validación: PDF del challenge vs Gold (Parquet)")
    print("=" * 72)
    if pdf_excerpt:
        print("\n--- Extracto del PDF (pdftotext, primeros 1200 caracteres) ---\n")
        print(pdf_excerpt[:1200].strip())
        if len(pdf_excerpt) > 1200:
            print("\n[… truncado …]")
    else:
        print(
            "\n(Instala `poppler-utils` para ver extracto del PDF; "
            "se usan resúmenes embebidos por pregunta.)\n"
        )

    for sec in sections:
        print("\n" + "-" * 72)
        print(f"{sec.question} — Esperado (PDF / brief)")
        print(sec.pdf_expectation)
        print("-" * 72)
        for f in sec.findings:
            mark = "OK " if f.ok else "!! "
            print(f"  [{f.severity:5}] {mark}{f.code}: {f.message}")

    print("\n" + "=" * 72)
    print("Leyenda: error = desalineación con PDF o datos inválidos; warn = revisar; info = OK / contexto.")
    print("=" * 72)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Gold vs Billups challenge PDF")
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=ROOT / "data" / "gold",
        help="Directorio gold (q1_results, q4/results, q5_results, etc.)",
    )
    parser.add_argument(
        "--pdf",
        type=Path,
        default=DEFAULT_PDF,
        help="Ruta al PDF del challenge",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 si hay hallazgos con severity error",
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=None,
        help="Escribir reporte JSON a esta ruta",
    )
    args = parser.parse_args()

    sections, excerpt = run_validation(args.gold_root, args.pdf)
    _print_report(sections, excerpt)

    payload = [
        {
            "question": s.question,
            "pdf_expectation": s.pdf_expectation,
            "findings": [
                {
                    "code": f.code,
                    "ok": f.ok,
                    "message": f.message,
                    "severity": f.severity,
                }
                for f in s.findings
            ],
        }
        for s in sections
    ]
    if args.json:
        args.json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nJSON escrito en {args.json}")

    err_count = sum(
        1
        for s in sections
        for f in s.findings
        if not f.ok and f.severity == "error"
    )
    warn_count = sum(
        1
        for s in sections
        for f in s.findings
        if not f.ok and f.severity == "warn"
    )
    print(
        f"\nResumen: {err_count} hallazgo(s) con severity=error, "
        f"{warn_count} con severity=warn (solo warn no hace fallar --strict)."
    )

    strict_fail = args.strict and err_count > 0
    if strict_fail:
        print("\nFallo (--strict): listado de hallazgos con severity=error:")
        for s in sections:
            for f in s.findings:
                if not f.ok and f.severity == "error":
                    print(f"  - [{s.question}] {f.code}: {f.message}")

    exit_code = 1 if strict_fail else 0
    print(f"\nCódigo de salida: {exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
