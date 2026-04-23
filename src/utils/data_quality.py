"""
Data Quality Checks
====================
Lightweight, dependency-free checks using PySpark native assertions.
Each check returns a dict with {check_name, passed, detail}.

We deliberately avoid importing Great Expectations at runtime to keep
the pipeline fast; GE is reserved for CI-time contract validation.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    detail: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def run_checks(df: DataFrame, layer: str, stream: str) -> list[CheckResult]:
    """Run all DQ checks for a given DataFrame. Raises on critical failures."""
    results: list[CheckResult] = []

    results.append(_check_not_empty(df, layer, stream))
    results.append(_check_no_nulls_on_key_cols(df, layer, stream))
    results.append(_check_no_duplicate_ids(df, layer, stream))

    if layer == "silver" and stream == "electricity_mix":
        results.append(_check_power_non_negative(df))

    if layer == "gold" and stream == "daily_electricity_mix":
        results.append(_check_pct_contribution_sum(df))

    _log_results(results)
    _raise_on_critical(results)
    return results


# ── Individual checks ─────────────────────────────────────────────────────────

def _check_not_empty(df: DataFrame, layer: str, stream: str) -> CheckResult:
    count = df.count()
    return CheckResult(
        check_name="not_empty",
        passed=count > 0,
        detail=f"{layer}/{stream} has {count} rows",
    )


def _check_no_nulls_on_key_cols(df: DataFrame, layer: str, stream: str) -> CheckResult:
    key_cols = {
        "silver": {
            "electricity_mix": ["record_id", "zone", "data_timestamp", "source_type"],
            "electricity_flows": ["record_id", "zone", "data_timestamp", "counterpart_zone"],
        },
        "gold": {
            "daily_electricity_mix": ["zone", "date", "source_type"],
            "electricity_imports": ["zone", "date", "counterpart_zone"],
            "electricity_exports": ["zone", "date", "counterpart_zone"],
        },
    }
    cols = key_cols.get(layer, {}).get(stream, [])
    if not cols:
        return CheckResult("no_nulls_on_key_cols", True, "No key columns configured — skipped.")

    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cols]
    ).collect()[0].asDict()

    failed_cols = {c: v for c, v in null_counts.items() if v > 0}
    return CheckResult(
        check_name="no_nulls_on_key_cols",
        passed=len(failed_cols) == 0,
        detail=f"Null counts: {null_counts}",
        extra={"null_counts": null_counts},
    )


def _check_no_duplicate_ids(df: DataFrame, layer: str, stream: str) -> CheckResult:
    if "record_id" not in df.columns:
        return CheckResult("no_duplicate_ids", True, "No record_id column — skipped.")

    total = df.count()
    distinct = df.select("record_id").distinct().count()
    dup_count = total - distinct
    return CheckResult(
        check_name="no_duplicate_ids",
        passed=dup_count == 0,
        detail=f"{dup_count} duplicate record_ids found ({total} total, {distinct} distinct)",
    )


def _check_power_non_negative(df: DataFrame) -> CheckResult:
    if "power_mw" not in df.columns:
        return CheckResult("power_non_negative", True, "No power_mw column — skipped.")
    neg_count = df.filter(F.col("power_mw") < 0).count()
    return CheckResult(
        check_name="power_non_negative",
        passed=neg_count == 0,
        detail=f"{neg_count} rows with negative power_mw",
    )

def _check_pct_contribution_sum(df: DataFrame) -> CheckResult:
    """
    For each (zone, date), pct_contribution should sum to ~100 (allow ±1 rounding).
    """
    if "pct_contribution" not in df.columns:
        return CheckResult("pct_contribution_sum", True, "Column absent — skipped.")

    totals = (
        df.groupBy("zone", "date")
        .agg(F.sum("pct_contribution").alias("total_pct"))
    )
    bad = totals.filter((F.col("total_pct") < 99) | (F.col("total_pct") > 101)).count()
    return CheckResult(
        check_name="pct_contribution_sum",
        passed=bad == 0,
        detail=f"{bad} (zone, date) groups where contributions don't sum to ~100%",
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log_results(results: list[CheckResult]) -> None:
    for r in results:
        level = logging.INFO if r.passed else logging.ERROR
        logger.log(level, "[DQ] %-35s %s  %s", r.check_name, "✓" if r.passed else "✗", r.detail)


def _raise_on_critical(results: list[CheckResult]) -> None:
    critical = ["not_empty", "no_nulls_on_key_cols", "no_duplicate_ids"]
    failures = [r for r in results if not r.passed and r.check_name in critical]
    if failures:
        names = [r.check_name for r in failures]
        raise ValueError(f"Critical data-quality checks failed: {names}")
