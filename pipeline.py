"""
ETL Pipeline Entry Point
========================
Orchestrates the full Bronze → Silver → Gold pipeline.

Usage
-----
    # Full load (ignores watermarks):
    python pipeline.py --full-load

    # Incremental run (default):
    python pipeline.py

    # Run only up to a specific layer:
    python pipeline.py --layer bronze
    python pipeline.py --layer silver
    python pipeline.py --layer gold
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

# ── Logging setup (before any imports that log) ───────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")

from config import Config
from src.bronze.ingest_electricity_mix import ingest as bronze_mix
from src.bronze.ingest_electricity_flows import ingest as bronze_flows
from src.silver.transform_electricity_mix import transform as silver_mix
from src.silver.transform_electricity_flows import transform as silver_flows
from src.gold.build_daily_mix import build as gold_mix
from src.gold.build_net_flows import build as gold_net_flows
from src.utils.data_quality import run_checks
from src.utils.spark_session import get_spark


def run_bronze(full_load: bool = False) -> None:
    logger.info("═" * 60)
    logger.info("LAYER: BRONZE")
    logger.info("═" * 60)
    paths_mix   = bronze_mix(full_load=full_load)
    paths_flows = bronze_flows(full_load=full_load)
    logger.info("Bronze mix written:   %d file(s)", len(paths_mix))
    logger.info("Bronze flows written: %d file(s)", len(paths_flows))


def run_silver(spark=None) -> None:
    logger.info("═" * 60)
    logger.info("LAYER: SILVER")
    logger.info("═" * 60)
    spark = spark or get_spark()

    mix_df   = silver_mix(spark)
    flows_df = silver_flows(spark)

    run_checks(mix_df,   layer="silver", stream="electricity_mix")
    run_checks(flows_df, layer="silver", stream="electricity_flows")


def run_gold(spark=None) -> None:
    logger.info("═" * 60)
    logger.info("LAYER: GOLD")
    logger.info("═" * 60)
    spark = spark or get_spark()

    daily_mix_df          = gold_mix(spark)
    imports_df, exports_df = gold_net_flows(spark)

    run_checks(daily_mix_df, layer="gold", stream="daily_electricity_mix")
    run_checks(imports_df,   layer="gold", stream="electricity_imports")
    run_checks(exports_df,   layer="gold", stream="electricity_exports")


def main() -> None:
    parser = argparse.ArgumentParser(description="Electricity Maps ETL Pipeline")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Which layer(s) to run (default: all)",
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        default=False,
        help="Ignore watermarks and ingest all available history.",
    )
    args = parser.parse_args()

    start = time.time()
    logger.info("Pipeline started at %s", datetime.now(tz=timezone.utc).isoformat())
    logger.info("Config: zone=%s  root=%s  full_load=%s",
                Config.ZONE, Config.DATA_LAKE_ROOT, args.full_load)

    try:
        spark = get_spark() if args.layer in ("silver", "gold", "all") else None

        if args.layer in ("bronze", "all"):
            run_bronze(full_load=args.full_load)

        if args.layer in ("silver", "all"):
            run_silver(spark)

        if args.layer in ("gold", "all"):
            run_gold(spark)

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        sys.exit(1)
    finally:
        elapsed = time.time() - start
        logger.info("Pipeline finished in %.1f seconds.", elapsed)


if __name__ == "__main__":
    main()
