"""
run_pipeline.py - the AUTOMATED Smog Early-Warning pipeline.

    python run_pipeline.py --daily      # what the scheduler runs every morning (see run_daily.bat)
    python run_pipeline.py              # full run: every step, models retrained with cross-validation
    python run_pipeline.py --quick      # full run without cross-validation (fast test)
    python run_pipeline.py --steps forecast,alerts   # run only some steps

Steps (in order):
    1. ingest    - downloads REAL air-quality and weather data (Open-Meteo), plus the 7-day weather forecast
    2. clean     - PySpark: fix names, duplicates, impossible and missing values
    3. features  - PySpark: season, yesterday's pollution, 7-day averages, future targets
    4. train     - PySpark MLlib: AQI/visibility regression + smog classification, tuned with CV
    5. forecast  - PySpark: next 7 days for every city with the best saved models (printed as a table)
    6. monitor   - compares past forecasts with what really happened; asks for retraining if needed
    7. alerts    - writes the Smog Early-Warning Bulletin with actions for each city
    8. evaluate  - result charts + written summary report

--daily mode (the automation):
    * the newest real data (up to yesterday) and a fresh weather forecast are downloaded
    * the 'train' step runs ONLY when needed: no models yet, models older than
      'retrain_every_days', or the monitor step found that forecasts got worse
    * otherwise the saved models are reused, so the daily run takes only a few minutes

Everything is written to the screen and to logs/pipeline_<date_time>.log
All settings are in config/config.yaml
"""

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)                    # all paths in config.yaml are relative to the project folder
sys.path.insert(0, str(ROOT))

from src import utils  # noqa: E402

ALL_STEPS = ["ingest", "clean", "features", "train", "forecast", "monitor", "alerts", "evaluate"]
SPARK_STEPS = {"clean", "features", "train", "forecast"}


def retrain_reason(cfg):
    """Return why the models must be retrained, or None if the saved models are still fine."""
    models = Path(cfg["paths"]["models_dir"])
    best = Path(cfg["paths"]["results_dir"]) / "best_models.csv"
    if not best.exists():
        return "no trained models found"
    names = cfg["targets"]["regression"] + [cfg["targets"]["classification"]]
    missing = [f"{t}_h{h}" for t in names for h in cfg["targets"]["horizons"] if not (models / f"{t}_h{h}").exists()]
    if missing:
        return f"models missing for {len(missing)} forecasts (e.g. {missing[0]})"
    if (models / "RETRAIN_NEEDED").exists():
        return "monitoring found that forecast errors increased"
    age = (datetime.now() - datetime.fromtimestamp(best.stat().st_mtime)).days
    limit = cfg.get("automation", {}).get("retrain_every_days", 7)
    if age >= limit:
        return f"models are {age} days old (limit {limit})"
    return None


def main():
    parser = argparse.ArgumentParser(description="Pakistan smog early-warning pipeline")
    parser.add_argument("--daily", action="store_true", help="scheduled daily run (smart retraining)")
    parser.add_argument("--quick", action="store_true", help="train without cross-validation")
    parser.add_argument("--steps", default=None, help="comma list, e.g. forecast,alerts")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()

    cfg = utils.load_config(args.config)
    if args.quick:
        cfg["training"]["quick_run"] = True
    utils.ensure_dirs(cfg)
    logger = utils.get_logger(cfg)

    steps = [s.strip() for s in (args.steps or ",".join(ALL_STEPS)).split(",") if s.strip()]
    unknown = [s for s in steps if s not in ALL_STEPS]
    if unknown:
        logger.error("Unknown step(s): %s. Choose from: %s", unknown, ALL_STEPS)
        sys.exit(1)

    if args.daily:
        reason = retrain_reason(cfg)
        if reason:
            logger.info("Retraining models because: %s", reason)
        else:
            steps = [s for s in steps if s != "train"]
            logger.info("Saved models are recent and accurate - skipping retraining")

    logger.info("=" * 70)
    logger.info("%s", cfg["project"]["name"])
    logger.info("Mode: %s | Steps: %s", "DAILY (scheduled)" if args.daily else "MANUAL", " -> ".join(steps))
    logger.info("=" * 70)

    spark, current, start_all = None, None, time.time()
    try:
        for i, step in enumerate(steps, start=1):
            current = step
            logger.info("STEP %d/%d: %s ... started", i, len(steps), step.upper())
            t0 = time.time()
            if step in SPARK_STEPS and spark is None:
                spark = utils.get_spark(cfg)
                logger.info("Spark %s started (%s, driver memory %s)",
                            spark.version, cfg["spark"]["master"], cfg["spark"]["driver_memory"])

            if step == "ingest":
                from src import ingest
                ingest.run(cfg, logger)
            elif step == "clean":
                from src import clean
                clean.run(spark, cfg, logger)
            elif step == "features":
                from src import features
                features.run(spark, cfg, logger)
            elif step == "train":
                from src import train
                train.run(spark, cfg, logger)
                (Path(cfg["paths"]["models_dir"]) / "RETRAIN_NEEDED").unlink(missing_ok=True)
            elif step == "forecast":
                from src import forecast
                forecast.run(spark, cfg, logger)
            elif step == "monitor":
                from src import monitor
                monitor.run(cfg, logger)
            elif step == "alerts":
                from src import alerts
                alerts.run(cfg, logger)
            elif step == "evaluate":
                from src import evaluate
                evaluate.run(cfg, logger)

            logger.info("STEP %d/%d: %s ... finished in %.1f s", i, len(steps), step.upper(), time.time() - t0)

        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED in %.1f minutes", (time.time() - start_all) / 60)
        logger.info("Bulletin: %s | Results: %s | Report: %s", cfg["paths"].get("alerts_dir", "reports/alerts"),
                    cfg["paths"]["results_dir"], cfg["paths"]["report_file"])
        logger.info("=" * 70)
    except Exception:
        logger.exception("PIPELINE FAILED at step '%s'. See the error above.", current)
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
