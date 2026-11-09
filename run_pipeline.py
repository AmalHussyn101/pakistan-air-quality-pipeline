"""
run_pipeline.py - runs the WHOLE project automatically, step by step.

    python run_pipeline.py                 # full run (with cross-validation)
    python run_pipeline.py --quick         # fast test run (no cross-validation)
    python run_pipeline.py --steps clean,features    # run only some steps

Steps (in order):
    1. generate  - create the synthetic CSV (skipped if it already exists, see config)
    2. clean     - PySpark: fix names, duplicates, impossible and missing values
    3. features  - PySpark: season, yesterday's pollution, 7-day averages, future targets
    4. train     - PySpark MLlib: AQI/visibility regression + smog classification, tuned with CV
    5. forecast  - PySpark: next 3 days for every city with the best saved models
    6. evaluate  - charts (PNG) + summary report (Markdown)

Everything that happens is written to the screen and to logs/pipeline_<date_time>.log
All settings are in config/config.yaml
"""

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)                    # all paths in config.yaml are relative to the project folder
sys.path.insert(0, str(ROOT))

from src import utils  # noqa: E402

ALL_STEPS = ["generate", "clean", "features", "train", "forecast", "evaluate"]
SPARK_STEPS = {"clean", "features", "train", "forecast"}


def step_generate(cfg, logger):
    raw = Path(cfg["paths"]["raw_data"])
    if raw.exists() and not cfg["data"]["regenerate"]:
        logger.info("Raw data already exists (%s) - skipping generation", raw)
        return
    from src import generate_data
    generate_data.OUTPUT_PATH = raw
    generate_data.START_DATE = cfg["data"]["start_date"]
    generate_data.END_DATE = cfg["data"]["end_date"]
    generate_data.main()
    logger.info("Synthetic data created -> %s", raw)


def main():
    parser = argparse.ArgumentParser(description="Pakistan air quality ML pipeline")
    parser.add_argument("--quick", action="store_true", help="fast run without cross-validation")
    parser.add_argument("--steps", default=",".join(ALL_STEPS), help="comma list, e.g. clean,features")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()

    cfg = utils.load_config(args.config)
    if args.quick:
        cfg["training"]["quick_run"] = True
    utils.ensure_dirs(cfg)
    logger = utils.get_logger(cfg)

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = [s for s in steps if s not in ALL_STEPS]
    if unknown:
        logger.error("Unknown step(s): %s. Choose from: %s", unknown, ALL_STEPS)
        sys.exit(1)

    logger.info("=" * 70)
    logger.info("%s", cfg["project"]["name"])
    logger.info("Steps to run: %s", " -> ".join(steps))
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

            if step == "generate":
                step_generate(cfg, logger)
            elif step == "clean":
                from src import clean
                clean.run(spark, cfg, logger)
            elif step == "features":
                from src import features
                features.run(spark, cfg, logger)
            elif step == "train":
                from src import train
                train.run(spark, cfg, logger)
            elif step == "forecast":
                from src import forecast
                forecast.run(spark, cfg, logger)
            elif step == "evaluate":
                from src import evaluate
                evaluate.run(cfg, logger)

            logger.info("STEP %d/%d: %s ... finished in %.1f s", i, len(steps), step.upper(), time.time() - t0)

        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED in %.1f minutes", (time.time() - start_all) / 60)
        logger.info("Results: %s | Charts: %s | Report: %s",
                    cfg["paths"]["results_dir"], cfg["paths"]["figures_dir"], cfg["paths"]["report_file"])
        logger.info("=" * 70)
    except Exception:
        logger.exception("PIPELINE FAILED at step '%s'. See the error above.", current)
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
