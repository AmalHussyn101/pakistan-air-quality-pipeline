"""
utils.py - small helper tools used by every step of the pipeline:
  * load_config()  : reads config/config.yaml
  * get_logger()   : writes messages to the screen AND to a log file in logs/
  * get_spark()    : starts Spark with laptop-friendly, Windows-safe settings
  * ensure_dirs()  : creates output folders if they are missing
  * to_pandas()    : turns a SMALL Spark DataFrame into a pandas DataFrame
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_config(path="config/config.yaml"):
    """Read the YAML settings file and return it as a Python dictionary."""
    with open(PROJECT_ROOT / path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dirs(cfg):
    """Create every output folder named in the config (if it does not exist yet)."""
    p = cfg["paths"]
    for folder in [p["models_dir"], p["figures_dir"], p["results_dir"], p["logs_dir"],
                   "data/raw", "data/processed"]:
        Path(folder).mkdir(parents=True, exist_ok=True)


def get_logger(cfg, name="pipeline"):
    """Logger that prints to the screen and saves the same lines to logs/pipeline_<time>.log"""
    logger = logging.getLogger(name)
    if logger.handlers:                      # already created in this run
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")

    log_file = Path(cfg["paths"]["logs_dir"]) / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    screen_handler = logging.StreamHandler(sys.stdout)
    screen_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(screen_handler)
    logger.info("Log file: %s", log_file)
    return logger


def get_spark(cfg):
    """Start a local Spark session. Settings come from the 'spark' part of config.yaml."""
    # Use the same Python as our venv (prevents a common Windows error)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # If HADOOP_HOME was not picked up, but winutils is in C:\hadoop, use it
    if os.name == "nt" and "HADOOP_HOME" not in os.environ and Path(r"C:\hadoop\bin\winutils.exe").exists():
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    from pyspark.sql import SparkSession

    s = cfg["spark"]
    spark = (SparkSession.builder
             .appName(s["app_name"])
             .master(s["master"])
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.memory", s["driver_memory"])
             .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel(s["log_level"])
    return spark


def to_pandas(spark_df):
    """Collect a SMALL Spark DataFrame into pandas (safe with any pandas version)."""
    rows = spark_df.collect()
    return pd.DataFrame([r.asDict() for r in rows], columns=spark_df.columns)


def save_csv(pdf, cfg, file_name, logger=None):
    """Save a pandas DataFrame into reports/results/<file_name>."""
    out = Path(cfg["paths"]["results_dir"]) / file_name
    pdf.to_csv(out, index=False)
    if logger:
        logger.info("Saved %s (%d rows)", out, len(pdf))
    return out
