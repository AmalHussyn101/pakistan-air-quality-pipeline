"""
clean.py - STEP 2 of the pipeline (PySpark).

Reads the raw CSV and fixes every data problem:
  1. City names written badly ("lahore " -> "Lahore")
  2. Duplicate rows
  3. Impossible sensor values (e.g. pm25 = -999, humidity = 150) -> treated as missing
  4. Missing values -> filled with the average of the nearest earlier and later
     valid reading of the SAME city (like a sensor repair person would do)

It also saves a data-quality report and simple city insights (Spark group-by).

Output: data/processed/clean_data.parquet
        reports/results/data_quality_report.csv
        reports/results/city_summary.csv
        reports/results/city_monthly_summary.csv
        reports/results/correlation_with_aqi.csv
"""

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.utils import save_csv, to_pandas

SENSOR_COLUMNS = ["temperature_c", "humidity_pct", "wind_speed_kmh", "rainfall_mm",
                  "pm25", "pm10", "co2_ppm", "no2_ppb", "so2_ppb", "co_ppm", "ozone_ppb"]
OTHER_NUMERIC = ["traffic_level", "industrial_activity", "crop_burning_index", "aqi", "visibility_km"]


def run(spark, cfg, logger):
    paths = cfg["paths"]
    quality = []                                    # rows of the data-quality report

    # ---------------- 1. Load ----------------
    raw = spark.read.csv(paths["raw_data"], header=True, inferSchema=True)
    n_raw = raw.count()
    logger.info("Loaded raw data: %d rows, %d columns", n_raw, len(raw.columns))
    quality.append(("Raw rows", n_raw))

    # ---------------- 2. Fix city names ----------------
    fixed_city = F.initcap(F.trim(F.col("city")))
    n_bad_names = raw.filter(F.col("city") != fixed_city).count()
    df = raw.withColumn("city", fixed_city)
    df = df.filter(F.col("city").isin(cfg["data"]["cities"]))
    logger.info("Fixed %d badly written city names", n_bad_names)
    quality.append(("Badly written city names fixed", n_bad_names))

    # ---------------- 3. Dates + duplicates ----------------
    df = df.withColumn("date", F.to_date(F.col("date")))
    df = df.dropDuplicates(["city", "date"])
    n_after_dupes = df.count()
    logger.info("Removed %d duplicate rows", n_raw - n_after_dupes)
    quality.append(("Duplicate rows removed", n_raw - n_after_dupes))

    # ---------------- 4. Impossible values -> missing ----------------
    for col, (low, high) in cfg["cleaning"]["valid_ranges"].items():
        bad = (F.col(col) < low) | (F.col(col) > high)
        n_bad = df.filter(bad).count()
        if n_bad:
            logger.info("Column %-15s: %d impossible values set to missing", col, n_bad)
            quality.append((f"Impossible values in {col}", n_bad))
        df = df.withColumn(col, F.when(bad, F.lit(None)).otherwise(F.col(col)).cast("double"))

    # ---------------- 5. Fill missing values ----------------
    missing_before = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in SENSOR_COLUMNS]).first()
    for c in SENSOR_COLUMNS:
        quality.append((f"Missing values filled in {c}", int(missing_before[c] or 0)))
    logger.info("Missing values before filling: %d", sum(int(v or 0) for v in missing_before))

    w_prev = Window.partitionBy("city").orderBy("date").rowsBetween(Window.unboundedPreceding, -1)
    w_next = Window.partitionBy("city").orderBy("date").rowsBetween(1, Window.unboundedFollowing)
    filled = []
    for c in df.columns:
        if c in SENSOR_COLUMNS:
            prev_val = F.last(c, ignorenulls=True).over(w_prev)
            next_val = F.first(c, ignorenulls=True).over(w_next)
            filled.append(F.round(F.coalesce(F.col(c), (prev_val + next_val) / 2, prev_val, next_val), 2).alias(c))
        else:
            filled.append(F.col(c))
    df = df.select(filled)

    missing_after = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in SENSOR_COLUMNS]).first()
    left = sum(int(v or 0) for v in missing_after)
    logger.info("Missing values after filling: %d", left)
    df = df.dropna(subset=SENSOR_COLUMNS + OTHER_NUMERIC)

    n_clean = df.count()
    quality.append(("Clean rows", n_clean))
    logger.info("Clean data: %d rows (%d cities)", n_clean, df.select("city").distinct().count())

    # ---------------- 6. Save ----------------
    df = df.orderBy("city", "date")
    df.write.mode("overwrite").parquet(paths["clean_data"])
    logger.info("Saved clean data -> %s", paths["clean_data"])
    # Read the saved file back and keep it in memory, so the insight queries below are fast
    df = spark.read.parquet(paths["clean_data"]).cache()
    save_csv(pd.DataFrame(quality, columns=["check", "count"]), cfg, "data_quality_report.csv", logger)

    # ---------------- 7. Insights with Spark ----------------
    city_summary = (df.groupBy("city")
                    .agg(F.round(F.avg("aqi"), 1).alias("avg_aqi"),
                         F.round(F.avg("pm25"), 1).alias("avg_pm25"),
                         F.round(F.avg("visibility_km"), 2).alias("avg_visibility_km"),
                         F.max("aqi").alias("max_aqi"),
                         F.sum((F.col("smog_level") == "Severe").cast("int")).alias("severe_days"),
                         F.sum((F.col("smog_level") == "Hazardous").cast("int")).alias("hazardous_days"))
                    .orderBy(F.desc("avg_aqi")))
    save_csv(to_pandas(city_summary), cfg, "city_summary.csv", logger)

    monthly = (df.groupBy("city", F.month("date").alias("month"))
               .agg(F.round(F.avg("aqi"), 1).alias("avg_aqi"),
                    F.round(F.avg("pm25"), 1).alias("avg_pm25"),
                    F.round(F.avg("visibility_km"), 2).alias("avg_visibility_km"))
               .orderBy("city", "month"))
    save_csv(to_pandas(monthly), cfg, "city_monthly_summary.csv", logger)

    numeric = SENSOR_COLUMNS + ["traffic_level", "industrial_activity", "crop_burning_index", "visibility_km"]
    corr = [(c, round(df.stat.corr(c, "aqi"), 3)) for c in numeric]
    corr_pdf = pd.DataFrame(corr, columns=["feature", "correlation_with_aqi"])
    corr_pdf = corr_pdf.reindex(corr_pdf["correlation_with_aqi"].abs().sort_values(ascending=False).index)
    save_csv(corr_pdf, cfg, "correlation_with_aqi.csv", logger)

    return df
