"""
clean.py - STEP 2 of the pipeline (PySpark).

Reads the live daily data and:
  1. removes duplicate city-days
  2. turns impossible values (negative pollution, humidity > 100 ...) into missing
  3. fills missing values with the average of the nearest earlier and later valid day of the
     same city (Spark Window functions); anything still missing gets the city's average
  4. CALCULATES the targets in Spark:
       * AQI with the official US-EPA formula (24-hour PM2.5 and PM10 breakpoints, 2024 update)
       * visibility in km, from PM2.5 and humidity with the Koschmieder formula
         (weather-model visibility ignores smog particles, so it is not used)
       * smog level from AQI: Low (0-150), Moderate (151-200), Severe (201-300), Hazardous (301+)
  5. saves a data-quality report and city insights (Spark group-by)

Future days (weather forecast only) are kept, because the forecast step needs their weather.

Output: data/processed/clean_data.parquet and reports/results/*.csv
"""

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.utils import save_csv, to_pandas

POLLUTION = ["pm25", "pm10", "no2_ugm3", "so2_ugm3", "co_ugm3", "ozone_ugm3", "co2_ppm", "dust_ugm3"]
WEATHER = ["temperature_c", "humidity_pct", "wind_speed_kmh", "rainfall_mm"]

PM25_BP = [(0.0, 9.0, 0, 50), (9.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
           (55.5, 125.4, 151, 200), (125.5, 225.4, 201, 300), (225.5, 325.4, 301, 500)]
PM10_BP = [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
           (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)]


def epa_aqi(col, breakpoints, decimals):
    """US-EPA formula as a Spark expression: AQI = (Ih-Il)/(Ch-Cl) * (C-Cl) + Il"""
    c = F.round(F.col(col), decimals)
    expr = None
    for c_lo, c_hi, i_lo, i_hi in breakpoints:
        value = (i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo
        cond = c <= c_hi
        expr = F.when(cond, value) if expr is None else expr.when(cond, value)
    return expr.otherwise(F.lit(500.0))


def fill_gaps(df, columns):
    w_prev = Window.partitionBy("city").orderBy("date").rowsBetween(Window.unboundedPreceding, -1)
    w_next = Window.partitionBy("city").orderBy("date").rowsBetween(1, Window.unboundedFollowing)
    w_city = Window.partitionBy("city")
    out = []
    for c in df.columns:
        if c in columns:
            prev_val = F.last(c, ignorenulls=True).over(w_prev)
            next_val = F.first(c, ignorenulls=True).over(w_next)
            city_mean = F.avg(c).over(w_city)
            out.append(F.round(F.coalesce(F.col(c), (prev_val + next_val) / 2, prev_val, next_val, city_mean,
                                          F.lit(0.0)), 3).alias(c))
        else:
            out.append(F.col(c))
    return df.select(out)


def run(spark, cfg, logger):
    paths = cfg["paths"]
    quality = []

    raw = spark.read.csv(paths["raw_data"], header=True, inferSchema=True)
    n_raw = raw.count()
    logger.info("Loaded live data: %d rows, %d columns", n_raw, len(raw.columns))
    quality.append(("Rows loaded", n_raw))

    df = (raw.withColumn("city", F.initcap(F.trim(F.col("city"))))
             .withColumn("date", F.to_date(F.col("date")))
             .dropDuplicates(["city", "date"]))
    quality.append(("Duplicate rows removed", n_raw - df.count()))

    for c in POLLUTION + WEATHER + ["visibility_m"]:
        df = df.withColumn(c, F.col(c).cast("double"))

    # Impossible values -> missing
    for col, (low, high) in cfg["cleaning"]["valid_ranges"].items():
        bad = (F.col(col) < low) | (F.col(col) > high)
        n_bad = df.filter(bad).count()
        quality.append((f"Impossible values in {col}", n_bad))
        if n_bad:
            logger.info("Column %-15s: %d impossible values set to missing", col, n_bad)
        df = df.withColumn(col, F.when(bad, F.lit(None)).otherwise(F.col(col)))

    past = df.filter(F.col("is_future") == 0)
    future = df.filter(F.col("is_future") == 1)

    missing = past.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in POLLUTION + WEATHER]).first()
    for c in POLLUTION + WEATHER:
        quality.append((f"Missing values filled in {c}", int(missing[c] or 0)))
    logger.info("Missing values found in past data: %d (filled from neighbouring days)",
                sum(int(v or 0) for v in missing))

    past = fill_gaps(past, POLLUTION + WEATHER)
    future = fill_gaps(future, WEATHER)

    # ---------------- targets, calculated with Spark ----------------
    estimated_vis = F.lit(3.912) / (F.lit(0.02) + F.lit(0.004) * F.col("pm25") *
                                    F.pow(1 - F.least(F.col("humidity_pct"), F.lit(97.0)) / 100, -0.55))
    past = (past
            .withColumn("aqi", F.round(F.greatest(epa_aqi("pm25", PM25_BP, 1), epa_aqi("pm10", PM10_BP, 0))))
            .withColumn("visibility_km", F.round(F.least(F.greatest(estimated_vis, F.lit(0.05)), F.lit(30.0)), 2))
            .withColumn("smog_level", F.when(F.col("aqi") <= 150, "Low").when(F.col("aqi") <= 200, "Moderate")
                        .when(F.col("aqi") <= 300, "Severe").otherwise("Hazardous")))
    future = (future.withColumn("aqi", F.lit(None).cast("double"))
                    .withColumn("visibility_km", F.lit(None).cast("double"))
                    .withColumn("smog_level", F.lit(None).cast("string")))
    df = past.unionByName(future).drop("visibility_m").orderBy("city", "date")

    df.write.mode("overwrite").parquet(paths["clean_data"])
    df = spark.read.parquet(paths["clean_data"]).cache()
    past = df.filter(F.col("is_future") == 0)
    n_past = past.count()
    quality.append(("Clean past city-days", n_past))
    quality.append(("Forecast-weather city-days", df.count() - n_past))
    logger.info("Clean data: %d past city-days + %d future weather days -> %s",
                n_past, df.count() - n_past, paths["clean_data"])
    save_csv(pd.DataFrame(quality, columns=["check", "count"]), cfg, "data_quality_report.csv", logger)

    # ---------------- insights with Spark ----------------
    city_summary = (past.groupBy("city")
                    .agg(F.round(F.avg("aqi"), 1).alias("avg_aqi"),
                         F.round(F.avg("pm25"), 1).alias("avg_pm25"),
                         F.round(F.avg("visibility_km"), 2).alias("avg_visibility_km"),
                         F.max("aqi").alias("max_aqi"),
                         F.sum((F.col("smog_level") == "Severe").cast("int")).alias("severe_days"),
                         F.sum((F.col("smog_level") == "Hazardous").cast("int")).alias("hazardous_days"))
                    .orderBy(F.desc("avg_aqi")))
    save_csv(to_pandas(city_summary), cfg, "city_summary.csv", logger)

    monthly = (past.groupBy("city", F.month("date").alias("month"))
               .agg(F.round(F.avg("aqi"), 1).alias("avg_aqi"), F.round(F.avg("pm25"), 1).alias("avg_pm25"),
                    F.round(F.avg("visibility_km"), 2).alias("avg_visibility_km"))
               .orderBy("city", "month"))
    save_csv(to_pandas(monthly), cfg, "city_monthly_summary.csv", logger)

    corr = [(c, round(past.stat.corr(c, "aqi") or 0.0, 3)) for c in POLLUTION + WEATHER + ["visibility_km"]]
    corr_pdf = pd.DataFrame(corr, columns=["feature", "correlation_with_aqi"])
    corr_pdf = corr_pdf.reindex(corr_pdf["correlation_with_aqi"].abs().sort_values(ascending=False).index)
    save_csv(corr_pdf, cfg, "correlation_with_aqi.csv", logger)

    save_csv(to_pandas(past.select(F.col("date").cast("string").alias("date"), "city", "aqi")),
             cfg, "observed_aqi.csv", logger)
    return df
