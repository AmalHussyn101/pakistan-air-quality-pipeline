"""
features.py - STEP 3 of the pipeline (PySpark Window functions).

For every city and every observed day t we build 7 training rows, one for each
forecast horizon h = 1..7 days ahead. Each row contains:

  TODAY (day t) - what we know now:
     pollution: PM2.5, PM10, NO2, SO2, CO, ozone, CO2, dust, AQI, visibility
     weather:   temperature, humidity, wind, rain
     history:   yesterday's PM2.5/AQI (lags), 3- and 7-day averages, 7-day max, AQI change
  TARGET DAY (day t+h) - what the weather forecast says about that day:
     temperature, humidity, wind, rain, month, season, smog season, crop-burning season, weekend
  horizon:  h (1..7)
  TARGETS:  AQI, visibility and smog level on day t+h

Why the target-day weather? Smog depends mostly on weather: calm, cold, dry days trap pollution,
wind and rain clean it. Real air-quality forecasts work the same way. In training we use the
weather that actually happened on day t+h; when forecasting we use the weather forecast.

Output: data/processed/features.parquet
"""

import math

from pyspark.sql import Window
from pyspark.sql import functions as F

SMOG_LEVELS = ["Low", "Moderate", "Severe", "Hazardous"]   # label 0, 1, 2, 3

TODAY_FEATURES = [
    "pm25", "pm10", "no2_ugm3", "so2_ugm3", "co_ugm3", "ozone_ugm3", "co2_ppm", "dust_ugm3",
    "aqi", "visibility_km", "temperature_c", "humidity_pct", "wind_speed_kmh", "rainfall_mm",
    "pm25_lag1", "pm25_lag2", "aqi_lag1", "aqi_lag2", "pm25_avg7", "aqi_avg3", "aqi_avg7", "aqi_max7",
    "aqi_change_1d", "rainfall_sum3", "wind_avg3",
]
TARGET_DAY_FEATURES = [
    "t_temperature_c", "t_humidity_pct", "t_wind_speed_kmh", "t_rainfall_mm",
    "t_month", "t_doy_sin", "t_doy_cos", "t_is_weekend", "t_is_smog_season", "t_is_crop_burning_season",
]
NUMERIC_FEATURES = TODAY_FEATURES + TARGET_DAY_FEATURES + ["horizon"]
CATEGORICAL_FEATURES = ["city", "t_season"]


def smog_label(col):
    expr = F.when(F.col(col) == SMOG_LEVELS[0], 0.0)
    for i, level in enumerate(SMOG_LEVELS[1:], start=1):
        expr = expr.when(F.col(col) == level, float(i))
    return expr


def calendar(date_col):
    """Calendar features of a date column (used for the target day)."""
    m, d = F.month(date_col), F.dayofmonth(date_col)
    return {
        "t_month": m.cast("double"),
        "t_doy_sin": F.sin(2 * math.pi * F.dayofyear(date_col) / 365.25),
        "t_doy_cos": F.cos(2 * math.pi * F.dayofyear(date_col) / 365.25),
        "t_is_weekend": F.when(F.dayofweek(date_col) == 1, 1.0).otherwise(0.0),          # Sunday
        "t_is_smog_season": F.when(m.isin(11, 12, 1) | ((m == 10) & (d >= 15)) | ((m == 2) & (d <= 15)), 1.0).otherwise(0.0),
        "t_is_crop_burning_season": F.when(((m == 10) & (d >= 10)) | (m == 11) |
                                           ((m == 4) & (d >= 15)) | ((m == 5) & (d <= 20)), 1.0).otherwise(0.0),
        "t_season": F.when(m.isin(12, 1, 2), "Winter").when(m.isin(3, 4), "Spring").when(m.isin(5, 6), "Summer")
                     .when(m.isin(7, 8, 9), "Monsoon").otherwise("Autumn"),
    }


def build(df, horizons):
    w = Window.partitionBy("city").orderBy("date")
    w3, w7 = w.rowsBetween(-2, 0), w.rowsBetween(-6, 0)

    # ---------- today's history features ----------
    df = (df
          .withColumn("pm25_lag1", F.lag("pm25", 1).over(w)).withColumn("pm25_lag2", F.lag("pm25", 2).over(w))
          .withColumn("aqi_lag1", F.lag("aqi", 1).over(w)).withColumn("aqi_lag2", F.lag("aqi", 2).over(w))
          .withColumn("pm25_avg7", F.avg("pm25").over(w7))
          .withColumn("aqi_avg3", F.avg("aqi").over(w3)).withColumn("aqi_avg7", F.avg("aqi").over(w7))
          .withColumn("aqi_max7", F.max("aqi").over(w7))
          .withColumn("aqi_change_1d", F.col("aqi") - F.col("aqi_lag1"))
          .withColumn("rainfall_sum3", F.sum("rainfall_mm").over(w3))
          .withColumn("wind_avg3", F.avg("wind_speed_kmh").over(w3)))

    # ---------- one block of rows per horizon ----------
    blocks = []
    for h in horizons:
        b = (df
             .withColumn("horizon", F.lit(float(h)))
             .withColumn("target_date", F.lead("date", h).over(w))
             .withColumn("t_temperature_c", F.lead("temperature_c", h).over(w))
             .withColumn("t_humidity_pct", F.lead("humidity_pct", h).over(w))
             .withColumn("t_wind_speed_kmh", F.lead("wind_speed_kmh", h).over(w))
             .withColumn("t_rainfall_mm", F.lead("rainfall_mm", h).over(w))
             .withColumn("aqi_target", F.lead("aqi", h).over(w))
             .withColumn("visibility_target", F.lead("visibility_km", h).over(w))
             .withColumn("smog_level_target", F.lead("smog_level", h).over(w)))
        for name, expr in calendar(F.col("target_date")).items():
            b = b.withColumn(name, expr)
        blocks.append(b)

    out = blocks[0]
    for b in blocks[1:]:
        out = out.unionByName(b)

    out = (out
           .filter((F.col("is_future") == 0) & F.col("aqi_lag2").isNotNull() & F.col("target_date").isNotNull())
           .withColumn("smog_label_target", smog_label("smog_level_target"))
           .withColumn("smog_label_today", smog_label("smog_level")))
    for c in NUMERIC_FEATURES:
        out = out.withColumn(c, F.col(c).cast("double"))
    keep = (["date", "city", "target_date", "is_future"] + NUMERIC_FEATURES + ["t_season"] +
            ["aqi_target", "visibility_target", "smog_level_target", "smog_label_target", "smog_label_today"])
    return out.select(keep)


def run(spark, cfg, logger):
    paths = cfg["paths"]
    horizons = cfg["targets"]["horizons"]
    df = spark.read.parquet(paths["clean_data"])
    feats = build(df, horizons)
    feats.write.mode("overwrite").parquet(paths["features_data"])

    feats = spark.read.parquet(paths["features_data"])
    n_train = feats.filter(F.col("aqi_target").isNotNull()).count()
    n_forecast = feats.filter(F.col("aqi_target").isNull()).count()
    logger.info("Created %d features (%d numeric + %d categorical) for horizons %s",
                len(NUMERIC_FEATURES) + len(CATEGORICAL_FEATURES), len(NUMERIC_FEATURES),
                len(CATEGORICAL_FEATURES), horizons)
    logger.info("Feature rows: %d with known outcome (for training/testing) + %d to forecast", n_train, n_forecast)
    return feats
