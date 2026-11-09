"""
features.py - STEP 3 of the pipeline (PySpark).

Creates new columns ("features") that help the models, using Spark Window functions
(each city is handled separately and in date order):

  Calendar  : month, day of week, weekend, season, crop-burning season, smog season
  Yesterday : pm25_lag1, aqi_lag1, visibility_km_lag1 ... ("kal ka pollution")
  Averages  : pm25_avg7, aqi_avg7 ("pichle 7 din ka average"), aqi_avg3, rain over 3 days ...
  Change    : aqi_change_1d (how much AQI changed since yesterday)

And the TARGETS (what we want to predict) for 1, 2 and 3 days ahead:
  aqi_h1, aqi_h2, aqi_h3, visibility_km_h1 ..., smog_label_h1 ...

Idea: at the end of day t we know everything about day t and the days before it.
From that we predict day t+1, t+2 and t+3.

Output: data/processed/features.parquet
"""

import math

from pyspark.sql import Window
from pyspark.sql import functions as F

SMOG_LEVELS = ["Low", "Moderate", "Severe", "Hazardous"]   # label 0, 1, 2, 3

# Numeric features given to the models
NUMERIC_FEATURES = [
    # today's weather and activity
    "temperature_c", "humidity_pct", "wind_speed_kmh", "rainfall_mm",
    "traffic_level", "industrial_activity", "crop_burning_index",
    # today's pollution
    "pm25", "pm10", "co2_ppm", "no2_ppb", "so2_ppb", "co_ppm", "ozone_ppb", "aqi", "visibility_km",
    # yesterday and the day before
    "pm25_lag1", "pm25_lag2", "aqi_lag1", "aqi_lag2", "visibility_km_lag1",
    # recent averages
    "pm25_avg7", "aqi_avg7", "aqi_avg3", "aqi_max7", "crop_burning_avg7",
    "rainfall_sum3", "wind_avg3", "humidity_avg3",
    # change
    "aqi_change_1d",
    # calendar
    "month", "day_of_week", "is_weekend", "doy_sin", "doy_cos",
    "is_crop_burning_season", "is_smog_season",
]
CATEGORICAL_FEATURES = ["city", "season"]


def smog_label(col):
    """Turn the smog text into a number the classifier can learn (Low=0 ... Hazardous=3)."""
    expr = F.when(F.col(col) == SMOG_LEVELS[0], 0.0)
    for i, level in enumerate(SMOG_LEVELS[1:], start=1):
        expr = expr.when(F.col(col) == level, float(i))
    return expr


def add_features(df, horizons):
    w = Window.partitionBy("city").orderBy("date")
    w3 = w.rowsBetween(-2, 0)      # today + 2 days before
    w7 = w.rowsBetween(-6, 0)      # today + 6 days before
    month = F.month("date")
    day = F.dayofmonth("date")

    # ---------------- calendar ----------------
    df = (df
          .withColumn("month", month)
          .withColumn("day_of_week", F.dayofweek("date"))                      # 1 = Sunday ... 7 = Saturday
          .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0))
          .withColumn("doy_sin", F.sin(2 * math.pi * F.dayofyear("date") / 365.25))
          .withColumn("doy_cos", F.cos(2 * math.pi * F.dayofyear("date") / 365.25))
          .withColumn("season",
                      F.when(month.isin(12, 1, 2), "Winter")
                       .when(month.isin(3, 4), "Spring")
                       .when(month.isin(5, 6), "Summer")
                       .when(month.isin(7, 8, 9), "Monsoon")
                       .otherwise("Autumn"))
          .withColumn("is_crop_burning_season",
                      F.when(((month == 10) & (day >= 10)) | (month == 11) |
                             ((month == 4) & (day >= 15)) | ((month == 5) & (day <= 20)), 1).otherwise(0))
          .withColumn("is_smog_season",
                      F.when(month.isin(11, 12, 1) | ((month == 10) & (day >= 15)) |
                             ((month == 2) & (day <= 15)), 1).otherwise(0)))

    # ---------------- yesterday's values (lags) ----------------
    for c in ["pm25", "aqi"]:
        df = df.withColumn(f"{c}_lag1", F.lag(c, 1).over(w)).withColumn(f"{c}_lag2", F.lag(c, 2).over(w))
    df = df.withColumn("visibility_km_lag1", F.lag("visibility_km", 1).over(w))

    # ---------------- recent averages ----------------
    df = (df
          .withColumn("pm25_avg7", F.avg("pm25").over(w7))
          .withColumn("aqi_avg7", F.avg("aqi").over(w7))
          .withColumn("aqi_avg3", F.avg("aqi").over(w3))
          .withColumn("aqi_max7", F.max("aqi").over(w7))
          .withColumn("crop_burning_avg7", F.avg("crop_burning_index").over(w7))
          .withColumn("rainfall_sum3", F.sum("rainfall_mm").over(w3))
          .withColumn("wind_avg3", F.avg("wind_speed_kmh").over(w3))
          .withColumn("humidity_avg3", F.avg("humidity_pct").over(w3))
          .withColumn("aqi_change_1d", F.col("aqi") - F.col("aqi_lag1")))

    # ---------------- targets: the future values ----------------
    df = df.withColumn("smog_label", smog_label("smog_level"))
    for h in horizons:
        df = (df
              .withColumn(f"target_date_h{h}", F.date_add("date", h))
              .withColumn(f"aqi_h{h}", F.lead("aqi", h).over(w).cast("double"))
              .withColumn(f"visibility_km_h{h}", F.lead("visibility_km", h).over(w).cast("double"))
              .withColumn(f"smog_level_h{h}", F.lead("smog_level", h).over(w)))
        df = df.withColumn(f"smog_label_h{h}", smog_label(f"smog_level_h{h}"))

    # The first 2 days of each city have no "yesterday" values -> drop them
    df = df.filter(F.col("aqi_lag2").isNotNull())

    # Make every numeric feature a decimal number (models need doubles)
    for c in NUMERIC_FEATURES:
        df = df.withColumn(c, F.col(c).cast("double"))
    return df


def run(spark, cfg, logger):
    paths = cfg["paths"]
    horizons = cfg["targets"]["horizons"]
    df = spark.read.parquet(paths["clean_data"])
    df = add_features(df, horizons)

    n = df.count()
    logger.info("Created %d features (%d numeric + %d categorical) and targets for horizons %s",
                len(NUMERIC_FEATURES) + len(CATEGORICAL_FEATURES), len(NUMERIC_FEATURES),
                len(CATEGORICAL_FEATURES), horizons)
    logger.info("Feature table: %d rows", n)

    df.write.mode("overwrite").parquet(paths["features_data"])
    logger.info("Saved features -> %s", paths["features_data"])
    return df
