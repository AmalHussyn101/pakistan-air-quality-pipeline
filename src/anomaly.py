"""
anomaly.py - STEP 4 of the pipeline: ANOMALY DETECTION (unsupervised ML, PySpark MLlib KMeans).

Finds days whose pollution mix is unusual - possible signs of fires, chemical emissions,
dust storms or sudden pollution events - in two ways:

  1. KMeans clustering: every city-day is described by its pollutants and weather (standardised).
     KMeans groups "normal" days into clusters. A day that is very far from every cluster centre
     (top 1% distance) is unusual -> anomaly.
  2. Spike rule: PM2.5 more than double its average of the previous 7 days -> sudden spike.

Only Spark expressions and MLlib are used (no Python UDFs).

Output: reports/results/anomalies.csv   (all flagged days, newest first)
"""

import pandas as pd
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.utils import save_csv, to_pandas

FEATURES = ["pm25", "pm10", "no2_ugm3", "so2_ugm3", "co_ugm3", "ozone_ugm3", "dust_ugm3",
            "temperature_c", "humidity_pct", "wind_speed_kmh"]
K = 4
TOP_SHARE = 0.01      # the 1% most unusual days are anomalies


def run(spark, cfg, logger):
    df = spark.read.parquet(cfg["paths"]["clean_data"]).filter(F.col("is_future") == 0)

    # 1) standardise every feature with Spark (mean 0, std 1)
    stats = df.agg(*[F.avg(c).alias(f"m_{c}") for c in FEATURES],
                   *[F.stddev(c).alias(f"s_{c}") for c in FEATURES]).first()
    z_cols = []
    for c in FEATURES:
        sd = stats[f"s_{c}"] or 1.0
        df = df.withColumn(f"z_{c}", (F.col(c) - F.lit(stats[f"m_{c}"])) / F.lit(sd if sd > 0 else 1.0))
        z_cols.append(f"z_{c}")

    # 2) KMeans clustering
    data = VectorAssembler(inputCols=z_cols, outputCol="features", handleInvalid="skip").transform(df).cache()
    model = KMeans(k=K, seed=cfg["training"]["seed"], featuresCol="features", predictionCol="cluster").fit(data)
    centers = model.clusterCenters()
    scored = model.transform(data)

    # distance of every day to its own cluster centre (built from plain Spark expressions)
    dist = None
    for k, center in enumerate(centers):
        sq = sum((F.col(z) - F.lit(float(center[j]))) ** 2 for j, z in enumerate(z_cols))
        dist = F.when(F.col("cluster") == k, F.sqrt(sq)) if dist is None else dist.when(F.col("cluster") == k, F.sqrt(sq))
    scored = scored.withColumn("distance", dist)
    limit = scored.approxQuantile("distance", [1 - TOP_SHARE], 0.001)[0]

    # 3) spike rule
    w = Window.partitionBy("city").orderBy("date").rowsBetween(-7, -1)
    scored = scored.withColumn("pm25_prev7", F.avg("pm25").over(w))
    flagged = (scored
               .withColumn("kmeans_anomaly", F.col("distance") > F.lit(limit))
               .withColumn("spike", (F.col("pm25") > 2 * F.col("pm25_prev7")) & (F.col("pm25") > 35))
               .filter(F.col("kmeans_anomaly") | F.col("spike")))

    pdf = to_pandas(flagged.select(F.col("date").cast("string").alias("date"), "city", "aqi", "pm25", "pm25_prev7",
                                   "pm10", "no2_ugm3", "so2_ugm3", "dust_ugm3", "wind_speed_kmh",
                                   "distance", "kmeans_anomaly", "spike"))
    if len(pdf):
        def reason(r):
            parts = []
            if r["spike"]:
                parts.append(f"PM2.5 jumped to {r['pm25']:.0f} (7-day average {r['pm25_prev7']:.0f})")
            if r["kmeans_anomaly"]:
                top = max(["pm25", "pm10", "no2_ugm3", "so2_ugm3", "dust_ugm3"],
                          key=lambda c: (r[c] - stats[f"m_{c}"]) / (stats[f"s_{c}"] or 1))
                names = {"pm25": "fine particles", "pm10": "coarse dust", "no2_ugm3": "nitrogen dioxide",
                         "so2_ugm3": "sulphur dioxide", "dust_ugm3": "desert dust"}
                parts.append(f"unusual pollution mix (mostly {names[top]})")
            return "; ".join(parts)

        pdf["reason"] = pdf.apply(reason, axis=1)
        pdf["type"] = pdf.apply(lambda r: "Sudden spike" if r["spike"] else "Unusual pattern (KMeans)", axis=1)
        pdf = pdf.sort_values("date", ascending=False)
    else:
        pdf = pd.DataFrame(columns=["date", "city", "aqi", "pm25", "type", "reason"])
    save_csv(pdf, cfg, "anomalies.csv", logger)

    recent = pdf[pd.to_datetime(pdf["date"]) >= pd.Timestamp.today() - pd.Timedelta(days=14)] if len(pdf) else pdf
    logger.info("Anomaly detection (KMeans k=%d + spike rule): %d unusual days found, %d in the last 14 days",
                K, len(pdf), len(recent))
    for _, r in recent.head(10).iterrows():
        logger.warning("ANOMALY %s %-10s %s", r["date"], r["city"], r["reason"])
    data.unpersist()
    return pdf
