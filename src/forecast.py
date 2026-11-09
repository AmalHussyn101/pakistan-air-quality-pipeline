"""
forecast.py - STEP 5 of the pipeline (PySpark).

Uses the saved best models to forecast the NEXT 3 DAYS for every city,
starting from the latest day in the data.

If the data ends "yesterday", then:
    +1 day = today, +2 days = tomorrow, +3 days = the day after tomorrow.

Output: reports/results/latest_forecast.csv
"""

from datetime import timedelta

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.ml.util import JavaMLReader
from pyspark.sql import functions as F

from src.features import SMOG_LEVELS
from src.utils import save_csv, to_pandas

ADVICE = {
    "Low": "Air is acceptable. Sensitive people should watch for symptoms.",
    "Moderate": "Unhealthy. Children, elderly and patients should limit time outdoors.",
    "Severe": "Very unhealthy. Wear a mask, avoid outdoor exercise, keep windows closed.",
    "Hazardous": "Emergency level. Stay indoors; schools and outdoor work should pause.",
}
VALID_RANGE = {"aqi": (0.0, 500.0), "visibility_km": (0.05, 20.0)}


def load_model(model_path):
    """Load a saved Spark model fully inside Java.
    (PipelineModel.load() starts a Python worker to read the metadata, which crashes on
    some Windows + Python 3.12 setups. JavaMLReader avoids that.)"""
    try:
        return JavaMLReader(PipelineModel).load(model_path)
    except Exception:
        return PipelineModel.load(model_path)


def predict(model_path, latest):
    model = load_model(model_path)
    pdf = to_pandas(model.transform(latest).select("city", "prediction"))
    return dict(zip(pdf["city"], pdf["prediction"]))


def run(spark, cfg, logger):
    paths = cfg["paths"]
    df = spark.read.parquet(paths["features_data"])
    last_date = df.agg(F.max("date")).first()[0]
    latest = df.filter(F.col("date") == F.lit(last_date)).cache()
    logger.info("Forecasting from the latest data day: %s (%d cities)", last_date, latest.count())

    rows = []
    for h in cfg["targets"]["horizons"]:
        values = {t: predict(f"{paths['models_dir']}/{t}_h{h}", latest) for t in cfg["targets"]["regression"]}
        smog = predict(f"{paths['models_dir']}/smog_level_h{h}", latest)
        for city in sorted(smog):
            level = SMOG_LEVELS[int(smog[city])]
            row = {"city": city, "based_on_data_up_to": last_date,
                   "forecast_date": last_date + timedelta(days=h), "days_ahead": h}
            for t in cfg["targets"]["regression"]:
                low, high = VALID_RANGE[t]
                row[f"predicted_{t}"] = round(min(max(values[t][city], low), high), 1 if t == "aqi" else 2)
            row["predicted_smog_level"] = level
            row["health_advice"] = ADVICE[level]
            rows.append(row)

    out = pd.DataFrame(rows).sort_values(["city", "days_ahead"])
    save_csv(out, cfg, "latest_forecast.csv", logger)
    for _, r in out.iterrows():
        logger.info("FORECAST %-10s %s (+%d): AQI %5.0f | visibility %5.2f km | smog %s",
                    r["city"], r["forecast_date"], r["days_ahead"],
                    r.get("predicted_aqi", float("nan")), r.get("predicted_visibility_km", float("nan")),
                    r["predicted_smog_level"])
    latest.unpersist()
    return out
