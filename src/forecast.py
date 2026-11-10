"""
forecast.py - STEP 5 of the pipeline (PySpark MLlib): the REAL-TIME 7-DAY FORECAST.

Takes the latest observed day (yesterday) of every city, combines it with the weather forecast
for each of the next 7 days, and asks the saved best models for:
    AQI, visibility (km) and smog level, for each day.

Prints a clear table in the terminal (with a short reason for each day) and saves
reports/results/latest_forecast.csv
"""

import os
import sys

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.ml.util import JavaMLReader
from pyspark.sql import functions as F

from src.features import SMOG_LEVELS
from src.utils import save_csv, to_pandas

ADVICE = {
    "Low": "Air acceptable",
    "Moderate": "Sensitive groups limit outdoors",
    "Severe": "Mask outdoors, avoid exercise",
    "Hazardous": "Stay indoors, close schools",
}
COLORS = {"Low": "\033[92m", "Moderate": "\033[93m", "Severe": "\033[38;5;208m", "Hazardous": "\033[91m"}
RESET, BOLD = "\033[0m", "\033[1m"


def load_model(path):
    """Load a saved model fully in Java (avoids a Python-worker crash on Windows)."""
    try:
        return JavaMLReader(PipelineModel).load(path)
    except Exception:
        return PipelineModel.load(path)


def reason(r):
    """Short plain-language reason for the day's forecast, from the forecast weather."""
    why = []
    if r["t_wind_speed_kmh"] < 6:
        why.append("calm wind traps smog")
    elif r["t_wind_speed_kmh"] > 15:
        why.append("strong wind clears air")
    if r["t_rainfall_mm"] >= 2:
        why.append("rain washes air")
    if r["t_temperature_c"] < 15:
        why.append("cold air (inversion)")
    if r["t_is_crop_burning_season"] == 1:
        why.append("crop-burning season")
    if r["t_humidity_pct"] > 80 and r["t_rainfall_mm"] < 2:
        why.append("humid: fog/haze")
    return ", ".join(why[:3]) or "normal conditions"


def print_table(out, base_date):
    use_color = sys.stdout.isatty()
    if use_color and os.name == "nt":
        os.system("")                      # turns on colours in the Windows terminal
    col = (lambda lvl, txt: f"{COLORS[lvl]}{txt}{RESET}") if use_color else (lambda lvl, txt: txt)
    bold = (lambda t: f"{BOLD}{t}{RESET}") if use_color else (lambda t: t)

    line = "=" * 104
    print("\n" + line)
    print(bold(f"  7-DAY AIR QUALITY & SMOG FORECAST   |   real data up to {base_date} (Open-Meteo / CAMS)"))
    print(line)
    for city, g in out.groupby("city", sort=False):
        print("\n" + bold(f"  {city.upper()}"))
        print(f"  {'Date':<11}{'Day':<5}{'AQI':>5}  {'Smog level':<11}{'Visibility':>11}  {'Temp':>6}{'Wind':>9}"
              f"{'Rain':>8}   Why")
        print("  " + "-" * 100)
        for _, r in g.iterrows():
            lvl = r["predicted_smog_level"]
            print(f"  {str(r['forecast_date']):<11}{r['day']:<5}{r['predicted_aqi']:>5.0f}  "
                  f"{col(lvl, f'{lvl:<11}')}{r['predicted_visibility_km']:>8.1f} km  "
                  f"{r['forecast_temp_c']:>4.0f} C{r['forecast_wind_kmh']:>5.0f} km/h{r['forecast_rain_mm']:>5.1f} mm"
                  f"   {r['reason']}")
    print("\n  AQI: 0-150 Low | 151-200 Moderate | 201-300 Severe | 301-500 Hazardous")
    print(line + "\n")


def run(spark, cfg, logger):
    paths = cfg["paths"]
    feats = spark.read.parquet(paths["features_data"])
    last_day = feats.filter(F.col("aqi_target").isNotNull()).agg(F.max("target_date")).first()[0]
    rows = feats.filter((F.col("date") == F.lit(last_day)) & F.col("t_temperature_c").isNotNull()).cache()
    logger.info("Forecasting from the latest observed day %s using the weather forecast for the next days", last_day)

    preds = {}
    for name in ["aqi", "visibility_km", "smog_level"]:
        model = load_model(f"{paths['models_dir']}/{name}")
        preds[name] = to_pandas(model.transform(rows).select("city", "horizon", "prediction"))
    base = to_pandas(rows.select("city", "horizon", "target_date", "aqi", "t_temperature_c", "t_humidity_pct",
                                 "t_wind_speed_kmh", "t_rainfall_mm", "t_is_crop_burning_season"))
    for name, p in preds.items():
        base = base.merge(p.rename(columns={"prediction": f"p_{name}"}), on=["city", "horizon"])

    out = pd.DataFrame({
        "city": base["city"],
        "based_on_data_up_to": str(last_day),
        "forecast_date": base["target_date"].astype(str),
        "day": pd.to_datetime(base["target_date"]).dt.strftime("%a"),
        "days_ahead": base["horizon"].astype(int),
        "predicted_aqi": base["p_aqi"].clip(0, 500).round(0),
        "predicted_visibility_km": base["p_visibility_km"].clip(0.05, 30).round(2),
        "predicted_smog_level": base["p_smog_level"].astype(int).map(dict(enumerate(SMOG_LEVELS))),
        "forecast_temp_c": base["t_temperature_c"].round(1),
        "forecast_humidity_pct": base["t_humidity_pct"].round(0),
        "forecast_wind_kmh": base["t_wind_speed_kmh"].round(1),
        "forecast_rain_mm": base["t_rainfall_mm"].round(1),
        "reason": base.apply(reason, axis=1),
    })
    out["health_advice"] = out["predicted_smog_level"].map(ADVICE)
    order = out.groupby("city")["predicted_aqi"].transform("max")
    out = out.assign(_o=order).sort_values(["_o", "city", "days_ahead"], ascending=[False, True, True]).drop(columns="_o")

    save_csv(out, cfg, "latest_forecast.csv", logger)
    print_table(out, last_day)
    for city, g in out.groupby("city"):
        logger.info("FORECAST %-10s next 7 days AQI: %s", city, " ".join(f"{a:.0f}" for a in g["predicted_aqi"]))
    rows.unpersist()
    return out
