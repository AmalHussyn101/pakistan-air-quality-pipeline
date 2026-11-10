"""
monitor.py - STEP 6 of the pipeline: the pipeline checks ITSELF.

1. Saves every new forecast into reports/results/forecast_history.csv.
2. When the real data for a forecast day arrives (next daily run), compares
   forecast vs reality and saves the error in reports/results/forecast_monitoring.csv.
3. If the recent next-day AQI error is larger than the limit in config.yaml,
   it writes models/RETRAIN_NEEDED so the next run retrains the models automatically.
"""

from pathlib import Path

import pandas as pd


def actual_aqi(results_dir):
    """Real (observed) AQI per city and day, calculated by the clean step."""
    df = pd.read_csv(Path(results_dir) / "observed_aqi.csv")
    return df.rename(columns={"date": "forecast_date", "aqi": "actual_aqi"})


def run(cfg, logger):
    results = Path(cfg["paths"]["results_dir"])
    flag = Path(cfg["paths"]["models_dir"]) / "RETRAIN_NEEDED"
    limit = cfg.get("monitoring", {}).get("max_mae_aqi", 40)

    # 1. Keep a history of every forecast made
    fc = pd.read_csv(results / "latest_forecast.csv")
    hist_path = results / "forecast_history.csv"
    keep = ["city", "based_on_data_up_to", "forecast_date", "days_ahead", "predicted_aqi", "predicted_smog_level"]
    history = pd.concat([pd.read_csv(hist_path), fc[keep]]) if hist_path.exists() else fc[keep]
    history = history.drop_duplicates(["city", "based_on_data_up_to", "days_ahead"], keep="last")
    history.to_csv(hist_path, index=False)
    logger.info("Forecast history: %d forecasts stored", len(history))

    # 2. Compare old forecasts with what really happened
    checked = history.merge(actual_aqi(cfg["paths"]["results_dir"]), on=["city", "forecast_date"], how="inner")
    if checked.empty:
        logger.info("No forecast can be checked yet (the forecast days are still in the future). "
                    "From the next daily run, accuracy will be tracked automatically.")
        return
    checked["abs_error"] = (checked["predicted_aqi"] - checked["actual_aqi"]).abs()
    checked.to_csv(results / "forecast_monitoring.csv", index=False)

    recent = checked[checked["days_ahead"] == 1].sort_values("forecast_date").groupby("city").tail(7)
    mae = recent["abs_error"].mean()
    logger.info("Live accuracy: next-day AQI error over the last checks = %.1f points (limit %s)", mae, limit)

    # 3. Ask for retraining if the forecasts got worse
    if mae > limit:
        flag.write_text(f"Next-day MAE {mae:.1f} > limit {limit}\n", encoding="utf-8")
        logger.warning("Forecast error is above the limit -> models will be RETRAINED on the next run")
