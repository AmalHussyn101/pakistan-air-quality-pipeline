"""
ingest.py - STEP 1 of the pipeline: get REAL, LIVE data from the internet.

Source: Open-Meteo (free, no API key)
  * Air quality  (CAMS model data): PM2.5, PM10, NO2, SO2, CO, ozone, CO2, dust
        https://air-quality-api.open-meteo.com/v1/air-quality
  * Past weather (historical forecasts): temperature, humidity, wind, rain, visibility
        https://historical-forecast-api.open-meteo.com/v1/forecast
  * Future weather (next 8 days): the same weather variables
        https://api.open-meteo.com/v1/forecast

Hourly values are turned into DAILY values (mean, or sum for rain) in Pakistan time.

The first run downloads everything since start_date (takes 1-3 minutes).
Later runs only download the last few days (incremental update) and the new weather forecast.
If the internet is down, the pipeline continues with the saved (cached) data.

Output: data/raw/live_daily.csv  - one row per city per day
        (past days have pollution + weather; future days have weather forecast only, is_future = 1)
"""

import json
import time
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
PAST_WEATHER_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
FUTURE_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

AIR_VARS = {   # API name -> our column name
    "pm2_5": "pm25", "pm10": "pm10", "nitrogen_dioxide": "no2_ugm3", "sulphur_dioxide": "so2_ugm3",
    "carbon_monoxide": "co_ugm3", "ozone": "ozone_ugm3", "carbon_dioxide": "co2_ppm", "dust": "dust_ugm3",
}
WEATHER_VARS = {
    "temperature_2m": "temperature_c", "relative_humidity_2m": "humidity_pct",
    "wind_speed_10m": "wind_speed_kmh", "precipitation": "rainfall_mm", "visibility": "visibility_m",
}


def fetch_json(url, params, logger, retries=3):
    full = url + "?" + urllib.parse.urlencode(params)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(full, headers={"User-Agent": "pakistan-smog-pipeline/1.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(data.get("reason"))
            return data
        except Exception as e:
            logger.warning("Download failed (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(2 * attempt)
    raise ConnectionError(f"Could not download {url}")


def hourly_to_daily(data, var_map):
    """Hourly JSON -> daily table (mean of each variable, sum for rain)."""
    h = data["hourly"]
    df = pd.DataFrame({"time": h["time"]})
    for api_name, col in var_map.items():
        df[col] = pd.to_numeric(pd.Series(h.get(api_name, [None] * len(df))), errors="coerce")
    df["date"] = df["time"].str[:10]
    agg = {col: "mean" for col in var_map.values()}
    if "rainfall_mm" in agg:
        agg["rainfall_mm"] = "sum"
    daily = df.groupby("date").agg(agg)
    # a day needs at least 18 hourly readings, otherwise it is treated as missing
    counts = df.groupby("date")[list(var_map.values())].count()
    daily = daily.where(counts >= 18)
    return daily.reset_index()


def year_chunks(start, end):
    """Split a long date range into one request per calendar year."""
    chunks, s = [], start
    while s <= end:
        e = min(date(s.year, 12, 31), end)
        chunks.append((s, e))
        s = e + timedelta(days=1)
    return chunks


def download_history(city, lat, lon, start, end, logger):
    air, weather = [], []
    for s, e in year_chunks(start, end):
        common = {"latitude": lat, "longitude": lon, "timezone": "Asia/Karachi",
                  "start_date": s.isoformat(), "end_date": e.isoformat()}
        air.append(hourly_to_daily(fetch_json(AIR_URL, {**common, "hourly": ",".join(AIR_VARS)}, logger), AIR_VARS))
        weather.append(hourly_to_daily(
            fetch_json(PAST_WEATHER_URL, {**common, "hourly": ",".join(WEATHER_VARS)}, logger), WEATHER_VARS))
    df = pd.concat(air).merge(pd.concat(weather), on="date", how="outer")
    df["city"] = city
    return df


def download_future_weather(city, lat, lon, days, logger):
    params = {"latitude": lat, "longitude": lon, "timezone": "Asia/Karachi",
              "hourly": ",".join(WEATHER_VARS), "forecast_days": days}
    df = hourly_to_daily(fetch_json(FUTURE_WEATHER_URL, params, logger), WEATHER_VARS)
    df["city"] = city
    return df


def run(cfg, logger):
    live = cfg["live_data"]
    cache_dir = Path(live["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)
    yesterday = date.today() - timedelta(days=1)
    start = date.fromisoformat(live["start_date"])
    horizon = max(cfg["targets"]["horizons"])

    history_parts, future_parts, failed = [], [], []
    for city, (lat, lon) in live["cities"].items():
        cache = cache_dir / f"{city.lower()}_history.csv"
        old = pd.read_csv(cache) if cache.exists() else None
        try:
            if old is not None and len(old):
                # incremental: re-download the last week (late corrections) up to yesterday
                fetch_from = max(start, date.fromisoformat(old["date"].max()) - timedelta(days=7))
            else:
                fetch_from = start
            new = download_history(city, lat, lon, fetch_from, yesterday, logger)
            hist = pd.concat([old, new]) if old is not None else new
            hist = hist.drop_duplicates(["city", "date"], keep="last").sort_values("date")
            hist.to_csv(cache, index=False)
            logger.info("%-10s history: %d days (downloaded %s to %s)", city, len(hist), fetch_from, yesterday)
        except ConnectionError:
            if old is None:
                raise
            hist = old
            failed.append(city)
            logger.warning("%-10s offline - using saved data up to %s", city, old["date"].max())
        history_parts.append(hist)

        fut_cache = cache_dir / f"{city.lower()}_forecast_weather.csv"
        try:
            fut = download_future_weather(city, lat, lon, horizon + 1, logger)
            fut.to_csv(fut_cache, index=False)
        except ConnectionError:
            if not fut_cache.exists():
                raise
            fut = pd.read_csv(fut_cache)
            logger.warning("%-10s offline - using saved weather forecast", city)
        future_parts.append(fut[fut["date"] > hist["date"].max()])

    history = pd.concat(history_parts).assign(is_future=0)
    future = pd.concat(future_parts).assign(is_future=1)
    daily = pd.concat([history, future], ignore_index=True).sort_values(["city", "date"])
    out = Path(cfg["paths"]["raw_data"])
    out.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out, index=False)

    last = history["date"].max()
    logger.info("Live data ready: %d past days + %d forecast-weather days for %d cities (latest observed day %s)",
                len(history), len(future), len(live["cities"]), last)
    if failed:
        logger.warning("Offline cities (cached data used): %s", ", ".join(failed))
    return daily
