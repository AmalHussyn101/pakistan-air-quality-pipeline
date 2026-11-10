"""
quality.py - STEP 2 of the pipeline: DATA QUALITY CHECKS and DATA SOURCE HEALTH.

Before Spark uses the new data, the pipeline checks it (like Great Expectations / PyDeequ would):
  * Freshness   - is the newest observed day recent?              (FAIL if older than 3 days)
  * Completeness - share of missing values in the last 30 days     (WARN > 10%, FAIL > 50%)
  * Validity    - values outside the allowed ranges in config.yaml  (WARN if any)
  * Frozen data - the same PM2.5 value repeated 3+ days in a row    (WARN: the source may be stuck)

Every city gets a health status:  Operational / Warning / Faulty.
If ALL cities are Faulty, the pipeline stops (bad data must not reach the models).

Outputs: reports/results/data_quality_checks.csv, reports/results/data_source_health.csv
"""

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

KEY_COLUMNS = ["pm25", "pm10", "temperature_c", "humidity_pct", "wind_speed_kmh"]


def longest_repeat(values):
    """Longest run of the same value in a row (used to find frozen data)."""
    best = run = 1
    prev = object()
    for v in values:
        if pd.notna(v) and v == prev:
            run += 1
            best = max(best, run)
        else:
            run = 1
        prev = v
    return best if len(values) else 0


def run(cfg, logger):
    df = pd.read_csv(cfg["paths"]["raw_data"])
    past = df[df["is_future"] == 0].copy()
    past["date"] = pd.to_datetime(past["date"])
    yesterday = pd.Timestamp(date.today() - timedelta(days=1))
    ranges = cfg["cleaning"]["valid_ranges"]

    checks, health = [], []
    for city, g in past.groupby("city"):
        g = g.sort_values("date")
        recent = g[g["date"] > g["date"].max() - pd.Timedelta(days=30)]

        lag = (yesterday - g["date"].max()).days
        fresh = "PASS" if lag <= 1 else "WARN" if lag <= 3 else "FAIL"
        checks.append((city, "Freshness", f"newest day {g['date'].max():%Y-%m-%d} ({max(lag, 0)} days old)",
                       "newest day must be at most 3 days old", fresh))

        miss = recent[KEY_COLUMNS].isna().mean().mean() * 100
        comp = "PASS" if miss <= 10 else "WARN" if miss <= 50 else "FAIL"
        checks.append((city, "Completeness", f"{miss:.1f}% missing (last 30 days)", "at most 10% missing", comp))

        bad = 0
        for col, (lo, hi) in ranges.items():
            if col in recent:
                bad += int(((recent[col] < lo) | (recent[col] > hi)).sum())
        valid = "PASS" if bad == 0 else "WARN"
        checks.append((city, "Validity", f"{bad} values outside allowed ranges",
                       "e.g. 0 <= humidity <= 100, PM2.5 >= 0", valid))

        rep = longest_repeat(recent["pm25"].round(2).tolist())
        frozen = "PASS" if rep < 3 else "WARN"
        checks.append((city, "Frozen data", f"same PM2.5 value {rep} days in a row", "fewer than 3 repeats", frozen))

        results = [fresh, comp, valid, frozen]
        status = "Faulty" if "FAIL" in results else "Warning" if "WARN" in results else "Operational"
        health.append({"city": city, "status": status, "last_update": f"{g['date'].max():%Y-%m-%d}",
                       "missing_pct_30d": round(miss, 1), "out_of_range_30d": bad, "longest_repeat_days": rep,
                       "days_of_history": len(g)})

    out = Path(cfg["paths"]["results_dir"])
    pd.DataFrame(checks, columns=["city", "check", "result_value", "rule", "status"]).to_csv(
        out / "data_quality_checks.csv", index=False)
    hp = pd.DataFrame(health)
    hp.to_csv(out / "data_source_health.csv", index=False)

    for _, h in hp.iterrows():
        log = logger.info if h["status"] == "Operational" else logger.warning
        log("DATA HEALTH %-10s %-11s last update %s | missing %.1f%% | out of range %d | repeat %d",
            h["city"], h["status"].upper(), h["last_update"], h["missing_pct_30d"], h["out_of_range_30d"],
            h["longest_repeat_days"])
    n_pass = sum(1 for c in checks if c[4] == "PASS")
    logger.info("Data quality: %d of %d checks passed", n_pass, len(checks))
    if (hp["status"] == "Faulty").all():
        raise RuntimeError("Data quality gate failed: every city's data is faulty. Models were NOT updated.")
    return hp
