"""
alerts.py - STEP 7 of the pipeline: turn the forecast into ACTION (the problem we solve).

Problem: every winter, smog in Pakistan's cities reaches hazardous levels and people,
schools, traffic police and the environment department react too late.

This step reads the 3-day forecast and automatically writes a SMOG EARLY-WARNING BULLETIN
for each city: the expected level, why it matters, and what each authority should do.

Outputs (reports/alerts/):
  smog_bulletin_<date>.txt  - plain-text bulletin (can be emailed / sent to departments)
  smog_alerts_<date>.csv    - one row per city, machine-readable (can feed SMS or other systems)
Every alert is also written to the pipeline log as a WARNING line.
"""

from pathlib import Path

import pandas as pd

LEVEL_ORDER = ["Low", "Moderate", "Severe", "Hazardous"]

PUBLIC_ADVICE = {
    "Low": "Air is acceptable. No special precautions.",
    "Moderate": "Children, elderly and heart/lung patients should limit time outdoors.",
    "Severe": "Wear an N95 mask outdoors. Avoid outdoor exercise. Keep windows closed.",
    "Hazardous": "Stay indoors. Seek medical help for breathing difficulty or chest pain.",
}
AUTHORITY_ACTIONS = {
    "Low": ["No action needed."],
    "Moderate": ["Health department: issue advisory for sensitive groups.",
                 "Environment department: monitor crop-burning hotspots."],
    "Severe": ["Education department: cancel outdoor school activities and sports.",
               "Environment department: increase patrols against crop-residue burning.",
               "City administration: water sprinkling on main roads to settle dust.",
               "Hospitals: prepare for more respiratory patients."],
    "Hazardous": ["Education department: close schools or move classes online.",
                  "Industries department: stop brick kilns and smoke-emitting factories.",
                  "Environment department: strictly enforce the ban on crop-residue burning.",
                  "Traffic police: restrict heavy vehicles and smoke-emitting vehicles.",
                  "Hospitals: activate smog emergency counters."],
}


def city_alert(rows, low_vis):
    """Build the alert for one city from its 3 forecast rows."""
    rows = rows.sort_values("days_ahead")
    worst = max(rows["predicted_smog_level"], key=LEVEL_ORDER.index)
    worst_row = rows[rows["predicted_smog_level"] == worst].iloc[0]
    first, last = rows.iloc[0], rows.iloc[-1]

    trend = "rising" if last["predicted_aqi"] > first["predicted_aqi"] + 10 else \
        "falling" if last["predicted_aqi"] < first["predicted_aqi"] - 10 else "steady"
    actions = list(AUTHORITY_ACTIONS[worst])
    min_vis = rows["predicted_visibility_km"].min()
    if min_vis < low_vis:
        actions.append(f"Motorway police: low-visibility warning (down to {min_vis:.1f} km), "
                       "reduce speed limits and consider night closures.")
    elif min_vis < 2 * low_vis:
        actions.append("Traffic police: driver advisory to use fog lights and keep distance.")

    return {
        "city": first["city"],
        "alert_level": worst,
        "worst_day": worst_row["forecast_date"],
        "max_aqi": round(rows["predicted_aqi"].max()),
        "min_visibility_km": round(min_vis, 2),
        "trend": trend,
        "daily_outlook": "\n            ".join(f"{r['forecast_date']}: AQI {r['predicted_aqi']:.0f}, {r['predicted_smog_level']}"
                                    for _, r in rows.iterrows()),
        "public_advice": PUBLIC_ADVICE[worst],
        "authority_actions": " ; ".join(actions),
    }


def write_bulletin(alerts, base_date, path, days):
    lines = ["=" * 78,
             "SMOG EARLY-WARNING BULLETIN (automated)",
             f"Based on data up to: {base_date}   |   Outlook: next {days} days",
             "Generated automatically by the PySpark pipeline from real Open-Meteo (CAMS) air-quality and weather data",
             "=" * 78, ""]
    counts = alerts["alert_level"].value_counts()
    lines.append("SUMMARY: " + ", ".join(f"{counts.get(l, 0)} city(ies) {l}" for l in reversed(LEVEL_ORDER)))
    lines.append("")
    for _, a in alerts.iterrows():
        lines += [f"--- {a['city'].upper()} : {a['alert_level'].upper()} (max AQI {a['max_aqi']}, trend {a['trend']}) ---",
                  f"Outlook   : {a['daily_outlook']}",
                  f"Visibility: lowest {a['min_visibility_km']} km",
                  f"Public    : {a['public_advice']}",
                  "Actions   :"]
        lines += [f"  - {x.strip()}" for x in a["authority_actions"].split(";")]
        lines.append("")
    lines.append("AQI guide: 0-150 Low | 151-200 Moderate | 201-300 Severe | 301-500 Hazardous")
    path.write_text("\n".join(lines), encoding="utf-8")


def run(cfg, logger):
    forecast_path = Path(cfg["paths"]["results_dir"]) / "latest_forecast.csv"
    if not forecast_path.exists():
        raise FileNotFoundError("latest_forecast.csv not found. Run the 'forecast' step first.")
    fc = pd.read_csv(forecast_path)
    low_vis = cfg.get("alerts", {}).get("low_visibility_km", 1.0)

    alerts = pd.DataFrame([city_alert(g, low_vis) for _, g in fc.groupby("city")])
    alerts["_rank"] = alerts["alert_level"].map(LEVEL_ORDER.index)
    alerts = alerts.sort_values(["_rank", "max_aqi"], ascending=False).drop(columns="_rank")

    out_dir = Path(cfg["paths"].get("alerts_dir", "reports/alerts"))
    out_dir.mkdir(parents=True, exist_ok=True)
    base_date = fc["based_on_data_up_to"].iloc[0]
    alerts.to_csv(out_dir / f"smog_alerts_{base_date}.csv", index=False)
    write_bulletin(alerts, base_date, out_dir / f"smog_bulletin_{base_date}.txt", fc["days_ahead"].nunique())
    logger.info("Saved bulletin -> %s", out_dir / f"smog_bulletin_{base_date}.txt")

    for _, a in alerts.iterrows():
        log = logger.warning if a["alert_level"] in ("Severe", "Hazardous") else logger.info
        log("ALERT %-10s %-9s max AQI %3d | lowest visibility %.1f km | trend %s",
            a["city"], a["alert_level"].upper(), a["max_aqi"], a["min_visibility_km"], a["trend"])
    return alerts
