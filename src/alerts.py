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


# Pollutant-specific rules: (column, limit, warning, action). Limits follow WHO 24-hour guidelines /
# Pakistan NEQS where available.
POLLUTANT_RULES = [
    ("no2_ugm3", 80, "Nitrogen dioxide high (mostly vehicle exhaust)",
     "Traffic police: divert heavy traffic, stop smoke-emitting vehicles, promote public transport."),
    ("so2_ugm3", 40, "Sulphur dioxide high (factories and brick kilns)",
     "Industries department: inspect and stop brick kilns and factories without emission control."),
    ("pm10", 150, "Coarse dust high (construction, roads)",
     "City administration: stop open construction, cover sand and soil, sprinkle water on roads."),
    ("co_ugm3", 4000, "Carbon monoxide high (vehicles and burning)",
     "Public: ventilate homes, do not leave engines running; traffic police: vehicle emission checks."),
    ("ozone_ugm3", 100, "Ground-level ozone high (sunlight + traffic gases)",
     "Public: avoid outdoor activity between 12:00 and 16:00."),
]


def city_events(city, latest, rows, anomalies, low_vis):
    """Pollutant warnings and disaster signs for one city -> list of event dicts."""
    events = []
    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    if latest is not None:
        for col, limit, warning, action in POLLUTANT_RULES:
            v = latest.get(col)
            if pd.notna(v) and v > limit:
                events.append({"time": now, "city": city, "type": "Pollutant warning", "severity": "High",
                               "message": f"{warning}: {v:.0f} (limit {limit})", "action": action})
        dust, pm10 = latest.get("dust_ugm3"), latest.get("pm10")
        if (pd.notna(dust) and dust > 150) or (pd.notna(pm10) and pm10 > 400):
            events.append({"time": now, "city": city, "type": "Dust storm warning", "severity": "Critical",
                           "message": f"Very high dust in the air (dust {dust:.0f}, PM10 {pm10:.0f} µg/m³)",
                           "action": "Public: stay indoors, close windows; Motorway police: low-visibility measures."})
    hot = rows[rows["forecast_temp_c"] >= 38]
    if len(hot):
        events.append({"time": now, "city": city, "type": "Heatwave warning", "severity": "Critical",
                       "message": f"Very hot days ahead (average up to {hot['forecast_temp_c'].max():.0f} °C) on "
                                  + ", ".join(hot["forecast_date"].astype(str)),
                       "action": "Health department: open heat relief centres; Public: drink water, avoid noon sun."})
    foggy = rows[rows["predicted_visibility_km"] < low_vis]
    if len(foggy):
        events.append({"time": now, "city": city, "type": "Dense smog / fog warning", "severity": "Critical",
                       "message": f"Visibility below {low_vis:.0f} km on " + ", ".join(foggy["forecast_date"].astype(str)),
                       "action": "Motorway police: reduce speed limits, consider night closures."})
    if anomalies is not None and len(anomalies):
        recent = anomalies[(anomalies["city"] == city) &
                           (pd.to_datetime(anomalies["date"]) >= pd.Timestamp.today().normalize() - pd.Timedelta(days=3))]
        for _, a in recent.head(2).iterrows():
            events.append({"time": now, "city": city, "type": "Unusual pollution event", "severity": "High",
                           "message": f"{a['date']}: {a['reason']}",
                           "action": "Environment department: send an inspection team to find the source."})
    return events


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
                  f"Public    : {a['public_advice']}"]
        if a.get("warnings"):
            lines += ["Warnings  :"] + [f"  ! {w.strip()}" for w in str(a["warnings"]).split("|")]
        lines += ["Actions   :"]
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

    # latest observed pollutants per city + recent anomalies -> pollutant warnings and disaster signs
    raw = pd.read_csv(cfg["paths"]["raw_data"])
    past = raw[raw["is_future"] == 0].sort_values("date")
    latest = {c: g.iloc[-1] for c, g in past.groupby("city")}
    an_path = Path(cfg["paths"]["results_dir"]) / "anomalies.csv"
    anomalies = pd.read_csv(an_path) if an_path.exists() else None
    events = []
    for city, g in fc.groupby("city"):
        ev = city_events(city, latest.get(city), g, anomalies, low_vis)
        events += ev
        extra = " ; ".join(e["action"] for e in ev)
        if extra:
            i = alerts.index[alerts["city"] == city][0]
            alerts.at[i, "authority_actions"] = alerts.at[i, "authority_actions"] + " ; " + extra
    alerts["warnings"] = alerts["city"].map(
        lambda c: " | ".join(f"{e['type']}: {e['message']}" for e in events if e["city"] == c))
    alerts["_rank"] = alerts["alert_level"].map(LEVEL_ORDER.index)
    alerts = alerts.sort_values(["_rank", "max_aqi"], ascending=False).drop(columns="_rank")

    out_dir = Path(cfg["paths"].get("alerts_dir", "reports/alerts"))
    out_dir.mkdir(parents=True, exist_ok=True)
    base_date = fc["based_on_data_up_to"].iloc[0]
    alerts.to_csv(out_dir / f"smog_alerts_{base_date}.csv", index=False)
    days = fc["days_ahead"].nunique()
    write_bulletin(alerts, base_date, out_dir / f"smog_bulletin_{base_date}.txt", days)
    write_bulletin(alerts, base_date, out_dir / "latest_bulletin.txt", days)     # always the newest one
    logger.info("Saved bulletin -> %s", out_dir / f"smog_bulletin_{base_date}.txt")

    # Event triggers: one JSON payload per run (can be sent to SMS / e-mail systems) + a running event log
    ev_dir = Path("reports/events")
    ev_dir.mkdir(parents=True, exist_ok=True)
    import json
    (ev_dir / f"events_{pd.Timestamp.now():%Y%m%d_%H%M}.json").write_text(json.dumps(events, indent=2, default=str),
                                                                         encoding="utf-8")
    log_path = Path(cfg["paths"]["results_dir"]) / "event_log.csv"
    ev_df = pd.DataFrame(events, columns=["time", "city", "type", "severity", "message", "action"])
    if log_path.exists():
        ev_df = pd.concat([pd.read_csv(log_path), ev_df])
    ev_df.drop_duplicates(["city", "type", "message"], keep="first").to_csv(log_path, index=False)
    for e in events:
        logger.warning("EVENT %-10s %-26s %s", e["city"], e["type"], e["message"])

    for _, a in alerts.iterrows():
        log = logger.warning if a["alert_level"] in ("Severe", "Hazardous") else logger.info
        log("ALERT %-10s %-9s max AQI %3d | lowest visibility %.1f km | trend %s",
            a["city"], a["alert_level"].upper(), a["max_aqi"], a["min_visibility_km"], a["trend"])
    return alerts
