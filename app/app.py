"""
app.py - Smart Environmental Monitoring System (the control room of the pipeline).

Start it with:   start_app.bat      (or: streamlit run app/app.py)

What it shows:
  * LIVE air quality of this hour for 6 cities (downloaded from Open-Meteo when the page opens)
  * the 7-day forecast made by the automated PySpark pipeline, with an expected error range
  * a plain-language reason for the forecast
  * automatic alerts and actions for authorities and the public
  * a what-if simulator (stronger wind / rain) using scenario predictions made by the Spark model
  * forecast accuracy: model vs reality
  * pipeline status, with a button to run the pipeline now

The pipeline itself runs automatically every morning (run_daily.bat + Windows Task Scheduler).
This app only reads its results and the live hourly data.
"""

import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src import utils  # noqa: E402
from src.alerts import city_alert  # noqa: E402
from src.ingest import fetch_json  # noqa: E402

CFG = utils.load_config()
RESULTS = ROOT / CFG["paths"]["results_dir"]
LOGS = ROOT / CFG["paths"]["logs_dir"]
CITIES = CFG["live_data"]["cities"]
WHO_PM25 = 15   # WHO 24-hour guideline (2021), µg/m³

LEVELS = ["Low", "Moderate", "Severe", "Hazardous"]
STYLE = {   # background, text, strong colour
    "Low": ("#EAF3DE", "#27500A", "#3B6D11"),
    "Moderate": ("#FAEEDA", "#633806", "#854F0B"),
    "Severe": ("#FAECE7", "#712B13", "#993C1D"),
    "Hazardous": ("#FCEBEB", "#791F1F", "#A32D2D"),
}
MEANING = {
    "Low": "Air is acceptable for most people.",
    "Moderate": "Unhealthy for sensitive people: children, elderly, and heart or lung patients.",
    "Severe": "Very unhealthy for everyone. Outdoor activity should be cut down.",
    "Hazardous": "Health emergency. Everyone should stay indoors as much as possible.",
}
HEALTH = {
    "Children and elderly": {"Low": "Normal activities", "Moderate": "Short outdoor time only",
                             "Severe": "Stay indoors", "Hazardous": "Stay indoors, use air purifier"},
    "Asthma and heart patients": {"Low": "Keep medicine nearby", "Moderate": "Avoid outdoor exertion",
                                  "Severe": "Stay indoors, follow your doctor's plan",
                                  "Hazardous": "Stay indoors, seek help if breathless"},
    "Outdoor workers": {"Low": "No precautions", "Moderate": "Take breaks, wear a mask",
                        "Severe": "N95 mask, shorter shifts", "Hazardous": "Pause outdoor work"},
    "Drivers": {"Low": "Normal", "Moderate": "Normal", "Severe": "Fog lights, keep distance",
                "Hazardous": "Avoid night and early-morning travel"},
}


def level_of(aqi):
    return "Low" if aqi <= 150 else "Moderate" if aqi <= 200 else "Severe" if aqi <= 300 else "Hazardous"


def html(s):
    st.markdown(" ".join(line.strip() for line in s.splitlines() if line.strip()), unsafe_allow_html=True)


def est_visibility(pm25, humidity):
    """Visibility (km) from PM2.5 and humidity - Koschmieder formula, same as the pipeline."""
    try:
        rh = min(float(humidity), 97.0)
        return max(0.05, min(30.0, 3.912 / (0.02 + 0.004 * float(pm25) * (1 - rh / 100) ** -0.55)))
    except (TypeError, ValueError):
        return None


def badge(level):
    bg, fg, _ = STYLE[level]
    return f'<span class="badge" style="background:{bg};color:{fg}">{level}</span>'


def num(v, d=0):
    """Format a number, or 'n/a' if it is missing."""
    try:
        return "n/a" if v is None or pd.isna(v) else f"{float(v):.{d}f}"
    except (TypeError, ValueError):
        return "n/a"


def read(name):
    p = RESULTS / name
    return pd.read_csv(p) if p.exists() else None


# ---------------------------------------------------------------------------
# Live data of this hour (Open-Meteo), refreshed every 15 minutes
# ---------------------------------------------------------------------------
@st.cache_data(ttl=900, show_spinner=False)
def live_now():
    log = logging.getLogger("app")
    lats = ",".join(str(v[0]) for v in CITIES.values())
    lons = ",".join(str(v[1]) for v in CITIES.values())
    common = {"latitude": lats, "longitude": lons, "timezone": "Asia/Karachi", "past_days": 1, "forecast_days": 1}
    air = fetch_json("https://air-quality-api.open-meteo.com/v1/air-quality",
                     {**common, "hourly": "us_aqi,pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,"
                                          "carbon_monoxide,ozone,carbon_dioxide"}, log, retries=2)
    wx = fetch_json("https://api.open-meteo.com/v1/forecast",
                    {**common, "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,visibility"},
                    log, retries=2)
    air = air if isinstance(air, list) else [air]
    wx = wx if isinstance(wx, list) else [wx]
    now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    rows = []
    for city, a, w in zip(CITIES, air, wx):
        da, dw = pd.DataFrame(a["hourly"]), pd.DataFrame(w["hourly"])
        da = da[(da["time"] <= now) & da["us_aqi"].notna()]
        dw = dw[dw["time"] <= now]
        if da.empty:
            continue
        r, x = da.iloc[-1], (dw.iloc[-1] if not dw.empty else {})
        rows.append({"city": city, "time": r["time"].replace("T", " "), "aqi": r["us_aqi"], "pm25": r["pm2_5"],
                     "pm10": r["pm10"], "no2": r["nitrogen_dioxide"], "so2": r["sulphur_dioxide"],
                     "co": r["carbon_monoxide"], "o3": r["ozone"], "co2": r.get("carbon_dioxide"),
                     "temp": x.get("temperature_2m"), "humidity": x.get("relative_humidity_2m"),
                     "wind": x.get("wind_speed_10m"), "visibility_km": (x.get("visibility") or 0) / 1000 or None})
    return pd.DataFrame(rows)


MAP_PLACES = {   # place: (latitude, longitude, province) - approximate city-centre coordinates
    "Lahore": (31.52, 74.36, "Punjab"), "Faisalabad": (31.45, 73.13, "Punjab"), "Rawalpindi": (33.60, 73.04, "Punjab"),
    "Multan": (30.16, 71.52, "Punjab"), "Gujranwala": (32.16, 74.19, "Punjab"), "Sialkot": (32.49, 74.53, "Punjab"),
    "Sargodha": (32.08, 72.67, "Punjab"), "Bahawalpur": (29.40, 71.68, "Punjab"), "Sheikhupura": (31.71, 73.98, "Punjab"),
    "Gujrat": (32.57, 74.08, "Punjab"), "Sahiwal": (30.66, 73.11, "Punjab"), "Okara": (30.81, 73.45, "Punjab"),
    "Rahim Yar Khan": (28.42, 70.30, "Punjab"), "Dera Ghazi Khan": (30.05, 70.63, "Punjab"), "Jhang": (31.27, 72.32, "Punjab"),
    "Kasur": (31.12, 74.45, "Punjab"), "Chiniot": (31.72, 72.98, "Punjab"), "Jhelum": (32.94, 73.73, "Punjab"),
    "Chakwal": (32.93, 72.86, "Punjab"), "Attock": (33.77, 72.36, "Punjab"), "Mianwali": (32.58, 71.54, "Punjab"),
    "Bhakkar": (31.63, 71.06, "Punjab"), "Layyah": (30.96, 70.94, "Punjab"), "Muzaffargarh": (30.07, 71.19, "Punjab"),
    "Khanewal": (30.30, 71.93, "Punjab"), "Vehari": (30.04, 72.35, "Punjab"), "Bahawalnagar": (29.99, 73.25, "Punjab"),
    "Pakpattan": (30.34, 73.39, "Punjab"), "Toba Tek Singh": (30.97, 72.48, "Punjab"), "Hafizabad": (32.07, 73.69, "Punjab"),
    "Mandi Bahauddin": (32.59, 73.49, "Punjab"), "Narowal": (32.10, 74.87, "Punjab"), "Khushab": (32.30, 72.35, "Punjab"),
    "Rajanpur": (29.10, 70.33, "Punjab"), "Lodhran": (29.53, 71.63, "Punjab"),
    "Islamabad": (33.68, 73.05, "Islamabad"),
    "Karachi": (24.86, 67.00, "Sindh"), "Hyderabad": (25.40, 68.37, "Sindh"), "Sukkur": (27.71, 68.86, "Sindh"),
    "Larkana": (27.56, 68.21, "Sindh"), "Nawabshah": (26.25, 68.41, "Sindh"), "Mirpur Khas": (25.53, 69.01, "Sindh"),
    "Jacobabad": (28.28, 68.44, "Sindh"), "Shikarpur": (27.96, 68.64, "Sindh"), "Khairpur": (27.53, 68.76, "Sindh"),
    "Dadu": (26.73, 67.78, "Sindh"), "Thatta": (24.75, 67.92, "Sindh"), "Badin": (24.66, 68.84, "Sindh"),
    "Sanghar": (26.05, 68.95, "Sindh"), "Mithi": (24.74, 69.80, "Sindh"),
    "Peshawar": (34.02, 71.52, "Khyber Pakhtunkhwa"), "Mardan": (34.20, 72.04, "Khyber Pakhtunkhwa"),
    "Abbottabad": (34.15, 73.21, "Khyber Pakhtunkhwa"), "Kohat": (33.58, 71.44, "Khyber Pakhtunkhwa"),
    "Dera Ismail Khan": (31.83, 70.90, "Khyber Pakhtunkhwa"), "Bannu": (32.99, 70.60, "Khyber Pakhtunkhwa"),
    "Nowshera": (34.02, 71.98, "Khyber Pakhtunkhwa"), "Swabi": (34.12, 72.47, "Khyber Pakhtunkhwa"),
    "Charsadda": (34.15, 71.74, "Khyber Pakhtunkhwa"), "Mingora (Swat)": (34.77, 72.36, "Khyber Pakhtunkhwa"),
    "Mansehra": (34.33, 73.20, "Khyber Pakhtunkhwa"), "Chitral": (35.85, 71.79, "Khyber Pakhtunkhwa"),
    "Parachinar": (33.90, 70.10, "Khyber Pakhtunkhwa"),
    "Quetta": (30.18, 66.98, "Balochistan"), "Gwadar": (25.12, 62.33, "Balochistan"), "Turbat": (26.00, 63.04, "Balochistan"),
    "Khuzdar": (27.81, 66.61, "Balochistan"), "Hub": (25.05, 66.88, "Balochistan"), "Sibi": (29.54, 67.88, "Balochistan"),
    "Zhob": (31.34, 69.45, "Balochistan"), "Loralai": (30.37, 68.60, "Balochistan"), "Chaman": (30.92, 66.45, "Balochistan"),
    "Panjgur": (26.97, 64.09, "Balochistan"),
    "Gilgit": (35.92, 74.31, "Gilgit-Baltistan"), "Skardu": (35.30, 75.63, "Gilgit-Baltistan"),
    "Hunza (Karimabad)": (36.32, 74.66, "Gilgit-Baltistan"),
    "Muzaffarabad": (34.37, 73.47, "Azad Kashmir"), "Mirpur (AJK)": (33.15, 73.75, "Azad Kashmir"),
    "Kotli": (33.52, 73.90, "Azad Kashmir"),
}


@st.cache_data(ttl=900, show_spinner=False)
def live_map():
    """Live air quality AND weather of this hour at every place in MAP_PLACES (two requests to Open-Meteo)."""
    names = list(MAP_PLACES)
    common = {"latitude": ",".join(str(MAP_PLACES[n][0]) for n in names),
              "longitude": ",".join(str(MAP_PLACES[n][1]) for n in names),
              "timezone": "Asia/Karachi", "past_days": 1, "forecast_days": 1}
    log = logging.getLogger("app")
    air = fetch_json("https://air-quality-api.open-meteo.com/v1/air-quality",
                     {**common, "hourly": "us_aqi,pm2_5"}, log, retries=2)
    wx = fetch_json("https://api.open-meteo.com/v1/forecast",
                    {**common, "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m"}, log, retries=2)
    air = air if isinstance(air, list) else [air]
    wx = wx if isinstance(wx, list) else [wx]
    now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    rows = []
    for name, a, w in zip(names, air, wx):
        da = pd.DataFrame(a["hourly"])
        da = da[(da["time"] <= now) & da["us_aqi"].notna()]
        dw = pd.DataFrame(w["hourly"])
        dw = dw[dw["time"] <= now]
        if da.empty:
            continue
        ra, rw = da.iloc[-1], (dw.iloc[-1] if len(dw) else {})
        lat, lon, prov = MAP_PLACES[name]
        rows.append({"place": name, "province": prov, "lat": lat, "lon": lon, "aqi": float(ra["us_aqi"]),
                     "pm25": ra["pm2_5"], "temp": rw.get("temperature_2m"), "humidity": rw.get("relative_humidity_2m"),
                     "wind": rw.get("wind_speed_10m"), "time": ra["time"].replace("T", " ")})
    return pd.DataFrame(rows)


@st.cache_data(ttl=1800, show_spinner=False)
def hourly_forecast(lat, lon):
    """Hour-by-hour forecast for the coming days: weather + CAMS air quality (live from Open-Meteo)."""
    log = logging.getLogger("app")
    common = {"latitude": lat, "longitude": lon, "timezone": "Asia/Karachi"}
    wx = fetch_json("https://api.open-meteo.com/v1/forecast",
                    {**common, "forecast_days": 8, "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,"
                                                           "precipitation,cloud_cover,weather_code,is_day,"
                                                           "apparent_temperature,dew_point_2m"}, log, retries=2)
    df = pd.DataFrame(wx["hourly"])
    aq = None
    for days in (7, 5, 4):
        try:
            aq = fetch_json("https://air-quality-api.open-meteo.com/v1/air-quality",
                            {**common, "forecast_days": days,
                             "hourly": "us_aqi,pm2_5,pm10,carbon_dioxide,nitrogen_dioxide"}, log, retries=1)
            break
        except Exception:
            continue
    if aq is not None:
        df = df.merge(pd.DataFrame(aq["hourly"]), on="time", how="left")
    for c in ["us_aqi", "pm2_5", "pm10", "carbon_dioxide", "nitrogen_dioxide"]:
        if c not in df:
            df[c] = None
    df["date"] = df["time"].str[:10]
    df["hour"] = df["time"].str[11:13].astype(int)
    return df


def condition(row, aqi):
    """Weather type for the animated box: smog, fog, thunder, rain, snow, cloudy, partly, sunny or night."""
    code = int(row.get("weather_code") or 0)
    day = int(row.get("is_day") or 0) == 1
    rain = float(row.get("precipitation") or 0)
    cloud = float(row.get("cloud_cover") or 0)
    if aqi is not None and aqi > 200:
        return "smog", "🌫️", "Smoggy"
    if code in (45, 48):
        return "fog", "🌫️", "Foggy"
    if code >= 95:
        return "thunder", "⛈️", "Thunderstorm"
    if code in range(51, 68) or code in (80, 81, 82) or rain > 0.2:
        return "rain", "🌧️", "Rain"
    if code in range(71, 78) or code in (85, 86):
        return "snow", "❄️", "Snow"
    if code == 3 or cloud > 70:
        return "cloudy", "☁️", "Cloudy"
    if code in (1, 2) or cloud > 30:
        return ("partly", "⛅", "Partly cloudy") if day else ("night", "🌙", "Clear night")
    return ("sunny", "☀️", "Sunny") if day else ("night", "🌙", "Clear night")


# ---------------------------------------------------------------------------
# Page style
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Smart Environmental Monitoring System", page_icon="🌫️", layout="wide")
html("""
<style>
html, body, [class*="css"] { font-size: 17px; }
.block-container{padding-top:3.2rem;max-width:1280px}
.top{display:flex;justify-content:space-between;align-items:flex-end;border-bottom:1px solid #e2e8f0;padding-bottom:12px;margin-bottom:16px}
.top .t{font-size:32px;font-weight:800;color:#0f172a;line-height:1.2}
.top .s{font-size:15px;color:#475569;margin-top:4px}
.top .r{font-size:14px;color:#475569;text-align:right;line-height:1.6}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px}
.sec{font-size:21px;font-weight:700;color:#0f172a;margin:6px 0 8px 0}
.sub{font-size:15px;color:#475569;margin:-4px 0 10px 0}
.strip{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px;margin-bottom:6px}
.chipcity{border-radius:12px;padding:10px;text-align:center;font-size:16px;font-weight:600}
.chipcity b{display:block;font-size:30px;font-weight:800}
.chipcity span{display:block;font-size:13px;font-weight:600}
.card{background:#fff;border:1px solid #e2e8f0;border-radius:16px;padding:18px 20px;height:100%}
.lbl{color:#475569;font-size:15px}
.big{font-size:64px;font-weight:800;line-height:1.05}
.badge{display:inline-block;padding:4px 12px;border-radius:8px;font-size:15px;font-weight:700}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:6px 16px;margin-top:12px;font-size:15.5px;color:#1e293b}
.days{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:8px;text-align:center}
.day{border-radius:12px;padding:10px 4px;font-size:14px;line-height:1.35}
.day .d{font-size:16px;font-weight:800}
.day .aqi{font-size:28px;font-weight:800;margin-top:4px}
.day .k{font-size:12px;opacity:.85}
.day .v{font-size:14.5px;font-weight:700}
.why{margin-top:12px;font-size:16px;color:#1e293b;background:#f1f5f9;border-radius:12px;padding:12px 14px;line-height:1.55}
.howto{margin-top:10px;font-size:15px;color:#334155;line-height:1.6}
.act{font-size:16px;color:#1e293b;line-height:1.8}
.small{color:#475569;font-size:14px}
table.hg td{padding:8px 10px;border-bottom:1px solid #f1f5f9;font-size:16px}
.hours{display:grid;grid-template-columns:repeat(8,minmax(0,1fr));gap:10px}
.wx{position:relative;overflow:hidden;border-radius:16px;padding:12px 10px;min-height:290px;color:#0f172a;border:1px solid rgba(15,23,42,.08)}
.wx .in{position:relative;z-index:2}
.wx .tm{font-size:17px;font-weight:800}
.wx .ic{font-size:44px;display:inline-block;margin:4px 0}
.wx .cd{font-size:13.5px;font-weight:700;margin-bottom:6px}
.wx .tp{font-size:30px;font-weight:800;line-height:1}
.wx .ln{font-size:13.5px;margin-top:5px;line-height:1.35}
.wx .pill{display:inline-block;padding:2px 8px;border-radius:7px;font-weight:800;font-size:14px}
.wx.sunny{background:linear-gradient(170deg,#fff3c4 0%,#ffe08a 35%,#cfe8ff 100%)}
.wx.partly{background:linear-gradient(170deg,#fff6d8 0%,#dbeafe 60%,#cbd5e1 100%)}
.wx.cloudy{background:linear-gradient(170deg,#e5e7eb 0%,#cbd5e1 100%)}
.wx.rain,.wx.thunder{background:linear-gradient(170deg,#cbd5e1 0%,#94a3b8 100%)}
.wx.snow{background:linear-gradient(170deg,#f8fafc 0%,#dbeafe 100%)}
.wx.fog{background:linear-gradient(170deg,#eef2f7 0%,#d7dde5 100%)}
.wx.smog{background:linear-gradient(170deg,#e8dfcf 0%,#c9bba3 55%,#a99b84 100%)}
.wx.night{background:linear-gradient(170deg,#1e293b 0%,#334155 100%);color:#f1f5f9}
.wx.sunny .ic{animation:spin 14s linear infinite;filter:drop-shadow(0 0 8px rgba(255,190,0,.8))}
.wx.night .ic{animation:glow 3s ease-in-out infinite alternate}
.wx.partly .ic,.wx.cloudy .ic{animation:drift 5s ease-in-out infinite alternate}
.wx.thunder{animation:flash 5s infinite}
.drop{position:absolute;top:-20px;width:2px;height:14px;border-radius:2px;background:rgba(30,64,175,.55);animation:fall .9s linear infinite;z-index:1}
.flake{position:absolute;top:-10px;font-size:12px;color:#94a3b8;animation:fall 4s linear infinite;z-index:1}
.haze{position:absolute;left:-60%;width:220%;height:60px;border-radius:50%;background:radial-gradient(ellipse at center,rgba(110,98,80,.42) 0%,rgba(110,98,80,0) 70%);animation:hazemove 10s ease-in-out infinite alternate;z-index:1}
.wx.fog .haze{background:radial-gradient(ellipse at center,rgba(255,255,255,.75) 0%,rgba(255,255,255,0) 70%)}
.cloudbg{position:absolute;font-size:46px;opacity:.35;animation:float 14s linear infinite;z-index:1}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes glow{from{filter:drop-shadow(0 0 2px #fde68a)}to{filter:drop-shadow(0 0 10px #fde68a)}}
@keyframes drift{from{transform:translateX(-7px)}to{transform:translateX(7px)}}
@keyframes fall{to{transform:translateY(330px)}}
@keyframes hazemove{from{transform:translateX(-18%)}to{transform:translateX(18%)}}
@keyframes float{from{transform:translateX(-60px)}to{transform:translateX(200px)}}
@keyframes flash{0%,92%,100%{filter:none}94%{filter:brightness(1.6)}96%{filter:none}97%{filter:brightness(1.4)}}
</style>
""")

forecast = read("latest_forecast.csv")
if forecast is None:
    st.warning("No forecast yet. Run the pipeline once: open a terminal and run `python run_pipeline.py --quick`.")
    st.stop()

logs = sorted(LOGS.glob("pipeline_*.log")) if LOGS.exists() else []
last_run = datetime.fromtimestamp(logs[-1].stat().st_mtime).strftime("%d %b, %I:%M %p") if logs else "never"
try:
    live = live_now()
    live_ok = not live.empty
except Exception:
    live, live_ok = pd.DataFrame(), False

# ---------------------------------------------------------------------------
# Night mode switch - placed in the top bar, next to "Deploy" and the ⋮ menu.
# Only the page background turns dark; every box and card keeps its light-mode colours.
# ---------------------------------------------------------------------------
html("""
<style>
.st-key-night_mode { position:fixed; top:12px; right:150px; z-index:1000002; width:auto !important; }
.st-key-night_mode label p { font-size:14px !important; }
</style>
""")
dark = st.toggle("🌙 Night mode", key="night_mode")
if dark:
    html("""
<style>
.stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] { background:#000000 !important; }
.top { border-bottom-color:#334155 !important; }
.top .t, .sec { color:#f8fafc !important; }
.top .s, .top .r, .sub, .small, .howto, .lbl { color:#cbd5e1 !important; }
.card .sec, .card .t { color:#0f172a !important; }
.card .sub, .card .small, .card .howto, .card .lbl { color:#475569 !important; }
/* text written directly on the black page -> light */
.stApp [data-testid="stMarkdownContainer"] p, .stApp [data-testid="stWidgetLabel"] p,
[data-testid="stRadio"] label p, [data-baseweb="tab-list"] button p, .st-key-night_mode label p,
[data-testid="stCaptionContainer"] { color:#e2e8f0 !important; }
[data-baseweb="tab-list"] button[aria-selected="true"] p { color:#ffffff !important; }
/* framed boxes and expanders keep their light-mode look */
.st-key-box_accuracy, [data-testid="stExpander"] details { background:#ffffff !important; border-radius:12px; }
.st-key-box_accuracy [data-testid="stMarkdownContainer"] p, .st-key-box_accuracy [data-testid="stCaptionContainer"],
.st-key-box_accuracy .sec, .st-key-box_accuracy .sub,
[data-testid="stExpander"] [data-testid="stMarkdownContainer"] p, [data-testid="stExpander"] summary p,
[data-testid="stExpander"] [data-testid="stWidgetLabel"] p, [data-testid="stExpander"] .sec,
[data-testid="stExpander"] .sub, [data-testid="stExpander"] .small { color:#0f172a !important; }
</style>
""")

html(f"""
<div class="top">
  <div><div class="t">Smart Environmental Monitoring System</div>
  <div class="s">Air quality and smog early warning for Pakistan · PySpark machine-learning pipeline · real Open-Meteo / CAMS data</div></div>
  <div class="s" style="text-align:right"><span class="dot" style="background:{'#16a34a' if live_ok else '#94a3b8'}"></span>
  {'Live data · ' + str(live['time'].max()) if live_ok else 'Live data offline · showing pipeline data'}<br>
  Pipeline last run: {last_run} · runs automatically every day at 7:00 AM</div>
</div>
""")

# ---------------------------------------------------------------------------
# City strip (live AQI now, or the first forecast day if offline)
# ---------------------------------------------------------------------------
strip_vals = {}
for city in CITIES:
    if live_ok and city in set(live["city"]):
        strip_vals[city] = float(live.loc[live["city"] == city, "aqi"].iloc[0])
    else:
        f = forecast[(forecast["city"] == city) & (forecast["days_ahead"] == 1)]
        strip_vals[city] = float(f["predicted_aqi"].iloc[0]) if len(f) else 0
cells = ""
for city, v in strip_vals.items():
    bg, fg, _ = STYLE[level_of(v)]
    cells += f'<div class="chipcity" style="background:{bg};color:{fg}">{city}<b>{v:.0f}</b><span>{level_of(v)}</span></div>'
html(f'''<div class="sec">{"Air quality right now in six cities" if live_ok else "Tomorrow's air quality in six cities"}</div>
<div class="sub">Number = <b>Air Quality Index (AQI)</b> on the US-EPA scale, 0 to 500. <i>Higher is worse.</i>
Colour = smog level. Pick a city below to see its details.</div>
<div class="strip">{cells}</div>''')

city = st.radio("City", list(CITIES), horizontal=True, label_visibility="collapsed")
fc = forecast[forecast["city"] == city].sort_values("days_ahead").reset_index(drop=True)

# expected error per horizon (from the test period, best AQI model)
err = {}
reg, best = read("regression_metrics.csv"), read("best_models.csv")
if reg is not None and best is not None and "aqi" in set(best["target"]):
    bm = best.loc[best["target"] == "aqi", "best_model"].iloc[0]
    e = reg[(reg["target"] == "aqi") & (reg["model"] == bm)]
    err = dict(zip(e["horizon_days"], e["mae"]))

st.write("")
tab1, tab2, tab3 = st.tabs(["🌤️  Live & Forecast", "⚠️  Alerts & Safety", "📊  Model & Pipeline"])

with tab1:
    left, right = st.columns([1, 1.7])

    # ---------------------------------------------------------------------------
    # NOW card
    # ---------------------------------------------------------------------------
    with left:
        if live_ok and city in set(live["city"]):
            n = live[live["city"] == city].iloc[0]
            lv = level_of(n["aqi"])
            vis = f"{num(est_visibility(n['pm25'], n['humidity']), 1)} km"
            co2 = f"{num(n['co2'])} ppm"
            html(f"""
            <div class="card">
              <div class="sec" style="margin-top:0">{city} right now</div>
              <div class="lbl">Live reading for {n['time']} · Air Quality Index</div>
              <div class="big" style="color:{STYLE[lv][2]}">{n['aqi']:.0f}</div>
              {badge(lv)} <span class="small"><i>smog level</i></span>
              <div style="margin-top:10px;font-size:14px;color:#334155">{MEANING[lv]}
              PM2.5 is <b>{num(n['pm25'] / WHO_PM25)}×</b> the WHO safe limit.</div>
              <div class="grid2">
                <span>PM2.5: <b>{num(n['pm25'])}</b> µg/m³</span><span>PM10: <b>{num(n['pm10'])}</b> µg/m³</span>
                <span>NO₂: <b>{num(n['no2'])}</b> µg/m³</span><span>SO₂: <b>{num(n['so2'])}</b> µg/m³</span>
                <span>CO: <b>{num(n['co'])}</b> µg/m³</span><span>Ozone: <b>{num(n['o3'])}</b> µg/m³</span>
                <span>CO₂: <b>{co2}</b></span><span>Visibility: <b>{vis}</b></span>
                <span>Temperature: <b>{num(n['temp'])} °C</b></span><span>Wind: <b>{num(n['wind'])} km/h</b></span>
              </div>
              <div class="small" style="margin-top:10px"><i>µg/m³ = micrograms of pollutant in one cubic metre of air.
              Visibility = how far you can see through the smog.</i></div>
            </div>""")
        else:
            f1 = fc.iloc[0]
            lv = f1["predicted_smog_level"]
            html(f"""
            <div class="card">
              <div class="lbl">{city} · live data unavailable, next-day forecast shown</div>
              <div class="big" style="color:{STYLE[lv][2]}">{f1['predicted_aqi']:.0f}</div>{badge(lv)}
              <div style="margin-top:10px;font-size:14px;color:#334155">{MEANING[lv]}</div>
            </div>""")

    # ---------------------------------------------------------------------------
    # 7-DAY forecast card
    # ---------------------------------------------------------------------------
    def why_sentence(rows):
        parts = []
        calm = [r["day"] for _, r in rows.iterrows() if "calm wind" in str(r["reason"])]
        rain = [r["day"] for _, r in rows.iterrows() if "rain" in str(r["reason"])]
        windy = [r["day"] for _, r in rows.iterrows() if "strong wind" in str(r["reason"])]
        cold = [r["day"] for _, r in rows.iterrows() if "cold" in str(r["reason"])]
        if calm:
            parts.append(f"calm wind on {', '.join(calm)} traps smog")
        if cold:
            parts.append(f"cold air on {', '.join(cold)} keeps pollution near the ground")
        if rain:
            parts.append(f"rain on {', '.join(rain)} washes the air")
        if windy:
            parts.append(f"strong wind on {', '.join(windy)} clears pollution")
        trend = rows["predicted_aqi"].iloc[-1] - rows["predicted_aqi"].iloc[0]
        t = "Air quality is expected to get worse over the week" if trend > 20 else \
            "Air quality is expected to improve over the week" if trend < -20 else "Air quality stays at a similar level"
        return t + (": " + "; ".join(parts) + "." if parts else ".")


    with right:
        days = ""
        for _, r in fc.iterrows():
            bg, fg, _ = STYLE[r["predicted_smog_level"]]
            e = err.get(r["days_ahead"])
            lo, hi = (max(0, r["predicted_aqi"] - e), min(500, r["predicted_aqi"] + e)) if e is not None else (None, None)
            rng = f'<div class="k">likely range</div><div class="v">{lo:.0f}–{hi:.0f}</div>' if e is not None else ""
            days += (f'<div class="day" style="background:{bg};color:{fg}">'
                     f'<div class="d">{r["day"]}</div><div class="k">{pd.to_datetime(r["forecast_date"]):%d %b}</div>'
                     f'<div class="k" style="margin-top:6px">AQI</div><div class="aqi">{r["predicted_aqi"]:.0f}</div>'
                     f'{rng}'
                     f'<div class="k" style="margin-top:6px">smog</div><div class="v">{r["predicted_smog_level"]}</div>'
                     f'<div class="k" style="margin-top:6px">visibility</div><div class="v">{r["predicted_visibility_km"]:.1f} km</div></div>')
        e1 = err.get(1)
        html(f"""
        <div class="card">
          <div class="sec" style="margin-top:0">7-day forecast for {city}</div>
          <div class="days">{days}</div>
          <div class="why"><b>Why?</b> {why_sentence(fc)}</div>
          <div class="howto"><b>How to read a day:</b>
          <b>AQI</b> is the forecast air quality.
          <b>Likely range</b> is where the real AQI will most probably fall: in past tests the model was off by about
          {f"±{e1:.0f}" if e1 is not None else "a few"} points for tomorrow, and a little more for later days, so the range gets wider.
          <b>Smog</b> is the expected smog level, and <b>visibility</b> is how far you will be able to see.</div>
        </div>""")

    # ---------------------------------------------------------------------------
    # HOUR-BY-HOUR view of a chosen day
    # ---------------------------------------------------------------------------
    st.write("")
    html(f'''<div class="sec">⏰ Hour by hour</div>
    <div class="sub">Choose a day to see how the weather and air in {city} change through the day (every 3 hours).</div>''')
    day_opts = fc["forecast_date"].astype(str).tolist()
    chosen = st.radio("Day", day_opts, horizontal=True, label_visibility="collapsed",
                      format_func=lambda d: pd.to_datetime(d).strftime("%a %d %b"))
    try:
        hf = hourly_forecast(*CITIES[city])
        hd = hf[(hf["date"] == chosen)]
    except Exception:
        hd = pd.DataFrame()
    if hd.empty:
        st.info("Hourly forecast is not available right now (no internet connection or this day is too far ahead).")
    else:
        day_aqi = float(fc.loc[fc["forecast_date"].astype(str) == chosen, "predicted_aqi"].iloc[0])
        cams_mean = pd.to_numeric(hd["us_aqi"], errors="coerce").mean()
        factor = day_aqi / cams_mean if pd.notna(cams_mean) and cams_mean > 0 else None
        boxes = ""
        for _, r in hd[hd["hour"] % 3 == 0].iterrows():
            aqi = None
            if factor is not None and pd.notna(r["us_aqi"]):
                aqi = max(0.0, min(500.0, float(r["us_aqi"]) * factor))
            kind, icon, label = condition(r, aqi)
            extra = ""
            if kind in ("rain", "thunder"):
                extra = "".join(f'<span class="drop" style="left:{x}%;animation-delay:{d}s"></span>'
                                for x, d in [(8, 0), (22, .3), (36, .6), (50, .15), (64, .45), (78, .75), (90, .2)])
            elif kind == "snow":
                extra = "".join(f'<span class="flake" style="left:{x}%;animation-delay:{d}s">❄</span>'
                                for x, d in [(10, 0), (35, 1.2), (60, .6), (85, 2)])
            elif kind in ("smog", "fog"):
                extra = ('<span class="haze" style="top:25%"></span><span class="haze" style="top:55%;animation-delay:-4s">'
                         '</span><span class="haze" style="top:80%;animation-delay:-7s"></span>')
            elif kind in ("cloudy", "partly"):
                extra = '<span class="cloudbg" style="top:55%">☁️</span>'
            aqi_html = (f'<span class="pill" style="background:{STYLE[level_of(aqi)][0]};color:{STYLE[level_of(aqi)][1]}">'
                        f'AQI {aqi:.0f}</span> <span style="font-size:12px">{level_of(aqi)}</span>') if aqi is not None else "AQI n/a"
            boxes += (f'<div class="wx {kind}">{extra}<div class="in">'
                      f'<div class="tm">{r["hour"]:02d}:00</div><div class="ic">{icon}</div><div class="cd">{label}</div>'
                      f'<div class="tp">{num(r["temperature_2m"])}°C</div>'
                  f'<div class="ln">Feels like <b>{num(r.get("apparent_temperature"))}°C</b> · dew point {num(r.get("dew_point_2m"))}°C</div>'
                      f'<div class="ln">{aqi_html}</div>'
                      f'<div class="ln">PM2.5 <b>{num(r["pm2_5"])}</b> µg/m³</div>'
                      f'<div class="ln">CO₂ <b>{num(r["carbon_dioxide"])}</b> ppm</div>'
                      f'<div class="ln">💧 Humidity <b>{num(r["relative_humidity_2m"])}%</b></div>'
                      f'<div class="ln">💨 Wind <b>{num(r["wind_speed_10m"])}</b> km/h</div>'
                      f'<div class="ln">☔ Rain <b>{num(r["precipitation"], 1)}</b> mm</div></div></div>')
        html(f'<div class="hours">{boxes}</div>')
        html(f'''<div class="howto"><b>How to read:</b> each box is one time of the day. <b>Temperature, humidity, wind
        and rain</b> come from the live weather forecast. <b>AQI</b> is our model's forecast for the day
        ({day_aqi:.0f}), spread over the hours using the hourly pattern of the CAMS air-quality forecast. Smog is usually
        worst at night and early morning, when the air is cold and calm. <b>PM2.5 and CO₂</b> are CAMS hourly values.
        <i>Far-ahead days may show "n/a" for air quality, because the CAMS forecast covers fewer days than the weather forecast.</i></div>''')



    # ---------------------------------------------------------------------------
    # PAKISTAN HEATMAP (live AQI at many places)
    # ---------------------------------------------------------------------------
    st.write("")
    html(f"""<div class="sec">🗺️ Pakistan air-quality map (live)</div>
    <div class="sub">Live air quality at <b>{len(MAP_PLACES)} places</b> in every province. Each dot is one place and its
    <b>colour</b> is the smog level. <b>Move the cursor onto a dot</b> to see its AQI, smog level, PM2.5, temperature,
    humidity, wind and the time of the reading. Use + / - or the mouse wheel to zoom.</div>""")
    try:
        gm = live_map()
        if gm.empty:
            raise ValueError
        import pydeck as pdk
        hexrgb = lambda h: [int(h[i:i + 2], 16) for i in (1, 3, 5)]
        gm["level"] = gm["aqi"].map(level_of)
        gm["rgb"] = gm["level"].map(lambda l: hexrgb(STYLE[l][2]) + [230])
        for c, d in [("aqi", 0), ("pm25", 0), ("temp", 0), ("humidity", 0), ("wind", 0)]:
            gm[f"{c}_t"] = gm[c].map(lambda v, d=d: num(v, d))
        deck = pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(latitude=30.0, longitude=69.3, zoom=4.6, pitch=0),
            layers=[pdk.Layer("ScatterplotLayer", data=gm, get_position=["lon", "lat"], get_fill_color="rgb",
                              get_radius=9000, radius_min_pixels=6, radius_max_pixels=12, stroked=True,
                              get_line_color=[255, 255, 255], line_width_min_pixels=1.5, pickable=True,
                              auto_highlight=True)],
            tooltip={"html": "<div style='font-size:14px;line-height:1.5'><b style='font-size:16px'>{place}</b>"
                             "<br/><i>{province}</i><hr style='margin:4px 0'/>"
                             "AQI: <b>{aqi_t}</b> ({level})<br/>PM2.5: <b>{pm25_t}</b> µg/m³<br/>"
                             "Temperature: <b>{temp_t} °C</b><br/>Humidity: <b>{humidity_t}%</b><br/>"
                             "Wind: <b>{wind_t} km/h</b><br/><span style='opacity:.8'>Reading: {time}</span></div>",
                     "style": {"backgroundColor": "#0f172a", "color": "white", "borderRadius": "10px", "padding": "10px"}},
        )
        st.pydeck_chart(deck, height=560)
        legend = " ".join(f'<span class="badge" style="background:{STYLE[l][0]};color:{STYLE[l][1]}">'
                          f'● {l} (AQI {rng})</span>' for l, rng in zip(LEVELS, ["0-150", "151-200", "201-300", "301-500"]))
        html(f'<div style="margin-top:6px">{legend}</div>')

        st.write("")
        html("""<div class="sec" style="font-size:19px">📋 All places, from most to least polluted</div>
        <div class="sub">Choose a province to filter. Click a column name to sort.</div>""")
        provs = ["All provinces"] + sorted(gm["province"].unique())
        pick = st.selectbox("Province", provs, label_visibility="collapsed")
        tb = gm if pick == "All provinces" else gm[gm["province"] == pick]
        tb = tb.sort_values("aqi", ascending=False)
        st.dataframe(
            tb[["place", "province", "aqi", "level", "pm25", "temp", "humidity", "wind", "time"]].rename(columns={
                "place": "Place", "province": "Province", "aqi": "AQI", "level": "Smog level", "pm25": "PM2.5 (µg/m³)",
                "temp": "Temperature (°C)", "humidity": "Humidity (%)", "wind": "Wind (km/h)", "time": "Reading time"}),
            hide_index=True, height=420,
            column_config={"AQI": st.column_config.ProgressColumn("AQI (0-500)", min_value=0, max_value=500, format="%d"),
                           "PM2.5 (µg/m³)": st.column_config.NumberColumn(format="%d"),
                           "Temperature (°C)": st.column_config.NumberColumn(format="%.0f"),
                           "Humidity (%)": st.column_config.NumberColumn(format="%.0f"),
                           "Wind (km/h)": st.column_config.NumberColumn(format="%.0f")})
        worst, best_p = tb.iloc[0], tb.iloc[-1]
        html(f"""<div class="why"><b>Summary{'' if pick == 'All provinces' else ' for ' + pick}:</b> most polluted is
        <b>{worst['place']}</b> (AQI {worst['aqi']:.0f}, {worst['level']}); cleanest is <b>{best_p['place']}</b>
        (AQI {best_p['aqi']:.0f}). {int((tb['aqi'] > 200).sum())} of {len(tb)} places have Severe or Hazardous air right now.</div>""")
    except Exception:
        st.info("The live map needs an internet connection.")


with tab2:
    st.write("")
    a_col, h_col = st.columns(2)

    # ---------------------------------------------------------------------------
    # ALERTS card
    # ---------------------------------------------------------------------------
    with a_col:
        al = city_alert(fc, CFG.get("alerts", {}).get("low_visibility_km", 1.0))
        acts = "".join(f"• {x.strip()}<br>" for x in al["authority_actions"].split(";"))
        html(f"""
        <div class="card">
          <div class="sec" style="margin-top:0">⚠️ Automatic alert: {badge(al['alert_level'])}</div>
          <div class="sub">Worst day: <b>{pd.to_datetime(al['worst_day']):%A %d %B}</b> · AQI trend this week: <b>{al['trend']}</b>.
          <i>Made automatically from the forecast.</i></div>
          <div class="act"><b>For the public:</b> {al['public_advice']}<br><b>For the authorities:</b><br>{acts}</div>
        </div>""")

    # ---------------------------------------------------------------------------
    # HEALTH guide
    # ---------------------------------------------------------------------------
    with h_col:
        worst = al["alert_level"]
        rows = "".join(f"<tr><td style='color:#475569'>{g}</td><td><b>{v[worst]}</b></td></tr>" for g, v in HEALTH.items())
        html(f"""<div class="card"><div class="sec" style="margin-top:0">🩺 Health guide {badge(worst)}</div>
        <div class="sub">What each group should do on the worst day of the week.</div>
        <table class="hg" style="width:100%">{rows}</table></div>""")


    st.write("")
    html("""<div class="sec">🚨 Pollutant warnings and disaster signs</div>
    <div class="sub">Checked automatically on every pipeline run: each pollutant against its health limit, plus dust storms,
    heatwaves, dense smog/fog and unusual pollution events. <i>Each warning comes with the action that solves it.</i></div>""")
    ev = read("event_log.csv")
    if ev is not None and len(ev[ev["city"] == city]):
        evc = ev[ev["city"] == city].sort_values("time", ascending=False).head(12)
        icons = {"Pollutant warning": "🧪", "Dust storm warning": "🌪️", "Heatwave warning": "🔥",
                 "Dense smog / fog warning": "🌫️", "Unusual pollution event": "📈"}
        cards = "".join(f"""<div class="card" style="margin-bottom:8px;border-left:6px solid
            {'#A32D2D' if e['severity'] == 'Critical' else '#993C1D'};border-radius:0 16px 16px 0">
            <b>{icons.get(e['type'], '⚠️')} {e['type']}</b> <span class="small">· {e['time']}</span><br>
            {e['message']}<br><span style="color:#185FA5"><b>Action:</b> {e['action']}</span></div>"""
                        for _, e in evc.iterrows())
        html(cards)
    else:
        st.success(f"No pollutant warnings or disaster signs for {city} right now.")

    st.write("")
    html("""<div class="sec">🔍 Unusual pollution days (anomaly detection)</div>
    <div class="sub">Found by <b>KMeans clustering</b> (Spark MLlib): days whose mix of pollutants and weather is far from
    every normal pattern, plus <b>sudden spikes</b> (PM2.5 more than double its 7-day average). These can point to fires,
    chemical emissions or dust storms.</div>""")
    an = read("anomalies.csv")
    if an is not None and len(an[an["city"] == city]):
        a = an[an["city"] == city].head(10)
        st.dataframe(a[["date", "type", "aqi", "pm25", "reason"]].rename(columns={
            "date": "Date", "type": "Type", "aqi": "AQI", "pm25": "PM2.5 (µg/m³)", "reason": "What was unusual"}),
            hide_index=True)
    else:
        st.caption("No unusual days found for this city.")

    bulletin = ROOT / CFG["paths"]["alerts_dir"] / "latest_bulletin.txt"
    if bulletin.exists():
        st.download_button("⬇ Download today's smog early-warning bulletin", bulletin.read_bytes(),
                           file_name="smog_bulletin.txt")


with tab3:
    st.write("")
    t_col, s_col = st.columns(2)
    # ---------------------------------------------------------------------------
    # ACCURACY tracker
    # ---------------------------------------------------------------------------
    with t_col:
        with st.container(border=True, key="box_accuracy"):
            html("""<div class="sec">🎯 How accurate is the forecast?</div>
            <div class="sub">The models were tested on recent months they had never seen. <i>The closer the two lines,
            the better the forecast.</i></div>""")
            if best is not None:
                b = best.set_index("target")
                parts = []
                if "aqi" in b.index:
                    parts.append(f"Next-day AQI error: about **±{err.get(1, b.loc['aqi', 'mae']):.0f}** points "
                                 f"(best model: {b.loc['aqi', 'best_model']}, "
                                 f"{b.loc['aqi', 'error_reduction_vs_baseline_pct']:.0f}% better than a simple guess).")
                if "smog_level" in b.index:
                    parts.append(f"Smog level correct on **{b.loc['smog_level', 'accuracy'] * 100:.0f}%** of test days.")
                st.markdown(" ".join(parts))
            mon = read("forecast_monitoring.csv")
            tp = read("test_predictions.csv")
            if mon is not None and len(mon[mon["city"] == city]):
                m = mon[(mon["city"] == city) & (mon["days_ahead"] == 1)].tail(30)
                st.caption("Live check: our next-day forecasts vs what really happened")
                st.line_chart(m.set_index("forecast_date")[["actual_aqi", "predicted_aqi"]]
                              .rename(columns={"actual_aqi": "Real AQI", "predicted_aqi": "Forecast"}), height=180)
            elif tp is not None:
                t = tp[(tp["city"] == city) & (tp["target"] == "aqi") & (tp["horizon_days"] == 1)].tail(60)
                st.caption("Test period: next-day forecast vs real AQI (live tracking starts after the next daily run)")
                st.line_chart(t.set_index("date")[["actual", "prediction"]]
                              .rename(columns={"actual": "Real AQI", "prediction": "Forecast"}), height=180)

    # ---------------------------------------------------------------------------
    # WHAT-IF simulator
    # ---------------------------------------------------------------------------
    with s_col:
        sc = read("what_if_scenarios.csv")
        with st.expander("🎛️ Scenario planner (optional) - what if the weather were different?"):
            html("""<div class="sub"><b>This is not the forecast.</b> The real forecast above already uses the real
            weather forecast (wind, rain, temperature). Here you can <i>imagine</i> different weather - for example
            "what if it rains 10 mm on Thursday?" or "what if the wind is 8 km/h stronger?" - and the trained model shows
            how the AQI would change. Authorities can use this to judge actions such as artificial rain or water
            spraying.</div>""")
            if sc is None:
                st.caption("Run the pipeline once to create the simulator data.")
            else:
                c1, c2, c3 = st.columns(3)
                day = c1.selectbox("1. Which day?", fc["days_ahead"].tolist(),
                                   format_func=lambda d: pd.to_datetime(fc.loc[fc["days_ahead"] == d, "forecast_date"]
                                                                        .iloc[0]).strftime("%A %d %b"))
                wind = c2.select_slider("2. Imagine the wind is stronger by (km/h)", options=[-4, 0, 4, 8, 12], value=0,
                                        help="0 = wind exactly as forecast. Negative = calmer than forecast.")
                rain_label = {-1: "Keep the forecast rain", 0: "Imagine: no rain", 2: "Imagine: light rain (2 mm)",
                              5: "Imagine: moderate rain (5 mm)", 10: "Imagine: heavy rain (10 mm)",
                              20: "Imagine: very heavy rain (20 mm)"}
                rain = c3.selectbox("3. Imagine the rain", list(rain_label), format_func=rain_label.get, index=4)
                s = sc[(sc["city"] == city) & (sc["days_ahead"] == day)]
                base = s[(s["wind_change"] == 0) & (s["rain_choice"] == -1)]["predicted_aqi"]
                new = s[(s["wind_change"] == wind) & (s["rain_choice"] == rain)]["predicted_aqi"]
                if len(base) and len(new):
                    b, n = float(base.iloc[0]), float(new.iloc[0])
                    dname = pd.to_datetime(fc.loc[fc["days_ahead"] == day, "forecast_date"].iloc[0]).strftime("%A")
                    wtxt = "the wind as forecast" if wind == 0 else (f"wind {wind} km/h stronger" if wind > 0
                                                                     else f"wind {-wind} km/h weaker")
                    rtxt = rain_label[rain].replace("Imagine: ", "").lower() if rain >= 0 else "rain as forecast"
                    verdict = ("cleaner" if n < b - 3 else "more polluted" if n > b + 3 else "about the same")
                    html(f"""<div style="font-size:17px;margin-top:6px">Real forecast AQI <b>{b:.0f}</b> {badge(level_of(b))}
                    &nbsp;→&nbsp; in your imagined weather <b style="font-size:24px">{n:.0f}</b> {badge(level_of(n))}</div>
                    <div class="why"><b>Reading:</b> on <b>{dname}</b>, with {wtxt} and {rtxt}, the air in {city} would be
                    <b>{verdict}</b> ({n - b:+.0f} AQI points). <i>These numbers are calculated by the trained Spark model.</i></div>""")


    st.write("")
    html("""<div class="sec">✅ Can we trust today's data? (data reliability check)</div>
    <div class="sub">Every morning, <b>before</b> the models use new data, the pipeline checks it - like checking ingredients
    before cooking. Bad data would give wrong forecasts, so a city that fails is not trusted until its data recovers.</div>""")
    dh, dq = read("data_source_health.csv"), read("data_quality_checks.csv")
    CHECK_INFO = {
        "Freshness": ("📅", "Fresh", "Is the newest data from yesterday? (not old)"),
        "Completeness": ("🧩", "Complete", "Are fewer than 10% of values missing?"),
        "Validity": ("📏", "Realistic", "Are all values possible? (e.g. humidity 0-100%)"),
        "Frozen data": ("🧊", "Not stuck", "Is the source changing, not repeating the same value?"),
    }
    if dh is not None and dq is not None:
        mark = {"PASS": ("✔", "#3B6D11"), "WARN": ("!", "#854F0B"), "FAIL": ("✖", "#A32D2D")}
        head = "".join(f"<th style='padding:8px;font-size:14px'>{i}<br>{n}</th>" for i, n, _ in CHECK_INFO.values())
        body = ""
        status_style = {"Operational": ("#EAF3DE", "#27500A", "🟢 Trusted"), "Warning": ("#FAEEDA", "#633806", "🟡 Small issues"),
                        "Faulty": ("#FCEBEB", "#791F1F", "🔴 Not trusted")}
        for _, h in dh.iterrows():
            cells = ""
            for chk in CHECK_INFO:
                r = dq[(dq["city"] == h["city"]) & (dq["check"] == chk)]
                stt = r["status"].iloc[0] if len(r) else "WARN"
                sym, col = mark.get(stt, ("?", "#475569"))
                tip = r["result_value"].iloc[0] if len(r) else ""
                cells += f"<td style='text-align:center;padding:8px' title='{tip}'><b style='color:{col};font-size:20px'>{sym}</b><br><span class='small'>{tip}</span></td>"
            bg, fg, txt = status_style.get(h["status"], status_style["Warning"])
            body += (f"<tr style='border-bottom:1px solid #eef2f7'><td style='padding:8px;font-size:16px'><b>{h['city']}</b></td>{cells}"
                     f"<td style='padding:8px'><span class='badge' style='background:{bg};color:{fg}'>{txt}</span></td></tr>")
        html(f"""<div class="card"><table style="width:100%;border-collapse:collapse">
        <tr style="background:#f8fafc"><th style='padding:8px;text-align:left'>City</th>{head}<th style='padding:8px'>Verdict</th></tr>
        {body}</table>
        <div class="howto"><b>How to read:</b> <b style="color:#3B6D11">✔</b> = check passed,
        <b style="color:#854F0B">!</b> = small issue, <b style="color:#A32D2D">✖</b> = failed.
        """ + " ".join(f"<b>{n}</b>: {d}" for _, n, d in CHECK_INFO.values()) + """</div></div>""")
        with st.expander("Technical details (every check with its rule)"):
            st.dataframe(dq.rename(columns={"city": "City", "check": "Check", "result_value": "Result",
                                            "rule": "Rule", "status": "Status"}), hide_index=True)
    st.write("")

        # ---------------------------------------------------------------------------
        # PIPELINE status
        # ---------------------------------------------------------------------------
    st.write("")
    html("""<div class="sec">⚙️ The automated pipeline</div>
    <div class="sub">A <b>pipeline</b> is a chain of steps that runs one after another <i>without anyone pressing a button</i>.
    Ours runs <b>by itself every morning at 7:00</b> (Windows Task Scheduler). This page only <i>shows</i> its results.</div>""")
    steps = [("1", "Download", "live air and weather data"), ("2", "Check", "data reliability"),
             ("3", "Clean", "PySpark fixes and fills data"), ("4", "Detect", "unusual days (KMeans)"),
             ("5", "Prepare", "features for the models"), ("6", "Learn", "retrain models if needed"),
             ("7", "Forecast", "next 7 days, every city"), ("8", "Check accuracy", "forecast vs reality"),
             ("9", "Warn", "alerts and actions")]
    html('<div style="display:grid;grid-template-columns:repeat(9,minmax(0,1fr));gap:6px">' + "".join(
        f'<div class="card" style="padding:10px;text-align:center"><div style="background:#185FA5;color:#fff;width:26px;'
        f'height:26px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-weight:700">{n}</div>'
        f'<div style="font-weight:700;margin-top:4px">{t}</div><div class="small">{d}</div></div>' for n, t, d in steps) + "</div>")
    st.write("")
    c1, c2 = st.columns([1, 2])
    with c1:
        html(f"""<div class="card"><div class="lbl">Last run</div><div style="font-size:22px;font-weight:800">{last_run}</div>
        <div class="lbl" style="margin-top:8px">Next automatic run</div><div style="font-size:22px;font-weight:800">Tomorrow 7:00 AM</div></div>""")
        st.write("")
        st.markdown("**Run it now** (instead of waiting for 7:00 AM). Takes a few minutes; please keep this page open.")
        if st.button("▶ Run pipeline now"):
            import os
            env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
            with st.spinner("Pipeline is running: download → check → clean → detect → forecast → warn ..."):
                res = subprocess.run([sys.executable, "run_pipeline.py", "--daily"], cwd=ROOT, capture_output=True,
                                     text=True, encoding="utf-8", errors="replace", env=env)
            st.cache_data.clear()
            out = ((res.stdout or "") + "\n" + (res.stderr or "")).strip().splitlines()
            if res.returncode == 0:
                st.success("✅ Done! New forecast and alerts are ready. Press R (or refresh the page) to see them.")
            else:
                errs = [l for l in out if any(k in l for k in ("Error", "ERROR", "FAILED", "Exception", "memory"))]
                st.error("❌ The run stopped. Main reason:\n\n" + ("\n".join(errs[-3:]) if errs else "see the output below"))
                if any("memory" in l.lower() for l in out):
                    st.warning("Tip: the laptop ran out of memory. Close this app (Ctrl + C in the terminal) and run "
                               "`python run_pipeline.py --daily` in the terminal instead.")
            with st.expander("Output of this run"):
                st.code("\n".join(out[-40:]), language="text")
    with c2:
        logs = sorted(LOGS.glob("pipeline_*.log")) if LOGS.exists() else []
        if logs:
            st.markdown(f"**Record of the last run** (`{logs[-1].name}`) - every step, its time and any warning:")
            st.code("\n".join(logs[-1].read_text(encoding="utf-8", errors="replace").splitlines()[-22:]), language="text")
