"""
generate_data.py
================
Creates a realistic SYNTHETIC daily air-quality dataset for 6 Pakistani cities:
Lahore, Faisalabad, Multan, Peshawar, Islamabad and Karachi.

Time period: 1 January 2023 to 8 November 2026 (almost 4 years, up to "yesterday").

IMPORTANT: This is synthetic (computer-made) data, NOT real sensor readings.
But it is built to behave like real Pakistani air, using public facts:

  * Yearly average PM2.5 for each city is matched to IQAir World Air Quality Reports:
      - 2023: Lahore 99.5, Faisalabad 88.2, Peshawar 76.5, Karachi 56.4, Islamabad 42.4
      - 2024: Lahore went above 100; Karachi fell to about 46; most Punjab/KPK cities rose
      - 2025: Pakistan average fell by about 6.6 (to 67.3), so most cities are a bit lower
      - 2026: no report yet, so we assume it is similar to 2025
  * Smog season starts in late October, peaks November to January, and ends by February.
  * Rice stubble (crop) burning in Punjab: about 10 October to 30 November.
    Wheat stubble burning: a smaller peak in late April and May.
  * Cold, calm winter air traps pollution near the ground (called "temperature inversion").
  * Monsoon rain (July and August) washes pollution out of the air.
  * Karachi gets a strong sea breeze, so it is less polluted and more humid.
  * AQI is calculated from PM2.5 and PM10 with the official US-EPA formula (2024 update).
  * Visibility is calculated with a physics formula: more PM2.5 + more humidity = less visibility.
  * Pollution "remembers" yesterday (today is similar to yesterday), so forecasting is possible.

For practice in the cleaning step, the file also contains some "dirty" data on purpose:
missing values, impossible values (like -999), badly written city names and duplicate rows.

HOW TO RUN (from the main project folder):
    python src\\generate_data.py

OUTPUT:
    data\\raw\\pakistan_air_quality.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd

# =============================================================================
# 1. SETTINGS (in a later step these will move to config\config.yaml)
# =============================================================================
SEED = 42                    # same seed = exactly the same data every time you run it
START_DATE = "2023-01-01"
END_DATE = "2026-11-08"      # the last day in the dataset (change this to get newer data)
OUTPUT_PATH = Path("data/raw/pakistan_air_quality.csv")
ADD_DIRTY_DATA = True        # True = add missing values, duplicates etc. for cleaning practice

rng = np.random.default_rng(SEED)

# =============================================================================
# 2. CITY PROFILES - every city has its own "personality"
# =============================================================================
# pm25_by_year   : yearly average PM2.5 in micrograms per cubic metre (ug/m3)
# temp_jan/jun   : average daily temperature (C) in the coldest / hottest part of the year
# hum_base       : normal humidity (%)
# wind_base      : normal wind speed (km/h)
# monsoon_mm     : how much monsoon rain the city gets (bigger = more rain)
# winter_rain_mm : winter rain from "western disturbances" (January to March)
# crop_exposure  : how much crop-burning smoke reaches the city (0 = none, 1 = the most)
# traffic_base   : normal traffic level on a scale of 1 to 10
# industry_base  : normal industrial activity on a scale of 0 to 100
# pm10_ratio     : PM10 divided by PM2.5 (dusty cities have a bigger number)
# winter_boost   : how strong winter smog is in this city (1 = the strongest)
CITIES = {
    "Lahore": dict(
        pm25_by_year={2023: 99.5, 2024: 102.0, 2025: 94.0, 2026: 94.0},
        temp_jan=12.5, temp_jun=34.0, hum_base=60, wind_base=6.0, monsoon_mm=7.0,
        winter_rain_mm=0.7, crop_exposure=1.00, traffic_base=8.5, industry_base=70,
        pm10_ratio=1.9, winter_boost=1.00),
    "Faisalabad": dict(
        pm25_by_year={2023: 88.2, 2024: 92.0, 2025: 86.0, 2026: 86.0},
        temp_jan=12.0, temp_jun=35.0, hum_base=55, wind_base=6.5, monsoon_mm=4.0,
        winter_rain_mm=0.5, crop_exposure=0.90, traffic_base=6.5, industry_base=85,
        pm10_ratio=2.0, winter_boost=0.95),
    "Multan": dict(
        pm25_by_year={2023: 82.0, 2024: 88.0, 2025: 81.0, 2026: 81.0},
        temp_jan=12.5, temp_jun=36.5, hum_base=50, wind_base=7.0, monsoon_mm=2.5,
        winter_rain_mm=0.3, crop_exposure=0.80, traffic_base=6.0, industry_base=60,
        pm10_ratio=2.3, winter_boost=0.90),
    "Peshawar": dict(
        pm25_by_year={2023: 76.5, 2024: 84.0, 2025: 77.0, 2026: 77.0},
        temp_jan=11.0, temp_jun=33.5, hum_base=55, wind_base=6.0, monsoon_mm=3.0,
        winter_rain_mm=1.5, crop_exposure=0.45, traffic_base=7.0, industry_base=55,
        pm10_ratio=2.1, winter_boost=0.85),
    "Islamabad": dict(
        pm25_by_year={2023: 42.4, 2024: 48.0, 2025: 43.0, 2026: 43.0},
        temp_jan=10.0, temp_jun=31.5, hum_base=58, wind_base=7.5, monsoon_mm=9.0,
        winter_rain_mm=2.0, crop_exposure=0.30, traffic_base=6.5, industry_base=35,
        pm10_ratio=1.8, winter_boost=0.70),
    "Karachi": dict(
        pm25_by_year={2023: 56.4, 2024: 46.0, 2025: 44.0, 2026: 44.0},
        temp_jan=19.0, temp_jun=31.5, hum_base=68, wind_base=14.0, monsoon_mm=2.0,
        winter_rain_mm=0.1, crop_exposure=0.05, traffic_base=9.0, industry_base=90,
        pm10_ratio=2.6, winter_boost=0.55),
}

# Monthly pollution multiplier (January ... December) for Punjab and KPK cities.
# November is the worst month (crop burning + cold calm air). July-August are the cleanest (monsoon).
SEASON_FACTOR = np.array([1.75, 1.30, 0.95, 0.85, 0.85, 0.75, 0.50, 0.50, 0.65, 1.15, 2.05, 1.90])
# Karachi has a flatter pattern: the summer sea breeze blows pollution away.
SEASON_FACTOR_KARACHI = np.array([1.55, 1.30, 1.00, 0.80, 0.70, 0.60, 0.55, 0.55, 0.70, 1.05, 1.45, 1.70])

# US-EPA PM2.5 breakpoints (24-hour average, 2024 update): (conc_low, conc_high, aqi_low, aqi_high)
PM25_BREAKPOINTS = [(0.0, 9.0, 0, 50), (9.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
                    (55.5, 125.4, 151, 200), (125.5, 225.4, 201, 300), (225.5, 325.4, 301, 500)]
# US-EPA PM10 breakpoints (24-hour average)
PM10_BREAKPOINTS = [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
                    (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)]


# =============================================================================
# 3. HELPER FUNCTIONS
# =============================================================================
def memory_noise(n, phi, sigma):
    """Random noise that 'remembers' yesterday (statisticians call it AR(1) noise):
    today = phi * yesterday + small random shock.
    This makes hot spells, calm spells and smog episodes last for several days, like in real life."""
    x = np.zeros(n)
    shocks = rng.normal(0, sigma, n)
    for t in range(1, n):
        x[t] = phi * x[t - 1] + shocks[t]
    return x


def yearly_curve(day_of_year, low, high, peak_day=172):
    """A smooth yearly wave: equals 'high' on peak_day and 'low' six months later.
    Day 172 is about 21 June."""
    return low + (high - low) * (1 + np.cos(2 * np.pi * (day_of_year - peak_day) / 365.25)) / 2


def crop_burning_index(dates, exposure):
    """Crop-burning index from 0 to 10 (like a count of farm fires seen by satellite).
    Rice stubble: peak around 5 November. Wheat stubble: smaller peak around 2 May."""
    doy = dates.dayofyear.values
    rice = 10 * np.exp(-0.5 * ((doy - 309) / 13) ** 2)
    wheat = 4 * np.exp(-0.5 * ((doy - 122) / 10) ** 2)
    base = (rice + wheat) * exposure
    daily_variation = rng.gamma(shape=3, scale=1 / 3, size=len(dates))   # average 1, never negative
    return np.clip(base * daily_variation, 0, 10)


def calculate_aqi(concentration, breakpoints):
    """Official US-EPA formula:
    AQI = (AQI_high - AQI_low) / (C_high - C_low) * (C - C_low) + AQI_low"""
    aqi = np.full(len(concentration), 500.0)          # anything above the table = 500 (maximum)
    for c_low, c_high, i_low, i_high in breakpoints:
        inside = (concentration >= c_low) & (concentration <= c_high)
        aqi[inside] = (i_high - i_low) / (c_high - c_low) * (concentration[inside] - c_low) + i_low
    return aqi


def smog_category(aqi):
    """Turns AQI into the 4 smog levels used in this project."""
    return np.select([aqi <= 150, aqi <= 200, aqi <= 300],
                     ["Low", "Moderate", "Severe"], default="Hazardous")


# =============================================================================
# 4. MAKE DATA FOR ONE CITY
# =============================================================================
def generate_city(city, p, dates):
    n = len(dates)
    doy = dates.dayofyear.values          # day of year: 1 to 366
    month = dates.month.values            # 1 to 12
    year = dates.year.values
    year_index = year - year.min()        # 0 for 2023, 1 for 2024, ...
    weekday = dates.dayofweek.values      # 0 = Monday ... 6 = Sunday

    # ---------------- WEATHER ----------------
    # Temperature: yearly wave + random hot/cold spells that last a few days
    temperature = yearly_curve(doy, p["temp_jan"], p["temp_jun"]) + memory_noise(n, 0.8, 1.2)

    # Rain: monsoon (peak about 1 August) + winter rain (January to March)
    monsoon_shape = np.exp(-0.5 * ((doy - 213) / 22) ** 2)
    winter_shape = np.isin(month, [1, 2, 3]).astype(float)
    rain_mean = p["monsoon_mm"] * monsoon_shape * 2.2 + p["winter_rain_mm"] * winter_shape * 1.5 + 0.05
    rain_chance = np.clip(0.10 + 0.45 * monsoon_shape + 0.12 * winter_shape, 0, 0.7)
    is_rainy = rng.random(n) < rain_chance
    rainfall = np.where(is_rainy, rng.exponential(rain_mean / rain_chance), 0.0)
    rainfall = np.round(np.clip(rainfall, 0, 150), 1)

    # Humidity: high in monsoon and winter fog, low in dry May-June, higher on rainy days
    humidity = (p["hum_base"]
                + 18 * monsoon_shape
                + 14 * np.exp(-0.5 * ((doy % 365 - 5) / 25) ** 2)      # early January fog
                + 14 * np.exp(-0.5 * ((doy - 360) / 15) ** 2)          # late December fog
                - 15 * np.exp(-0.5 * ((doy - 145) / 25) ** 2)          # dry hot May-June
                + 8 * (rainfall > 0)
                + memory_noise(n, 0.7, 4))
    humidity = np.clip(humidity, 12, 100)

    # Wind: stronger in summer, very calm in winter (calm air traps smog)
    wind_speed = p["wind_base"] * yearly_curve(doy, 0.65, 1.35, peak_day=160) * np.exp(memory_noise(n, 0.5, 0.25))
    wind_speed = np.clip(wind_speed, 0.5, 45)

    # ---------------- HUMAN ACTIVITY ----------------
    # Traffic: lower on Saturday and Sunday, grows about 2% every year
    weekend_factor = np.where(weekday == 6, 0.75, np.where(weekday == 5, 0.92, 1.0))
    traffic_level = np.clip(p["traffic_base"] * weekend_factor * (1 + 0.02 * year_index)
                            + rng.normal(0, 0.5, n), 1, 10)

    # Industry: lower on Sunday; in Punjab, some factories are closed in November (smog rules)
    smog_rules = np.where((month == 11) & (p["crop_exposure"] > 0.5), 0.85, 1.0)
    industrial_activity = np.clip(p["industry_base"] * np.where(weekday == 6, 0.8, 1.0) * smog_rules
                                  + rng.normal(0, 4, n), 0, 100)

    crop_burning = crop_burning_index(dates, p["crop_exposure"])

    # ---------------- PM2.5 (the main pollutant) ----------------
    if city == "Karachi":
        season = SEASON_FACTOR_KARACHI[month - 1]
    else:
        season = 1 + (SEASON_FACTOR[month - 1] - 1) * p["winter_boost"]

    wind_effect = (p["wind_base"] / (wind_speed + 1.5)) ** 0.55          # less wind = more pollution
    rain_washout = np.exp(-0.06 * rainfall)                               # rain cleans the air
    inversion = 1 + 0.35 * np.clip((18 - temperature) / 10, 0, 1.2)       # cold air traps pollution
    crop_effect = 1 + 0.12 * crop_burning                                 # index 10 = about 2.2x pollution
    activity = (0.5 + 0.3 * traffic_level / p["traffic_base"]
                + 0.2 * industrial_activity / max(p["industry_base"], 1))

    pm25_shape = season * wind_effect * rain_washout * inversion * crop_effect * activity
    pm25_shape *= np.exp(memory_noise(n, 0.75, 0.18))                     # multi-day random episodes

    # Calibration: scale every year so its average equals the real IQAir yearly average
    pm25 = np.empty(n)
    for y in np.unique(year):
        this_year = year == y
        pm25[this_year] = pm25_shape[this_year] * p["pm25_by_year"][y] / pm25_shape[this_year].mean()
    pm25 = np.clip(pm25, 3, 700)

    # ---------------- OTHER POLLUTANTS ----------------
    # PM10: bigger dust particles; summer dust storms make it even higher
    dust = 1 + 0.35 * np.exp(-0.5 * ((doy - 150) / 30) ** 2) * (wind_speed / p["wind_base"])
    pm10 = pm25 * p["pm10_ratio"] * dust * rng.lognormal(0, 0.08, n)

    # NO2 (ppb): mostly from vehicles; builds up in calm cold air
    no2 = 6 * traffic_level * wind_effect * inversion * 0.8 + rng.normal(0, 4, n)
    # SO2 (ppb): from factories and brick kilns
    so2 = 0.25 * industrial_activity * wind_effect * (0.7 + 0.3 * season) + rng.normal(0, 2.5, n)
    # CO (ppm): from vehicles and burning; moves together with PM2.5
    co = 0.3 + 0.012 * pm25 + 0.05 * traffic_level + rng.normal(0, 0.15, n)
    # Ozone (ppb): made by sunlight and heat; lower on dark smoggy or rainy days
    ozone = 12 + 1.6 * np.clip(temperature, 0, None) - 0.04 * pm25 - 6 * (rainfall > 0) + rng.normal(0, 6, n)
    # CO2 (ppm): world level about 421 in 2023, rising about 2.5 per year, plus extra from city activity
    co2 = (421 + 2.5 * year_index + 2.5 * doy / 365
           + 8 * (traffic_level / 10) + 0.06 * industrial_activity
           + 6 * (season - 1) + rng.normal(0, 2.5, n))

    # ---------------- AQI, VISIBILITY, SMOG LEVEL ----------------
    aqi = np.maximum(calculate_aqi(np.round(pm25, 1), PM25_BREAKPOINTS),
                     calculate_aqi(np.round(pm10), PM10_BREAKPOINTS))   # AQI = the worse of the two
    aqi = np.clip(np.round(aqi), 0, 500)

    # Visibility (Koschmieder formula): visibility = 3.912 / extinction.
    # In humid air, particles absorb water and grow, so they block more light.
    humidity_growth = (1 - np.clip(humidity, 0, 97) / 100) ** -0.55
    extinction = 0.02 + 0.0040 * pm25 * humidity_growth + 0.004 * rainfall
    visibility = np.clip(3.912 / extinction * rng.lognormal(0, 0.10, n), 0.05, 20.0)

    return pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "city": city,
        "temperature_c": np.round(temperature, 1),
        "humidity_pct": np.round(humidity, 1),
        "wind_speed_kmh": np.round(wind_speed, 1),
        "rainfall_mm": rainfall,
        "pm25": np.round(pm25, 1),
        "pm10": np.round(pm10, 1),
        "co2_ppm": np.round(co2, 1),
        "no2_ppb": np.round(np.clip(no2, 2, None), 1),
        "so2_ppb": np.round(np.clip(so2, 1, None), 1),
        "co_ppm": np.round(np.clip(co, 0.1, None), 2),
        "ozone_ppb": np.round(np.clip(ozone, 3, None), 1),
        "traffic_level": np.round(traffic_level, 1),
        "industrial_activity": np.round(industrial_activity, 1),
        "crop_burning_index": np.round(crop_burning, 2),
        "aqi": aqi.astype(int),
        "visibility_km": np.round(visibility, 2),
        "smog_level": smog_category(aqi),
    })


# =============================================================================
# 5. ADD "DIRTY" DATA ON PURPOSE (for the cleaning step)
# =============================================================================
def add_dirty_data(df):
    df = df.copy()
    n = len(df)
    # (a) about 2% missing values in sensor columns (sensor was offline)
    for col in ["temperature_c", "humidity_pct", "wind_speed_kmh", "pm25", "pm10",
                "no2_ppb", "so2_ppb", "co_ppm", "ozone_ppb"]:
        rows = rng.choice(n, size=int(0.02 * n), replace=False)
        df.loc[rows, col] = np.nan
    # (b) sensor errors: impossible values
    df.loc[rng.choice(n, size=15, replace=False), "pm25"] = -999.0
    df.loc[rng.choice(n, size=10, replace=False), "humidity_pct"] = 150.0
    # (c) badly typed city names, e.g. "lahore " instead of "Lahore"
    rows = rng.choice(n, size=20, replace=False)
    df.loc[rows, "city"] = df.loc[rows, "city"].str.lower() + " "
    # (d) 30 duplicate rows (the same row saved twice)
    df = pd.concat([df, df.sample(30, random_state=SEED)], ignore_index=True)
    return df


# =============================================================================
# 6. MAIN PROGRAM
# =============================================================================
def main():
    # Make full years first (so the yearly calibration is correct), then cut at END_DATE
    full_dates = pd.date_range(START_DATE, f"{pd.Timestamp(END_DATE).year}-12-31", freq="D")
    df = pd.concat([generate_city(c, p, full_dates) for c, p in CITIES.items()], ignore_index=True)
    df = df[df["date"] <= END_DATE]
    df = df.sort_values(["city", "date"]).reset_index(drop=True)

    if ADD_DIRTY_DATA:
        df = add_dirty_data(df)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved {len(df):,} rows x {df.shape[1]} columns -> {OUTPUT_PATH}")
    print(f"Dates: {df['date'].min()} to {df['date'].max()}")

    good = df[df["pm25"] > 0].copy()
    good["city"] = good["city"].str.strip().str.title()
    good["year"] = good["date"].str[:4]
    print("\nAverage PM2.5 per city per year (ug/m3).")
    print("Note: the last year is lower only because it stops before the worst smog weeks (late Nov-Dec).")
    print(good.pivot_table(index="city", columns="year", values="pm25").round(1))
    print("\nAverage AQI and visibility per city:")
    print(good.groupby("city")[["aqi", "visibility_km"]].mean().round(1).sort_values("aqi", ascending=False))
    print("\nNumber of days in each smog level:")
    print(df["smog_level"].value_counts())


if __name__ == "__main__":
    main()
