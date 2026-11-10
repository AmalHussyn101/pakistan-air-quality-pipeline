# Data Dictionary: Live Pakistan Air Quality Data

**Raw file:** `data/raw/live_daily.csv`, made by `src/ingest.py`
**Source:** Open-Meteo (free). Air quality comes from CAMS (Copernicus Atmosphere Monitoring Service). Weather comes from Open-Meteo historical forecasts (past days) and the live forecast (next days).
**Cities:** Lahore, Faisalabad, Multan, Peshawar, Islamabad, Karachi
**Period:** 1 January 2023 to yesterday (it updates every run), plus weather forecast days ahead
**One row** = one city on one day. Hourly values are averaged to daily values; rain is summed.

| Column | Meaning (easy English) | Unit |
|---|---|---|
| `date` | Day (Pakistan time) | YYYY-MM-DD |
| `city` | City name | text |
| `is_future` | 0 = past day with real measurements, 1 = future day (weather forecast only) | 0/1 |
| `pm25` | **PM2.5**: very fine smoke and dust that reach deep into the lungs. The main smog pollutant. | µg/m³ |
| `pm10` | **PM10**: bigger dust particles from roads, construction and dust storms | µg/m³ |
| `no2_ugm3` | Nitrogen dioxide, mainly from vehicles | µg/m³ |
| `so2_ugm3` | Sulphur dioxide, from factories and brick kilns | µg/m³ |
| `co_ugm3` | Carbon monoxide, from vehicles and burning | µg/m³ |
| `ozone_ugm3` | Ground-level ozone, formed by sunlight and heat | µg/m³ |
| `co2_ppm` | Carbon dioxide, a greenhouse gas | ppm |
| `dust_ugm3` | Desert and soil dust | µg/m³ |
| `temperature_c` | Average temperature. Cold air traps smog (inversion). | °C |
| `humidity_pct` | Relative humidity. Humid air thickens haze and fog. | % |
| `wind_speed_kmh` | Average wind speed. Wind blows pollution away. | km/h |
| `rainfall_mm` | Total rain in the day. Rain washes the air. | mm |
| `visibility_m` | How far you can see, from the weather model | metres |

## Columns calculated by the pipeline (in Spark, in `src/clean.py`)

| Column | How it is made |
|---|---|
| `aqi` | US-EPA Air Quality Index (2024 update) from daily PM2.5 and PM10; the higher of the two |
| `visibility_km` | Weather-model visibility. If it is missing, it is estimated from PM2.5 and humidity (Koschmieder formula). |
| `smog_level` | From AQI: **Low** 0–150, **Moderate** 151–200, **Severe** 201–300, **Hazardous** 301–500 |

## What the models predict

For every city and each of the next **7 days**: `aqi`, `visibility_km` and `smog_level`.

## Note

CAMS data are model estimates for each city's coordinates, not readings from one street sensor. Values can differ from local monitoring stations.
