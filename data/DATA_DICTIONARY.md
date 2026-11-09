# Data Dictionary: Pakistan Air Quality Dataset

**File:** `data/raw/pakistan_air_quality.csv`
**Made by:** `src/generate_data.py`
**Type:** Synthetic (computer-made) data calibrated to real public figures. It is not real sensor data.

## Quick facts

| Item | Value |
|---|---|
| Cities | Lahore, Faisalabad, Multan, Peshawar, Islamabad, Karachi |
| Time period | 1 January 2023 to 8 November 2026 (daily) |
| Rows | 8,478 (8,448 real days plus 30 duplicate rows added on purpose) |
| Columns | 19 |
| One row means | The weather and air quality of **one city on one day** |

## What is in each column

| # | Column | Meaning (easy English) | Unit | Typical range |
|---|---|---|---|---|
| 1 | `date` | The day of the reading | YYYY-MM-DD | 2023-01-01 to 2026-11-08 |
| 2 | `city` | City name | text | 6 cities |
| 3 | `temperature_c` | Average temperature of the day | °C | 5 to 40 |
| 4 | `humidity_pct` | How much water vapour is in the air | % | 15 to 100 |
| 5 | `wind_speed_kmh` | Average wind speed. Calm wind traps pollution. | km/h | 1 to 30 |
| 6 | `rainfall_mm` | Rain on that day. Rain cleans the air. | mm | 0 to 150 |
| 7 | `pm25` | **PM2.5**: very fine dust and smoke particles. They enter deep into the lungs. This is the **main smog pollutant**. | µg/m³ | 5 to 700 |
| 8 | `pm10` | **PM10**: bigger dust particles from roads, construction and dust storms | µg/m³ | 10 to 1,000+ |
| 9 | `co2_ppm` | Carbon dioxide, from burning fuel | ppm | 420 to 450 |
| 10 | `no2_ppb` | Nitrogen dioxide, mostly from vehicle exhaust | ppb | 5 to 90 |
| 11 | `so2_ppb` | Sulphur dioxide, from factories and brick kilns | ppb | 1 to 50 |
| 12 | `co_ppm` | Carbon monoxide, from vehicles and burning | ppm | 0.3 to 9 |
| 13 | `ozone_ppb` | Ground-level ozone. Sunlight and heat make it, so it is higher in summer. | ppb | 5 to 90 |
| 14 | `traffic_level` | How busy the roads are. Lower on weekends. | 1 to 10 | 4 to 10 |
| 15 | `industrial_activity` | How much factories are working. Lower on Sundays and during November smog rules. | 0 to 100 | 25 to 100 |
| 16 | `crop_burning_index` | How much farm stubble is being burned nearby (like a satellite fire count) | 0 to 10 | 0 most of the year, up to 10 in Oct–Nov |
| 17 | `aqi` | **Air Quality Index**: one number that shows how dangerous the air is. **Target 1.** | 0 to 500 | 30 to 500 |
| 18 | `visibility_km` | How far you can see. Smog and fog make it short. **Target 2.** | km | 0.8 to 20 |
| 19 | `smog_level` | Smog category made from AQI. **Target 3.** | text | Low / Moderate / Severe / Hazardous |

## How the targets are made

**AQI** uses the official US-EPA formula (2024 update). The AQI is the higher of the PM2.5 AQI and the PM10 AQI.

| AQI | US-EPA name | Our `smog_level` |
|---|---|---|
| 0 – 150 | Good / Moderate / Unhealthy for sensitive groups | **Low** |
| 151 – 200 | Unhealthy | **Moderate** |
| 201 – 300 | Very Unhealthy | **Severe** |
| 301 – 500 | Hazardous | **Hazardous** |

**Visibility** uses the Koschmieder physics formula. More PM2.5 and more humidity block more light, so you see less far.

## Real-world facts the data follows

- **Yearly PM2.5 averages** match IQAir World Air Quality Reports. For example, Lahore was about 99.5 in 2023, about 102 in 2024 and lower in 2025; Karachi fell from 56 to about 46.
- **Smog season:** starts in late October, is worst in November, stays bad until January and ends by February.
- **Crop burning:** rice stubble from about 10 October to 30 November, and a smaller wheat-stubble peak in late April and May.
- **Winter:** cold, calm air traps pollution near the ground (temperature inversion). Fog plus smog drops visibility to 1–3 km.
- **Monsoon (July–August):** rain washes the air clean. This is the cleanest time of the year.
- **Karachi:** the sea breeze keeps pollution lower and humidity higher.
- **Persistence:** today's pollution is very similar to yesterday's (correlation about 0.88). This is why forecasting tomorrow and the day after is possible.
- **2026:** there is no official report yet, so 2026 is assumed to be similar to 2025.

## "Dirty" data added on purpose (for the cleaning step)

| Problem | How much | How we will fix it |
|---|---|---|
| Missing values (empty cells) | About 2% in sensor columns | Fill them using nearby days |
| `pm25 = -999` | 15 rows | Treat as missing (sensor error) |
| `humidity_pct = 150` | 10 rows | Treat as missing (impossible above 100) |
| City names like `"lahore "` | 20 rows | Remove spaces and fix capital letters |
| Duplicate rows | 30 rows | Delete the copies |

## Important note for the report

> "The dataset is synthetic. It was generated in Python and calibrated to publicly reported IQAir annual PM2.5 averages (2023–2025) and known seasonal patterns of smog, crop burning and monsoon in Pakistan. It is suitable for learning and demonstrating a machine learning pipeline, but it is not a replacement for real monitoring data."
