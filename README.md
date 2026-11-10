# Pakistan Smog Early-Warning Pipeline (PySpark)

An **automated, end-to-end PySpark machine learning pipeline** that runs every day on its own. It downloads **real, live air-quality and weather data** for six Pakistani cities and forecasts each city's **Air Quality Index (AQI)**, **visibility (km)** and **smog level** (Low / Moderate / Severe / Hazardous) for the **next 7 days**. It prints the forecast as a table in the terminal and turns it into a **Smog Early-Warning Bulletin** that says what the public and each authority should do.

## The problem

Every winter (October to February), smog in Punjab and KPK reaches hazardous levels. For example, Lahore's citywide AQI reached about 475 in November 2025. Decisions are usually made **after** the air is already toxic, for things like:
- closing schools,
- stopping brick kilns,
- enforcing the crop-burning ban, and
- warning motorway drivers.

**This pipeline gives a 7-day early warning from real data, automatically.** Every morning it:
1. ingests the newest environmental data,
2. cleans it,
3. learns from it,
4. forecasts the next 7 days,
5. checks its own accuracy, and
6. publishes an action bulletin.

No one has to press a button.

## How it works

```
 live data -> quality checks -> clean -> anomaly detection -> features -> train* -> forecast -> monitor -> alerts -> report
                                  (*only when needed)
```

| Step | File | What it does | Technology |
|---|---|---|---|
| 1. Ingest | `src/ingest.py` | Downloads **real** hourly PM2.5, PM10, NO₂, SO₂, CO, ozone, CO₂ and dust (CAMS), plus temperature, humidity, wind, rain and visibility, from Open-Meteo for 6 cities since 2023. Also downloads the **7-day weather forecast**. Data is aggregated to daily values. Updates are incremental, and the step falls back to the cache when offline. | Python (urllib), pandas |
| Quality | `src/quality.py` | **Data quality gate:** checks freshness, missing values, valid ranges and frozen data. Gives every city a health status (Operational / Warning / Faulty) and stops the pipeline if every city's data is faulty. | pandas |
| 2. Clean | `src/clean.py` | Removes duplicates, turns impossible values into missing, and fills gaps from neighbouring days. Then **calculates AQI (US-EPA formula), visibility and smog level in Spark**, and writes a data-quality report and city insights. | **PySpark DataFrame API, Window functions** |
| Anomaly | `src/anomaly.py` | **Anomaly detection:** KMeans clustering on standardised pollutants and weather, plus a spike rule (PM2.5 more than double its 7-day average). It flags unusual days that could mean a fire, a chemical emission or a dust storm. | **PySpark MLlib (KMeans)** |
| 3. Features | `src/features.py` | Builds one row per day and horizon (1–7). Each row holds today's pollution and weather, yesterday's values, 3- and 7-day averages, and the **forecast weather of the target day** (wind, rain, temperature, humidity) plus its season, smog season and crop-burning season. | **PySpark Window functions** |
| 4. Train | `src/train.py` | **Model 1:** AQI and visibility (Linear Regression, Random Forest, Gradient-Boosted Trees). **Model 2:** smog level (Logistic Regression, Random Forest). Uses a time-based split, 3-fold cross-validation and hyperparameter tuning, with results for every horizon (1–7 days) compared against a baseline. The best model is saved. | **PySpark MLlib** |
| 5. Forecast | `src/forecast.py` | The best models plus the real weather forecast give the **next 7 days** for each city, printed as a table in the terminal with a short reason for each day | **PySpark MLlib** |
| 6. Monitor | `src/monitor.py` | Stores every forecast and compares it with the real data when that day arrives. If the error grows too large, it triggers automatic retraining. | pandas |
| 7. Alerts | `src/alerts.py` | Writes the **Smog Early-Warning Bulletin**: alert level, trend, public advice and actions for each department. Adds **pollutant-specific rules** (NO₂ → traffic, SO₂ → industry, PM10 → construction, CO, ozone) and **disaster signs** (dust storm, heatwave, dense smog/fog, unusual events). It also writes **event triggers** as JSON payloads to `reports/events/` and keeps an event log. | pandas |
| 8. Report | `src/evaluate.py` | Result tables, evaluation figures and a written summary report | Matplotlib |

**Automation features**
- **Scheduled:** `run_daily.bat` runs the pipeline every morning through Windows Task Scheduler.
- **Smart retraining:** models are retrained only when there are no models yet, when they are older than 7 days, or when monitoring finds that accuracy dropped. Otherwise the daily run reuses the saved models and takes only a few minutes.
- **Self-monitoring:** every forecast is checked against reality later (`forecast_monitoring.csv`).
- **Config-driven:** every setting is in `config/config.yaml` (paths, Spark memory, cleaning rules, horizons, CV grid, retraining and alert thresholds).
- **Logged:** every run writes `logs/pipeline_<date_time>.log`, recording each step, its duration, all alerts and any error.

## Smart Environmental Monitoring System (interface)

`start_app.bat` (or `streamlit run app/app.py`) opens the control room of the pipeline in the browser. It shows:
The interface has three tabs:
- **Live & Forecast**
- **Alerts & Safety**
- **Model & Pipeline**

It shows:
- **Live AQI of this hour** for all six cities, with PM2.5, PM10, NO₂, SO₂, CO, ozone, CO₂, temperature, wind and visibility (downloaded live from Open-Meteo).
- **The 7-day forecast** from the PySpark models, with an **expected error range** for each day and a plain-language reason.
- **Automatic alerts** and actions for authorities and the public, plus a **health guide** for each group.
- A **what-if simulator**: how stronger wind or rain would change the AQI (scenario predictions made by the Spark model).
- An **hour-by-hour animated forecast** for any chosen day (sun, clouds, rain, smog), with temperature, feels-like temperature, dew point, AQI, PM2.5, CO₂, humidity, wind and rain.
- A **live map of Pakistan** with the current AQI at 36 places.
- **Pollutant warnings, disaster signs and anomalies**, each with its recommended action.
- **Data source health** (Operational / Warning / Faulty) and every data quality check.
- An **accuracy tracker** comparing forecasts with real values, and the pipeline status, with a button to run it now.

The pipeline still runs by itself every morning. The interface only reads its results and the live hourly data.

## How to run

```bash
venv\Scripts\activate

python run_pipeline.py --daily     # the scheduled daily run (live data, smart retrain, 7-day forecast, monitor, alerts)
python run_pipeline.py             # full rebuild: all steps, models retrained with cross-validation (~30 min)
python run_pipeline.py --quick     # full rebuild without cross-validation (~5-10 min)
python run_pipeline.py --steps forecast,alerts    # only some steps
```

**Schedule it to run every day at 7:00 AM (Windows, run once in Command Prompt):**
```bash
schtasks /create /sc daily /st 07:00 /tn "SmogEarlyWarning" /tr "C:\projects\pakistan-air-quality-pipeline\run_daily.bat"
```

Requirements: Python 3.10+, Java 17, `pip install -r requirements.txt` (including `setuptools` for Python 3.12). On Windows you also need Hadoop `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`, with `HADOOP_HOME=C:\hadoop` set.

## Outputs

| Location | Contents |
|---|---|
| `reports/alerts/smog_bulletin_<date>.txt` | **Early-warning bulletin**: per-city alert, 7-day outlook, public advice, authority actions |
| `reports/alerts/smog_alerts_<date>.csv` | The same alerts in machine-readable form (can feed SMS or email systems) |
| `reports/results/latest_forecast.csv` | 7-day AQI, visibility and smog-level forecast for every city, with forecast weather and reasons |
| `reports/results/forecast_history.csv`, `forecast_monitoring.csv` | Every forecast made, and how accurate it turned out |
| `reports/results/*.csv` | Data quality, city summary, correlations, model metrics, best models, feature importance |
| `reports/summary_report.md` | Automatically written summary of all results, including the latest bulletin |
| `models/` | Best saved Spark model for every target and horizon |
| `logs/` | One log file per run |

## Data (real, live)

**Source:** [Open-Meteo](https://open-meteo.com), which is free and needs no API key.
- **Air quality:** CAMS atmospheric model data (Copernicus): PM2.5, PM10, NO₂, SO₂, CO, ozone, CO₂ and dust.
- **Weather:** temperature, humidity, wind, precipitation and visibility, from historical forecasts for the past and the live forecast for the next 8 days.
- **Cities:** Lahore, Faisalabad, Multan, Peshawar, Islamabad and Karachi.
- **Period:** 1 January 2023 to yesterday, updated automatically on every run.

AQI is calculated in Spark from daily PM2.5 and PM10 with the US-EPA formula. Every column is explained in `data/DATA_DICTIONARY.md`.

> CAMS values are model estimates for each city's location, not readings from one street sensor. They can differ from local monitors such as the EPA Punjab stations.

## Evaluation

- **Regression:** RMSE, MAE and R² on the test period (1 Oct 2025 onwards, which includes two smog seasons), reported separately for each horizon from 1 to 7 days
- **Classification:** accuracy, weighted F1, precision and recall, plus a confusion matrix
- Every model is compared with a **baseline** that predicts tomorrow will be the same as today.

## Objectives

1. Build a scalable, automated data cleaning and feature engineering pipeline using PySpark.
2. Identify the factors that most strongly influence air quality.
3. Compare multiple regression models and select the best performer, using cross-validation and hyperparameter tuning.
4. Turn forecasts into early warnings and actions: the problem-solving output.
5. Package the work as a reproducible, well-documented GitHub project.

## Tech stack

Python, PySpark (DataFrame API and MLlib), Open-Meteo API, pandas, NumPy, Matplotlib, YAML, Windows Task Scheduler, Git/GitHub
