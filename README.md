# Pakistan Smog Early-Warning Pipeline (PySpark)

An **automated, end-to-end PySpark machine learning pipeline** that runs every day on its own and forecasts each city's **Air Quality Index (AQI)**, **visibility (km)** and **smog level** (Low / Moderate / Severe / Hazardous) for the **next 3 days**. It then turns the forecast into a **Smog Early-Warning Bulletin** that says what the public and each authority should do.

## The problem

Every winter (October to February), smog in Punjab and KPK reaches hazardous levels. For example, Lahore's citywide AQI reached about 475 in November 2025. Decisions are usually made **after** the air is already toxic, for things like:
- closing schools,
- stopping brick kilns,
- enforcing the crop-burning ban, and
- warning motorway drivers.

**This pipeline gives a 3-day early warning, automatically.** Every morning it:
1. ingests the newest environmental data,
2. cleans it,
3. learns from it,
4. forecasts the next 3 days,
5. checks its own accuracy, and
6. publishes an action bulletin.

No one has to press a button.

## How it works

```
 new data -> clean -> features -> train* -> forecast -> monitor -> alerts -> report
                                  (*only when needed)
```

| Step | File | What it does | Technology |
|---|---|---|---|
| 1. Generate / ingest | `src/generate_data.py` | Daily environmental data for 6 cities: weather, PM2.5, PM10, CO₂, NO₂, SO₂, CO, ozone, traffic, industry, crop burning | NumPy, pandas |
| 2. Clean | `src/clean.py` | Fixes city names, removes duplicates, turns impossible sensor values into missing, fills gaps from neighbouring days, writes a data-quality report | **PySpark DataFrame API, Window functions** |
| 3. Features | `src/features.py` | Season, crop-burning season, smog season, yesterday's pollution (lags), 3- and 7-day averages, plus targets for +1/+2/+3 days | **PySpark Window functions** |
| 4. Train | `src/train.py` | **Model 1:** AQI and visibility (Linear Regression, Random Forest, Gradient-Boosted Trees). **Model 2:** smog level (Logistic Regression, Random Forest). Uses a time-based split, 3-fold cross-validation, hyperparameter tuning and a baseline comparison. The best model is saved. | **PySpark MLlib** |
| 5. Forecast | `src/forecast.py` | The best saved models forecast the next 3 days for each city | **PySpark MLlib** |
| 6. Monitor | `src/monitor.py` | Stores every forecast and compares it with the real data when that day arrives. If the error grows too large, it triggers automatic retraining. | pandas |
| 7. Alerts | `src/alerts.py` | Writes the **Smog Early-Warning Bulletin**: alert level, trend, public advice, and actions for the education, industries, environment, traffic, motorway and health departments | pandas |
| 8. Report | `src/evaluate.py` | Result tables, evaluation figures and a written summary report | Matplotlib |

**Automation features**
- **Scheduled:** `run_daily.bat` runs the pipeline every morning through Windows Task Scheduler.
- **Smart retraining:** models are retrained only when there are no models yet, when they are older than 7 days, or when monitoring finds that accuracy dropped. Otherwise the daily run reuses the saved models and takes only a few minutes.
- **Self-monitoring:** every forecast is checked against reality later (`forecast_monitoring.csv`).
- **Config-driven:** every setting is in `config/config.yaml` (paths, Spark memory, cleaning rules, horizons, CV grid, retraining and alert thresholds).
- **Logged:** every run writes `logs/pipeline_<date_time>.log`, recording each step, its duration, all alerts and any error.

## How to run

```bash
venv\Scripts\activate

python run_pipeline.py --daily     # the scheduled daily run (ingest, smart retrain, forecast, monitor, alert)
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
| `reports/alerts/smog_bulletin_<date>.txt` | **Early-warning bulletin**: per-city alert, 3-day outlook, public advice, authority actions |
| `reports/alerts/smog_alerts_<date>.csv` | The same alerts in machine-readable form (can feed SMS or email systems) |
| `reports/results/latest_forecast.csv` | 3-day AQI, visibility and smog-level forecast for every city |
| `reports/results/forecast_history.csv`, `forecast_monitoring.csv` | Every forecast made, and how accurate it turned out |
| `reports/results/*.csv` | Data quality, city summary, correlations, model metrics, best models, feature importance |
| `reports/summary_report.md` | Automatically written summary of all results, including the latest bulletin |
| `models/` | Best saved Spark model for every target and horizon |
| `logs/` | One log file per run |

## Data

Synthetic daily data for **Lahore, Faisalabad, Multan, Peshawar, Islamabad and Karachi**, from **1 Jan 2023 to yesterday**. It is calibrated to real public facts:
- IQAir annual PM2.5 averages for 2023–2025
- the smog season
- rice and wheat stubble burning
- monsoon washout
- Karachi's sea breeze

AQI uses the official US-EPA formula, and visibility uses the Koschmieder formula. Dirty records (missing values, sensor errors, misspelled names, duplicates) are added on purpose, so the cleaning step has real problems to solve. Every column is explained in `data/DATA_DICTIONARY.md`.

> The data is synthetic. It is meant for learning and demonstration, not for real health decisions.

## Evaluation

- **Regression:** RMSE, MAE and R² on the test period (1 Oct 2025 onwards, which includes two smog seasons)
- **Classification:** accuracy, weighted F1, precision and recall, plus a confusion matrix
- Every model is compared with a **baseline** that predicts tomorrow will be the same as today.

## Objectives

1. Build a scalable, automated data cleaning and feature engineering pipeline using PySpark.
2. Identify the factors that most strongly influence air quality.
3. Compare multiple regression models and select the best performer, using cross-validation and hyperparameter tuning.
4. Turn forecasts into early warnings and actions: the problem-solving output.
5. Package the work as a reproducible, well-documented GitHub project.

## Tech stack

Python, PySpark (DataFrame API and MLlib), pandas, NumPy, Matplotlib, YAML, Windows Task Scheduler, Git/GitHub
