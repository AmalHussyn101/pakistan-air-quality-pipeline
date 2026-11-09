# Pakistan Air Quality & Smog Forecasting Pipeline (PySpark)

An end-to-end, **automated** PySpark machine learning pipeline that forecasts a city's **AQI**, **visibility (km)** and **smog level** (Low / Moderate / Severe / Hazardous) for the **next 1, 2 and 3 days** in six Pakistani cities.

## The problem

Every winter, smog in Punjab and KPK reaches hazardous levels. In November 2025, for example, Lahore's citywide AQI reached about 475. Schools, hospitals, traffic police and families need to know **before** a bad day arrives, so they can close schools, warn patients, plan traffic and stop crop burning.

This project builds a pipeline that:
1. cleans messy sensor data,
2. learns how weather, crop burning, traffic and industry drive pollution,
3. forecasts the next 3 days for every city, and
4. turns the forecast into simple health advice.

## Data

Synthetic daily data is generated in code (`src/generate_data.py`) for **Lahore, Faisalabad, Multan, Peshawar, Islamabad and Karachi**, from **1 Jan 2023 to 8 Nov 2026** (8,478 rows, 19 columns).

The data is calibrated to real public facts:
- IQAir annual PM2.5 averages for 2023–2025
- the smog season (late October to February)
- rice and wheat stubble burning
- monsoon washout in July and August
- Karachi's sea breeze

AQI uses the official US-EPA formula, and visibility uses the Koschmieder formula. Missing values, sensor errors, misspelled city names and duplicates are added on purpose, so the cleaning step has real problems to solve. Every column is explained in `data/DATA_DICTIONARY.md`.

> The data is synthetic. It is meant for learning and for demonstrating the pipeline, not for real health decisions.

## Pipeline

```
generate -> clean -> features -> train -> forecast -> evaluate
```

| Step | File | What it does | Technology |
|---|---|---|---|
| 1. Generate | `src/generate_data.py` | Creates the synthetic CSV (skipped if it already exists) | NumPy, pandas |
| 2. Clean | `src/clean.py` | Fixes city names, removes duplicates, turns impossible values into missing, fills gaps from neighbouring days, makes insight tables | **PySpark DataFrame API, Window functions** |
| 3. Features | `src/features.py` | Season, crop-burning season, smog season, yesterday's pollution (lags), 3- and 7-day averages, AQI change, plus targets for +1/+2/+3 days | **PySpark Window functions** |
| 4. Train | `src/train.py` | **Model 1 (regression):** AQI and visibility with Linear Regression, Random Forest and Gradient-Boosted Trees. **Model 2 (classification):** smog level with Logistic Regression and Random Forest. Uses a time-based split, k-fold cross-validation, hyperparameter tuning and a "same as today" baseline. The best model is saved. | **PySpark MLlib** (Pipeline, CrossValidator) |
| 5. Forecast | `src/forecast.py` | Loads the saved best models and forecasts the next 3 days for each city, with health advice | **PySpark MLlib** |
| 6. Evaluate | `src/evaluate.py` | Makes PNG charts and a written summary report | Matplotlib |

Everything is controlled by **`config/config.yaml`**, and every run writes a log file in **`logs/`**.

## How to run

```bash
# 1. activate the virtual environment (Windows)
venv\Scripts\activate

# 2. fast test run (no cross-validation, ~3-5 minutes)
python run_pipeline.py --quick

# 3. full run (with 3-fold cross-validation and tuning, ~15-30 minutes)
python run_pipeline.py

# run only some steps
python run_pipeline.py --steps clean,features
```

Requirements: Python 3.10+, Java 17, and `pip install -r requirements.txt`. On Windows you also need Hadoop `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`, with `HADOOP_HOME=C:\hadoop` set.

## Outputs

| Folder / file | Contents |
|---|---|
| `data/processed/` | Clean data and the feature table (Parquet, written by Spark) |
| `models/` | Best saved Spark model for every target and horizon |
| `reports/results/` | CSVs: data quality, city summary, monthly summary, correlations, regression and classification metrics, best models, test predictions, feature importance, **latest 3-day forecast** |
| `reports/figures/` | PNG charts: monthly AQI heatmap, model comparison, smog accuracy, actual vs predicted, feature importance, confusion matrix |
| `reports/summary_report.md` | Automatically written summary of all results |
| `logs/` | One log file per run (what happened in each step, how long it took, any errors) |

## Evaluation

- **Regression:** RMSE, MAE and R² on the test period (1 Oct 2025 onwards, which includes two smog seasons)
- **Classification:** accuracy, weighted F1, precision and recall, plus a confusion matrix
- Every model is compared with a **baseline** that predicts tomorrow will be the same as today. A model is only useful if it beats this baseline.

## Project structure

```
pakistan-air-quality-pipeline/
├── config/config.yaml        # all settings
├── data/
│   ├── raw/                  # synthetic CSV
│   ├── processed/            # Spark Parquet outputs
│   └── DATA_DICTIONARY.md    # every column explained
├── src/
│   ├── generate_data.py      # step 1
│   ├── clean.py              # step 2
│   ├── features.py           # step 3
│   ├── train.py              # step 4
│   ├── forecast.py           # step 5
│   ├── evaluate.py           # step 6
│   └── utils.py              # config, logging, Spark session
├── models/                   # saved best models
├── reports/                  # figures, results, summary report
├── logs/                     # run logs
├── notebooks/                # data exploration
├── run_pipeline.py           # runs everything automatically
└── requirements.txt
```

## Objectives

1. Build a scalable data cleaning and feature engineering pipeline using PySpark.
2. Identify the factors that most strongly influence air quality (correlations and feature importance).
3. Compare multiple regression models and select the best performer, with cross-validation and hyperparameter tuning.
4. Package the work as a reproducible, well-documented GitHub project.

## Tech stack

Python, PySpark (DataFrame API and MLlib), pandas, NumPy, Matplotlib, Jupyter, YAML, Git/GitHub
