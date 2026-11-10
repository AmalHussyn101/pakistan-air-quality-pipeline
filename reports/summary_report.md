# Pakistan Smog Early-Warning Pipeline (PySpark)

_Report generated automatically by `run_pipeline.py` on 2026-11-10 05:14._

> The data is synthetic, calibrated to IQAir annual PM2.5 averages (2023-2025) and known seasonal smog, crop-burning and monsoon patterns in Pakistan.

## 1. Data quality (cleaning step)

| check | count |
|---|---|
| Raw rows | 8484 |
| Badly written city names fixed | 20 |
| Duplicate rows removed | 30 |
| Impossible values in humidity_pct | 10 |
| Impossible values in pm25 | 15 |
| Missing values filled in temperature_c | 169 |
| Missing values filled in humidity_pct | 179 |
| Missing values filled in wind_speed_kmh | 169 |
| Missing values filled in rainfall_mm | 0 |
| Missing values filled in pm25 | 184 |
| Missing values filled in pm10 | 169 |
| Missing values filled in co2_ppm | 0 |
| Missing values filled in no2_ppb | 169 |
| Missing values filled in so2_ppb | 169 |
| Missing values filled in co_ppm | 169 |
| Missing values filled in ozone_ppb | 169 |
| Clean rows | 8454 |

## 2. City overview

| city | avg_aqi | avg_pm25 | avg_visibility_km | max_aqi | severe_days | hazardous_days |
|---|---|---|---|---|---|---|
| Lahore | 169.2 | 91.6 | 9.73 | 500 | 201 | 117 |
| Faisalabad | 162.9 | 84.1 | 10.45 | 500 | 155 | 109 |
| Multan | 161.9 | 79.4 | 11.16 | 500 | 164 | 94 |
| Peshawar | 154.6 | 74.7 | 11.07 | 500 | 162 | 79 |
| Karachi | 115.4 | 46.1 | 12.41 | 500 | 34 | 6 |
| Islamabad | 106.2 | 41.9 | 14.54 | 288 | 38 | 0 |

**Insight:** Lahore has the worst air (average AQI 169.2, 117 hazardous days). Karachi and Islamabad are the cleanest.

## 3. What drives AQI?

Correlation of each measured column with AQI (closer to +1 or -1 = stronger link):

| feature | correlation_with_aqi |
|---|---|
| pm25 | 0.964 |
| pm10 | 0.957 |
| co_ppm | 0.943 |
| visibility_km | -0.797 |
| ozone_ppb | -0.62 |
| temperature_c | -0.603 |
| crop_burning_index | 0.551 |
| wind_speed_kmh | -0.456 |
| so2_ppb | 0.44 |
| no2_ppb | 0.397 |

Top 10 features used by the Random Forest for next-day AQI:

| feature | importance |
|---|---|
| pm25 | 0.33216 |
| aqi | 0.1789 |
| aqi_avg3 | 0.11327 |
| co_ppm | 0.0953 |
| pm10 | 0.05718 |
| aqi_avg7 | 0.05439 |
| pm25_lag1 | 0.03226 |
| pm25_avg7 | 0.02811 |
| aqi_max7 | 0.02461 |
| visibility_km | 0.01238 |

## 4. Model results (test period: 2025-10-01 onwards)

**Baseline** = 'the value will be the same as today'. A model is useful only if it beats it.

### Model 1: AQI and visibility (regression)

| target | horizon_days | model | rmse | mae | r2 | best_params |
|---|---|---|---|---|---|---|
| aqi | 1 | Baseline (same as today) | 41.271 | 24.967 | 0.787 | - |
| aqi | 1 | Linear Regression | 35.808 | 22.591 | 0.84 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 1 | Random Forest | 37.347 | 23.474 | 0.826 | numTrees=60, maxDepth=6 |
| aqi | 1 | Gradient-Boosted Trees | 38.581 | 24.233 | 0.814 | maxIter=40, maxDepth=3 |
| aqi | 2 | Baseline (same as today) | 48.538 | 29.924 | 0.706 | - |
| aqi | 2 | Linear Regression | 40.877 | 26.323 | 0.792 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 2 | Random Forest | 42.927 | 27.52 | 0.77 | numTrees=60, maxDepth=6 |
| aqi | 2 | Gradient-Boosted Trees | 42.85 | 27.639 | 0.771 | maxIter=40, maxDepth=3 |
| aqi | 3 | Baseline (same as today) | 53.412 | 32.675 | 0.645 | - |
| aqi | 3 | Linear Regression | 43.879 | 28.556 | 0.76 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 3 | Random Forest | 45.35 | 29.02 | 0.744 | numTrees=60, maxDepth=6 |
| aqi | 3 | Gradient-Boosted Trees | 44.521 | 28.6 | 0.753 | maxIter=40, maxDepth=3 |
| visibility_km | 1 | Baseline (same as today) | 2.868 | 1.942 | 0.786 | - |
| visibility_km | 1 | Linear Regression | 2.487 | 1.856 | 0.839 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 1 | Random Forest | 2.508 | 1.917 | 0.837 | numTrees=60, maxDepth=6 |
| visibility_km | 1 | Gradient-Boosted Trees | 2.561 | 1.936 | 0.83 | maxIter=40, maxDepth=3 |
| visibility_km | 2 | Baseline (same as today) | 3.344 | 2.334 | 0.71 | - |
| visibility_km | 2 | Linear Regression | 2.772 | 2.116 | 0.801 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 2 | Random Forest | 2.814 | 2.201 | 0.795 | numTrees=60, maxDepth=6 |
| visibility_km | 2 | Gradient-Boosted Trees | 2.882 | 2.216 | 0.785 | maxIter=40, maxDepth=3 |
| visibility_km | 3 | Baseline (same as today) | 3.587 | 2.51 | 0.666 | - |
| visibility_km | 3 | Linear Regression | 2.9 | 2.254 | 0.782 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 3 | Random Forest | 2.913 | 2.295 | 0.78 | numTrees=60, maxDepth=6 |
| visibility_km | 3 | Gradient-Boosted Trees | 3.005 | 2.316 | 0.766 | maxIter=40, maxDepth=3 |

**Best model per target:**

| target | horizon_days | best_model | best_rmse | baseline_rmse | error_reduction_vs_baseline |
|---|---|---|---|---|---|
| aqi | 1 | Linear Regression | 35.808 | 41.271 | 13.2% |
| aqi | 2 | Linear Regression | 40.877 | 48.538 | 15.8% |
| aqi | 3 | Linear Regression | 43.879 | 53.412 | 17.8% |
| visibility_km | 1 | Linear Regression | 2.487 | 2.868 | 13.3% |
| visibility_km | 2 | Linear Regression | 2.772 | 3.344 | 17.1% |
| visibility_km | 3 | Linear Regression | 2.9 | 3.587 | 19.2% |

### Model 2: smog level (classification)

| horizon_days | model | accuracy | f1 | weightedPrecision | weightedRecall |
|---|---|---|---|---|---|
| 1 | Baseline (same as today) | 0.798 | 0.798 | 0.798 | 0.798 |
| 1 | Logistic Regression | 0.815 | 0.815 | 0.818 | 0.815 |
| 1 | Random Forest Classifier | 0.813 | 0.813 | 0.815 | 0.813 |
| 2 | Baseline (same as today) | 0.765 | 0.764 | 0.764 | 0.765 |
| 2 | Logistic Regression | 0.782 | 0.78 | 0.78 | 0.782 |
| 2 | Random Forest Classifier | 0.784 | 0.782 | 0.782 | 0.784 |
| 3 | Baseline (same as today) | 0.745 | 0.744 | 0.744 | 0.745 |
| 3 | Logistic Regression | 0.771 | 0.769 | 0.767 | 0.771 |
| 3 | Random Forest Classifier | 0.769 | 0.765 | 0.768 | 0.769 |

## 5. Latest forecast (next 3 days)

| city | forecast_date | days_ahead | predicted_aqi | predicted_visibility_km | predicted_smog_level | health_advice |
|---|---|---|---|---|---|---|
| Faisalabad | 2026-11-10 | 1 | 441.9 | 1.2 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Faisalabad | 2026-11-11 | 2 | 425.6 | 1.29 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Faisalabad | 2026-11-12 | 3 | 426.5 | 1.35 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Islamabad | 2026-11-10 | 1 | 186.5 | 7.65 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Islamabad | 2026-11-11 | 2 | 192.0 | 7.84 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Islamabad | 2026-11-12 | 3 | 200.8 | 7.75 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Karachi | 2026-11-10 | 1 | 180.8 | 5.77 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Karachi | 2026-11-11 | 2 | 183.5 | 5.53 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Karachi | 2026-11-12 | 3 | 184.6 | 6.04 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Lahore | 2026-11-10 | 1 | 462.3 | 0.05 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Lahore | 2026-11-11 | 2 | 443.2 | 0.05 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Lahore | 2026-11-12 | 3 | 437.9 | 0.05 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Multan | 2026-11-10 | 1 | 353.9 | 1.96 | Severe | Very unhealthy. Wear a mask, avoid outdoor exercise, keep windows closed. |
| Multan | 2026-11-11 | 2 | 357.3 | 1.94 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Multan | 2026-11-12 | 3 | 365.5 | 2.04 | Severe | Very unhealthy. Wear a mask, avoid outdoor exercise, keep windows closed. |
| Peshawar | 2026-11-10 | 1 | 407.3 | 1.65 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Peshawar | 2026-11-11 | 2 | 381.9 | 2.53 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Peshawar | 2026-11-12 | 3 | 381.0 | 2.66 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |

## 6. Smog early-warning bulletin (latest)

```
==============================================================================
SMOG EARLY-WARNING BULLETIN (automated)
Based on data up to: 2026-11-09   |   Outlook: next 3 days
Generated by the Pakistan Air Quality PySpark pipeline (synthetic data, for demonstration)
==============================================================================

SUMMARY: 4 city(ies) Hazardous, 0 city(ies) Severe, 2 city(ies) Moderate, 0 city(ies) Low

--- LAHORE : HAZARDOUS (max AQI 462, trend falling) ---
Outlook   : 2026-11-10: AQI 462, Hazardous | 2026-11-11: AQI 443, Hazardous | 2026-11-12: AQI 438, Hazardous
Visibility: lowest 0.05 km
Public    : Stay indoors. Seek medical help for breathing difficulty or chest pain.
Actions   :
  - Education department: close schools or move classes online.
  - Industries department: stop brick kilns and smoke-emitting factories.
  - Environment department: strictly enforce the ban on crop-residue burning.
  - Traffic police: restrict heavy vehicles and smoke-emitting vehicles.
  - Hospitals: activate smog emergency counters.
  - Motorway police: low-visibility warning (down to 0.1 km), reduce speed limits and consider night closures.

--- FAISALABAD : HAZARDOUS (max AQI 442, trend falling) ---
Outlook   : 2026-11-10: AQI 442, Hazardous | 2026-11-11: AQI 426, Hazardous | 2026-11-12: AQI 426, Hazardous
Visibility: lowest 1.2 km
Public    : Stay indoors. Seek medical help for breathing difficulty or chest pain.
Actions   :
  - Education department: close schools or move classes online.
  - Industries department: stop brick kilns and smoke-emitting factories.
  - Environment department: strictly enforce the ban on crop-residue burning.
  - Traffic police: restrict heavy vehicles and smoke-emitting vehicles.
  - Hospitals: activate smog emergency counters.
  - Traffic police: driver advisory to use fog lights and keep distance.

--- PESHAWAR : HAZARDOUS (max AQI 407, trend falling) ---
Outlook   : 2026-11-10: AQI 407, Hazardous | 2026-11-11: AQI 382, Hazardous | 2026-11-12: AQI 381, Hazardous
Visibility: lowest 1.65 km
Public    : Stay indoors. Seek medical help for breathing difficulty or chest pain.
Actions   :
  - Education department: close schools or move classes online.
  - Industries department: stop brick kilns and smoke-emitting factories.
  - Environment department: strictly enforce the ban on crop-residue burning.
  - Traffic police: restrict heavy vehicles and smoke-emitting vehicles.
  - Hospitals: activate smog emergency counters.
  - Traffic police: driver advisory to use fog lights and keep distance.

--- MULTAN : HAZARDOUS (max AQI 366, trend rising) ---
Outlook   : 2026-11-10: AQI 354, Severe | 2026-11-11: AQI 357, Hazardous | 2026-11-12: AQI 366, Severe
Visibility: lowest 1.94 km
Public    : Stay indoors. Seek medical help for breathing difficulty or chest pain.
Actions   :
  - Education department: close schools or move classes online.
  - Industries department: stop brick kilns and smoke-emitting factories.
  - Environment department: strictly enforce the ban on crop-residue burning.
  - Traffic police: restrict heavy vehicles and smoke-emitting vehicles.
  - Hospitals: activate smog emergency counters.
  - Traffic police: driver advisory to use fog lights and keep distance.

--- ISLAMABAD : MODERATE (max AQI 201, trend rising) ---
Outlook   : 2026-11-10: AQI 186, Moderate | 2026-11-11: AQI 192, Moderate | 2026-11-12: AQI 201, Moderate
Visibility: lowest 7.65 km
Public    : Children, elderly and heart/lung patients should limit time outdoors.
Actions   :
  - Health department: issue advisory for sensitive groups.
  - Environment department: monitor crop-burning hotspots.

--- KARACHI : MODERATE (max AQI 185, trend steady) ---
Outlook   : 2026-11-10: AQI 181, Moderate | 2026-11-11: AQI 184, Moderate | 2026-11-12: AQI 185, Moderate
Visibility: lowest 5.53 km
Public    : Children, elderly and heart/lung patients should limit time outdoors.
Actions   :
  - Health department: issue advisory for sensitive groups.
  - Environment department: monitor crop-burning hotspots.

AQI guide: 0-150 Low | 151-200 Moderate | 201-300 Severe | 301-500 Hazardous
```

## 7. Charts

![monthly_aqi_heatmap.png](figures/monthly_aqi_heatmap.png)

![model_comparison_rmse.png](figures/model_comparison_rmse.png)

![smog_accuracy_by_horizon.png](figures/smog_accuracy_by_horizon.png)

![actual_vs_predicted_aqi.png](figures/actual_vs_predicted_aqi.png)

![feature_importance_aqi.png](figures/feature_importance_aqi.png)

![smog_confusion_matrix.png](figures/smog_confusion_matrix.png)
