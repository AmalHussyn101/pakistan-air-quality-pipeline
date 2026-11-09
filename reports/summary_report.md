# Pakistan Air Quality and Smog Forecasting Pipeline

_Report generated automatically by `run_pipeline.py` on 2026-11-10 02:11._

> The data is synthetic, calibrated to IQAir annual PM2.5 averages (2023-2025) and known seasonal smog, crop-burning and monsoon patterns in Pakistan.

## 1. Data quality (cleaning step)

| check | count |
|---|---|
| Raw rows | 8478 |
| Badly written city names fixed | 20 |
| Duplicate rows removed | 30 |
| Impossible values in humidity_pct | 10 |
| Impossible values in pm25 | 15 |
| Missing values filled in temperature_c | 168 |
| Missing values filled in humidity_pct | 178 |
| Missing values filled in wind_speed_kmh | 168 |
| Missing values filled in rainfall_mm | 0 |
| Missing values filled in pm25 | 182 |
| Missing values filled in pm10 | 168 |
| Missing values filled in co2_ppm | 0 |
| Missing values filled in no2_ppb | 168 |
| Missing values filled in so2_ppb | 168 |
| Missing values filled in co_ppm | 168 |
| Missing values filled in ozone_ppb | 168 |
| Clean rows | 8448 |

## 2. City overview

| city | avg_aqi | avg_pm25 | avg_visibility_km | max_aqi | severe_days | hazardous_days |
|---|---|---|---|---|---|---|
| Lahore | 168.9 | 91.2 | 9.74 | 500 | 201 | 116 |
| Faisalabad | 162.7 | 83.8 | 10.46 | 500 | 155 | 108 |
| Multan | 161.8 | 79.4 | 11.16 | 500 | 164 | 93 |
| Peshawar | 154.3 | 74.5 | 11.08 | 500 | 162 | 78 |
| Karachi | 115.4 | 46.1 | 12.42 | 500 | 34 | 6 |
| Islamabad | 106.2 | 41.8 | 14.55 | 288 | 38 | 0 |

**Insight:** Lahore has the worst air (average AQI 168.9, 116 hazardous days). Karachi and Islamabad are the cleanest.

## 3. What drives AQI?

Correlation of each measured column with AQI (closer to +1 or -1 = stronger link):

| feature | correlation_with_aqi |
|---|---|
| pm25 | 0.965 |
| pm10 | 0.958 |
| co_ppm | 0.943 |
| visibility_km | -0.797 |
| ozone_ppb | -0.618 |
| temperature_c | -0.603 |
| crop_burning_index | 0.547 |
| wind_speed_kmh | -0.457 |
| so2_ppb | 0.442 |
| no2_ppb | 0.395 |

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
| aqi | 1 | Linear Regression | 35.799 | 22.578 | 0.84 | regParam=0.1, elasticNetParam=0.0 |
| aqi | 1 | Random Forest | 37.347 | 23.474 | 0.826 | numTrees=60, maxDepth=6 |
| aqi | 1 | Gradient-Boosted Trees | 38.581 | 24.233 | 0.814 | maxIter=40, maxDepth=3 |
| aqi | 2 | Baseline (same as today) | 48.538 | 29.924 | 0.706 | - |
| aqi | 2 | Linear Regression | 40.865 | 26.306 | 0.792 | regParam=0.1, elasticNetParam=0.0 |
| aqi | 2 | Random Forest | 43.509 | 27.675 | 0.764 | numTrees=60, maxDepth=10 |
| aqi | 2 | Gradient-Boosted Trees | 42.85 | 27.639 | 0.771 | maxIter=40, maxDepth=3 |
| aqi | 3 | Baseline (same as today) | 53.412 | 32.675 | 0.645 | - |
| aqi | 3 | Linear Regression | 43.876 | 28.55 | 0.76 | regParam=0.1, elasticNetParam=0.0 |
| aqi | 3 | Random Forest | 45.544 | 29.205 | 0.742 | numTrees=60, maxDepth=10 |
| aqi | 3 | Gradient-Boosted Trees | 44.521 | 28.6 | 0.753 | maxIter=40, maxDepth=3 |
| visibility_km | 1 | Baseline (same as today) | 2.868 | 1.942 | 0.786 | - |
| visibility_km | 1 | Linear Regression | 2.487 | 1.856 | 0.839 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 1 | Random Forest | 2.469 | 1.868 | 0.842 | numTrees=60, maxDepth=10 |
| visibility_km | 1 | Gradient-Boosted Trees | 2.561 | 1.936 | 0.83 | maxIter=40, maxDepth=3 |
| visibility_km | 2 | Baseline (same as today) | 3.344 | 2.334 | 0.71 | - |
| visibility_km | 2 | Linear Regression | 2.781 | 2.134 | 0.799 | regParam=0.1, elasticNetParam=0.0 |
| visibility_km | 2 | Random Forest | 2.799 | 2.176 | 0.797 | numTrees=60, maxDepth=10 |
| visibility_km | 2 | Gradient-Boosted Trees | 2.882 | 2.216 | 0.785 | maxIter=40, maxDepth=3 |
| visibility_km | 3 | Baseline (same as today) | 3.587 | 2.51 | 0.666 | - |
| visibility_km | 3 | Linear Regression | 2.9 | 2.254 | 0.782 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 3 | Random Forest | 2.905 | 2.272 | 0.781 | numTrees=60, maxDepth=10 |
| visibility_km | 3 | Gradient-Boosted Trees | 3.005 | 2.316 | 0.766 | maxIter=40, maxDepth=3 |

**Best model per target:**

| target | horizon_days | best_model | best_rmse | baseline_rmse | error_reduction_vs_baseline |
|---|---|---|---|---|---|
| aqi | 1 | Linear Regression | 35.799 | 41.271 | 13.3% |
| aqi | 2 | Linear Regression | 40.865 | 48.538 | 15.8% |
| aqi | 3 | Linear Regression | 43.876 | 53.412 | 17.9% |
| visibility_km | 1 | Random Forest | 2.469 | 2.868 | 13.9% |
| visibility_km | 2 | Linear Regression | 2.781 | 3.344 | 16.8% |
| visibility_km | 3 | Linear Regression | 2.9 | 3.587 | 19.2% |

### Model 2: smog level (classification)

| horizon_days | model | accuracy | f1 | weightedPrecision | weightedRecall |
|---|---|---|---|---|---|
| 1 | Baseline (same as today) | 0.798 | 0.798 | 0.798 | 0.798 |
| 1 | Logistic Regression | 0.815 | 0.815 | 0.818 | 0.815 |
| 1 | Random Forest Classifier | 0.811 | 0.811 | 0.812 | 0.811 |
| 2 | Baseline (same as today) | 0.765 | 0.764 | 0.764 | 0.765 |
| 2 | Logistic Regression | 0.782 | 0.78 | 0.78 | 0.782 |
| 2 | Random Forest Classifier | 0.776 | 0.774 | 0.773 | 0.776 |
| 3 | Baseline (same as today) | 0.745 | 0.744 | 0.744 | 0.745 |
| 3 | Logistic Regression | 0.771 | 0.769 | 0.767 | 0.771 |
| 3 | Random Forest Classifier | 0.772 | 0.768 | 0.769 | 0.772 |

## 5. Latest forecast (next 3 days)

| city | forecast_date | days_ahead | predicted_aqi | predicted_visibility_km | predicted_smog_level | health_advice |
|---|---|---|---|---|---|---|
| Faisalabad | 2026-11-09 | 1 | 436.1 | 2.42 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Faisalabad | 2026-11-10 | 2 | 433.1 | 0.69 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Faisalabad | 2026-11-11 | 3 | 413.3 | 1.03 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Islamabad | 2026-11-09 | 1 | 156.0 | 9.65 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Islamabad | 2026-11-10 | 2 | 177.8 | 8.59 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Islamabad | 2026-11-11 | 3 | 186.4 | 8.25 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Karachi | 2026-11-09 | 1 | 172.4 | 7.04 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Karachi | 2026-11-10 | 2 | 179.6 | 6.3 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Karachi | 2026-11-11 | 3 | 181.1 | 6.3 | Moderate | Unhealthy. Children, elderly and patients should limit time outdoors. |
| Lahore | 2026-11-09 | 1 | 366.3 | 2.33 | Severe | Very unhealthy. Wear a mask, avoid outdoor exercise, keep windows closed. |
| Lahore | 2026-11-10 | 2 | 380.5 | 0.6 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Lahore | 2026-11-11 | 3 | 389.9 | 0.05 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Multan | 2026-11-09 | 1 | 351.2 | 3.28 | Severe | Very unhealthy. Wear a mask, avoid outdoor exercise, keep windows closed. |
| Multan | 2026-11-10 | 2 | 366.0 | 2.28 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Multan | 2026-11-11 | 3 | 376.0 | 1.81 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Peshawar | 2026-11-09 | 1 | 381.7 | 2.5 | Severe | Very unhealthy. Wear a mask, avoid outdoor exercise, keep windows closed. |
| Peshawar | 2026-11-10 | 2 | 364.3 | 1.71 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |
| Peshawar | 2026-11-11 | 3 | 365.0 | 2.06 | Hazardous | Emergency level. Stay indoors; schools and outdoor work should pause. |

## 6. Charts

![monthly_aqi_heatmap.png](figures/monthly_aqi_heatmap.png)

![model_comparison_rmse.png](figures/model_comparison_rmse.png)

![smog_accuracy_by_horizon.png](figures/smog_accuracy_by_horizon.png)

![actual_vs_predicted_aqi.png](figures/actual_vs_predicted_aqi.png)

![feature_importance_aqi.png](figures/feature_importance_aqi.png)

![smog_confusion_matrix.png](figures/smog_confusion_matrix.png)
