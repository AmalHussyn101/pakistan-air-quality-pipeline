# Pakistan Smog Early-Warning Pipeline (PySpark, live data)

_Report generated automatically by `run_pipeline.py` on 2026-11-10 15:03._

> Data: real hourly air-quality (CAMS, via Open-Meteo) and weather data for six Pakistani cities since 2023, aggregated to daily values. AQI is calculated with the US-EPA formula.

## 1. Data quality (cleaning step)

| check | count |
|---|---|
| Rows loaded | 8502 |
| Duplicate rows removed | 0 |
| Impossible values in temperature_c | 0 |
| Impossible values in humidity_pct | 0 |
| Impossible values in wind_speed_kmh | 0 |
| Impossible values in rainfall_mm | 0 |
| Impossible values in pm25 | 0 |
| Impossible values in pm10 | 0 |
| Impossible values in no2_ugm3 | 0 |
| Impossible values in so2_ugm3 | 0 |
| Impossible values in co_ugm3 | 0 |
| Impossible values in ozone_ugm3 | 0 |
| Impossible values in co2_ppm | 0 |
| Impossible values in dust_ugm3 | 0 |
| Missing values filled in pm25 | 0 |
| Missing values filled in pm10 | 0 |
| Missing values filled in no2_ugm3 | 0 |
| Missing values filled in so2_ugm3 | 0 |
| Missing values filled in co_ugm3 | 0 |
| Missing values filled in ozone_ugm3 | 0 |
| Missing values filled in co2_ppm | 3984 |
| Missing values filled in dust_ugm3 | 0 |
| Missing values filled in temperature_c | 0 |
| Missing values filled in humidity_pct | 0 |
| Missing values filled in wind_speed_kmh | 0 |
| Missing values filled in rainfall_mm | 0 |
| Clean past city-days | 8454 |
| Forecast-weather city-days | 48 |

## 2. City overview

| city | avg_aqi | avg_pm25 | avg_visibility_km | max_aqi | severe_days | hazardous_days |
|---|---|---|---|---|---|---|
| Faisalabad | 156.5 | 75.2 | 20.98 | 425.0 | 162 | 33 |
| Lahore | 148.2 | 68.0 | 20.91 | 472.0 | 134 | 11 |
| Multan | 142.3 | 64.8 | 21.16 | 335.0 | 127 | 6 |
| Islamabad | 106.1 | 37.6 | 21.69 | 190.0 | 0 | 0 |
| Peshawar | 104.4 | 36.6 | 22.18 | 176.0 | 0 | 0 |
| Karachi | 88.8 | 28.9 | 23.02 | 196.0 | 0 | 0 |

**Insight:** Faisalabad has the worst air (average AQI 156.5, 33 hazardous days). Karachi and Islamabad are the cleanest.

## 3. What drives AQI?

Correlation of each measured column with AQI (closer to +1 or -1 = stronger link):

| feature | correlation_with_aqi |
|---|---|
| pm25 | 0.958 |
| pm10 | 0.831 |
| co_ugm3 | 0.712 |
| so2_ugm3 | 0.638 |
| no2_ugm3 | 0.56 |
| wind_speed_kmh | -0.406 |
| temperature_c | -0.354 |
| humidity_pct | 0.188 |
| visibility_km | -0.188 |
| co2_ppm | 0.184 |

Top 10 features used by the Random Forest for next-day AQI:

| feature | importance |
|---|---|
| pm25_avg7 | 0.31313 |
| aqi_avg7 | 0.12662 |
| pm25 | 0.0979 |
| aqi_max7 | 0.09713 |
| aqi_avg3 | 0.09332 |
| aqi | 0.05483 |
| t_wind_speed_kmh | 0.04602 |
| t_humidity_pct | 0.02408 |
| t_doy_cos | 0.02009 |
| t_temperature_c | 0.01706 |

## 4. Model results (test period: 2025-10-01 onwards)

**Baseline** = 'the value will be the same as today'. A model is useful only if it beats it.

### Model 1: AQI and visibility (regression)

| target | horizon_days | model | rmse | mae | r2 | best_params |
|---|---|---|---|---|---|---|
| aqi | 1 | Baseline (same as today) | 22.3886 | 15.4563 | 0.7899 | - |
| aqi | 2 | Baseline (same as today) | 29.4861 | 20.7754 | 0.6357 | - |
| aqi | 3 | Baseline (same as today) | 31.8917 | 22.8445 | 0.5743 | - |
| aqi | 4 | Baseline (same as today) | 33.9332 | 24.6804 | 0.5185 | - |
| aqi | 5 | Baseline (same as today) | 35.4278 | 25.6217 | 0.4757 | - |
| aqi | 6 | Baseline (same as today) | 36.0652 | 26.2924 | 0.4569 | - |
| aqi | 7 | Baseline (same as today) | 36.4077 | 26.4276 | 0.447 | - |
| aqi | 1 | Linear Regression | 22.999 | 17.8681 | 0.7782 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 2 | Linear Regression | 25.4176 | 19.4682 | 0.7293 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 3 | Linear Regression | 26.2392 | 20.1057 | 0.7118 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 4 | Linear Regression | 26.9853 | 20.5211 | 0.6955 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 5 | Linear Regression | 27.6066 | 20.9833 | 0.6816 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 6 | Linear Regression | 27.8667 | 21.0823 | 0.6758 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 7 | Linear Regression | 28.105 | 21.2594 | 0.6705 | regParam=0.0, elasticNetParam=0.0 |
| aqi | 1 | Random Forest | 21.7677 | 16.3946 | 0.8013 | numTrees=50, maxDepth=8 |
| aqi | 2 | Random Forest | 24.0565 | 18.1396 | 0.7575 | numTrees=50, maxDepth=8 |
| aqi | 3 | Random Forest | 24.9181 | 18.931 | 0.7401 | numTrees=50, maxDepth=8 |
| aqi | 4 | Random Forest | 25.6226 | 19.5532 | 0.7254 | numTrees=50, maxDepth=8 |
| aqi | 5 | Random Forest | 26.1976 | 19.9644 | 0.7133 | numTrees=50, maxDepth=8 |
| aqi | 6 | Random Forest | 26.3221 | 20.2368 | 0.7107 | numTrees=50, maxDepth=8 |
| aqi | 7 | Random Forest | 26.3519 | 20.3025 | 0.7103 | numTrees=50, maxDepth=8 |
| aqi | 1 | Gradient-Boosted Trees | 21.3831 | 15.5811 | 0.8083 | maxIter=40, maxDepth=4 |
| aqi | 2 | Gradient-Boosted Trees | 23.9569 | 17.6076 | 0.7595 | maxIter=40, maxDepth=4 |
| aqi | 3 | Gradient-Boosted Trees | 24.6281 | 18.3985 | 0.7461 | maxIter=40, maxDepth=4 |
| aqi | 4 | Gradient-Boosted Trees | 25.065 | 18.6107 | 0.7373 | maxIter=40, maxDepth=4 |
| aqi | 5 | Gradient-Boosted Trees | 25.7258 | 19.197 | 0.7235 | maxIter=40, maxDepth=4 |
| aqi | 6 | Gradient-Boosted Trees | 25.7499 | 19.3574 | 0.7231 | maxIter=40, maxDepth=4 |
| aqi | 7 | Gradient-Boosted Trees | 26.1026 | 19.4918 | 0.7157 | maxIter=40, maxDepth=4 |
| visibility_km | 1 | Baseline (same as today) | 2.1145 | 1.1843 | 0.8866 | - |
| visibility_km | 2 | Baseline (same as today) | 2.6758 | 1.6174 | 0.8186 | - |
| visibility_km | 3 | Baseline (same as today) | 2.8841 | 1.7889 | 0.7897 | - |
| visibility_km | 4 | Baseline (same as today) | 2.8809 | 1.8498 | 0.7905 | - |
| visibility_km | 5 | Baseline (same as today) | 3.0125 | 1.9549 | 0.7714 | - |
| visibility_km | 6 | Baseline (same as today) | 3.097 | 2.0234 | 0.7585 | - |
| visibility_km | 7 | Baseline (same as today) | 3.108 | 2.0803 | 0.7571 | - |
| visibility_km | 1 | Linear Regression | 8.3606 | 6.878 | -0.7726 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 2 | Linear Regression | 8.3872 | 6.9114 | -0.7823 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 3 | Linear Regression | 8.404 | 6.9307 | -0.7859 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 4 | Linear Regression | 8.4133 | 6.9359 | -0.7866 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 5 | Linear Regression | 8.4266 | 6.9464 | -0.7886 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 6 | Linear Regression | 8.4307 | 6.9505 | -0.7895 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 7 | Linear Regression | 8.4387 | 6.9592 | -0.7909 | regParam=0.0, elasticNetParam=0.0 |
| visibility_km | 1 | Random Forest | 9.4298 | 7.6983 | -1.255 | numTrees=50, maxDepth=8 |
| visibility_km | 2 | Random Forest | 9.5389 | 7.8098 | -1.3054 | numTrees=50, maxDepth=8 |
| visibility_km | 3 | Random Forest | 9.5549 | 7.8236 | -1.3085 | numTrees=50, maxDepth=8 |
| visibility_km | 4 | Random Forest | 9.5804 | 7.8495 | -1.3166 | numTrees=50, maxDepth=8 |
| visibility_km | 5 | Random Forest | 9.6009 | 7.8682 | -1.3219 | numTrees=50, maxDepth=8 |
| visibility_km | 6 | Random Forest | 9.6036 | 7.87 | -1.3221 | numTrees=50, maxDepth=8 |
| visibility_km | 7 | Random Forest | 9.6118 | 7.8769 | -1.3234 | numTrees=50, maxDepth=8 |
| visibility_km | 1 | Gradient-Boosted Trees | 9.7103 | 7.9806 | -1.3911 | maxIter=40, maxDepth=4 |
| visibility_km | 2 | Gradient-Boosted Trees | 9.7175 | 7.9915 | -1.3925 | maxIter=40, maxDepth=4 |
| visibility_km | 3 | Gradient-Boosted Trees | 9.7242 | 7.997 | -1.3911 | maxIter=40, maxDepth=4 |
| visibility_km | 4 | Gradient-Boosted Trees | 9.734 | 8.0083 | -1.3915 | maxIter=40, maxDepth=4 |
| visibility_km | 5 | Gradient-Boosted Trees | 9.7405 | 8.0149 | -1.3899 | maxIter=40, maxDepth=4 |
| visibility_km | 6 | Gradient-Boosted Trees | 9.7429 | 8.0204 | -1.39 | maxIter=40, maxDepth=4 |
| visibility_km | 7 | Gradient-Boosted Trees | 9.7512 | 8.0278 | -1.3913 | maxIter=40, maxDepth=4 |

**Best model per target:**

| target | horizon_days | best_model | best_rmse | baseline_rmse | error_reduction_vs_baseline |
|---|---|---|---|---|---|
| aqi | 1 | Gradient-Boosted Trees | 21.3831 | 22.3886 | 4.5% |
| aqi | 2 | Gradient-Boosted Trees | 23.9569 | 29.4861 | 18.8% |
| aqi | 3 | Gradient-Boosted Trees | 24.6281 | 31.8917 | 22.8% |
| aqi | 4 | Gradient-Boosted Trees | 25.065 | 33.9332 | 26.1% |
| aqi | 5 | Gradient-Boosted Trees | 25.7258 | 35.4278 | 27.4% |
| aqi | 6 | Gradient-Boosted Trees | 25.7499 | 36.0652 | 28.6% |
| aqi | 7 | Gradient-Boosted Trees | 26.1026 | 36.4077 | 28.3% |
| visibility_km | 1 | Linear Regression | 8.3606 | 2.1145 | -295.4% |
| visibility_km | 2 | Linear Regression | 8.3872 | 2.6758 | -213.4% |
| visibility_km | 3 | Linear Regression | 8.404 | 2.8841 | -191.4% |
| visibility_km | 4 | Linear Regression | 8.4133 | 2.8809 | -192.0% |
| visibility_km | 5 | Linear Regression | 8.4266 | 3.0125 | -179.7% |
| visibility_km | 6 | Linear Regression | 8.4307 | 3.097 | -172.2% |
| visibility_km | 7 | Linear Regression | 8.4387 | 3.108 | -171.5% |

### Model 2: smog level (classification)

| horizon_days | model | accuracy | f1 | weightedPrecision | weightedRecall |
|---|---|---|---|---|---|
| 1 | Baseline (same as today) | 0.8267 | 0.8266 | 0.8264 | 0.8267 |
| 2 | Baseline (same as today) | 0.7701 | 0.7698 | 0.7695 | 0.7701 |
| 3 | Baseline (same as today) | 0.7454 | 0.745 | 0.7446 | 0.7454 |
| 4 | Baseline (same as today) | 0.7278 | 0.7273 | 0.7268 | 0.7278 |
| 5 | Baseline (same as today) | 0.7129 | 0.7122 | 0.7116 | 0.7129 |
| 6 | Baseline (same as today) | 0.7063 | 0.7054 | 0.7046 | 0.7063 |
| 7 | Baseline (same as today) | 0.706 | 0.7049 | 0.7038 | 0.706 |
| 1 | Logistic Regression | 0.7896 | 0.8005 | 0.8199 | 0.7896 |
| 2 | Logistic Regression | 0.7816 | 0.7918 | 0.8089 | 0.7816 |
| 3 | Logistic Regression | 0.7757 | 0.7851 | 0.8025 | 0.7757 |
| 4 | Logistic Regression | 0.771 | 0.7796 | 0.7962 | 0.771 |
| 5 | Logistic Regression | 0.765 | 0.7742 | 0.7899 | 0.765 |
| 6 | Logistic Regression | 0.7678 | 0.7748 | 0.7888 | 0.7678 |
| 7 | Logistic Regression | 0.7592 | 0.7653 | 0.78 | 0.7592 |
| 1 | Random Forest Classifier | 0.8148 | 0.8016 | 0.801 | 0.8148 |
| 2 | Random Forest Classifier | 0.8015 | 0.7888 | 0.7875 | 0.8015 |
| 3 | Random Forest Classifier | 0.7844 | 0.7702 | 0.7685 | 0.7844 |
| 4 | Random Forest Classifier | 0.7731 | 0.7578 | 0.7549 | 0.7731 |
| 5 | Random Forest Classifier | 0.7721 | 0.7564 | 0.7552 | 0.7721 |
| 6 | Random Forest Classifier | 0.7632 | 0.7468 | 0.7441 | 0.7632 |
| 7 | Random Forest Classifier | 0.7663 | 0.7499 | 0.7482 | 0.7663 |

## 5. Latest forecast (next 7 days)

| city | forecast_date | days_ahead | predicted_aqi | predicted_visibility_km | predicted_smog_level | health_advice |
|---|---|---|---|---|---|---|
| Faisalabad | 2026-11-10 | 1 | 299.0 | 21.33 | Hazardous | Stay indoors, close schools |
| Faisalabad | 2026-11-11 | 2 | 302.0 | 21.32 | Hazardous | Stay indoors, close schools |
| Faisalabad | 2026-11-12 | 3 | 237.0 | 21.37 | Hazardous | Stay indoors, close schools |
| Faisalabad | 2026-11-13 | 4 | 243.0 | 21.34 | Hazardous | Stay indoors, close schools |
| Faisalabad | 2026-11-14 | 5 | 243.0 | 21.33 | Hazardous | Stay indoors, close schools |
| Faisalabad | 2026-11-15 | 6 | 161.0 | 21.39 | Severe | Mask outdoors, avoid exercise |
| Faisalabad | 2026-11-16 | 7 | 156.0 | 21.43 | Hazardous | Stay indoors, close schools |
| Lahore | 2026-11-10 | 1 | 202.0 | 21.6 | Severe | Mask outdoors, avoid exercise |
| Lahore | 2026-11-11 | 2 | 196.0 | 21.62 | Severe | Mask outdoors, avoid exercise |
| Lahore | 2026-11-12 | 3 | 196.0 | 21.63 | Severe | Mask outdoors, avoid exercise |
| Lahore | 2026-11-13 | 4 | 182.0 | 21.69 | Moderate | Sensitive groups limit outdoors |
| Lahore | 2026-11-14 | 5 | 192.0 | 21.65 | Severe | Mask outdoors, avoid exercise |
| Lahore | 2026-11-15 | 6 | 189.0 | 21.71 | Moderate | Sensitive groups limit outdoors |
| Lahore | 2026-11-16 | 7 | 194.0 | 21.65 | Severe | Mask outdoors, avoid exercise |
| Multan | 2026-11-10 | 1 | 183.0 | 21.6 | Severe | Mask outdoors, avoid exercise |
| Multan | 2026-11-11 | 2 | 179.0 | 21.58 | Severe | Mask outdoors, avoid exercise |
| Multan | 2026-11-12 | 3 | 178.0 | 21.49 | Severe | Mask outdoors, avoid exercise |
| Multan | 2026-11-13 | 4 | 172.0 | 21.39 | Severe | Mask outdoors, avoid exercise |
| Multan | 2026-11-14 | 5 | 169.0 | 21.44 | Moderate | Sensitive groups limit outdoors |
| Multan | 2026-11-15 | 6 | 178.0 | 21.53 | Severe | Mask outdoors, avoid exercise |
| Multan | 2026-11-16 | 7 | 178.0 | 21.56 | Severe | Mask outdoors, avoid exercise |
| Peshawar | 2026-11-10 | 1 | 151.0 | 22.18 | Moderate | Sensitive groups limit outdoors |
| Peshawar | 2026-11-11 | 2 | 147.0 | 22.35 | Moderate | Sensitive groups limit outdoors |
| Peshawar | 2026-11-12 | 3 | 136.0 | 21.97 | Moderate | Sensitive groups limit outdoors |
| Peshawar | 2026-11-13 | 4 | 135.0 | 22.17 | Moderate | Sensitive groups limit outdoors |
| Peshawar | 2026-11-14 | 5 | 136.0 | 22.28 | Low | Air acceptable |
| Peshawar | 2026-11-15 | 6 | 128.0 | 22.48 | Low | Air acceptable |
| Peshawar | 2026-11-16 | 7 | 128.0 | 22.49 | Low | Air acceptable |
| Islamabad | 2026-11-10 | 1 | 150.0 | 22.09 | Moderate | Sensitive groups limit outdoors |
| Islamabad | 2026-11-11 | 2 | 146.0 | 21.82 | Moderate | Sensitive groups limit outdoors |
| Islamabad | 2026-11-12 | 3 | 126.0 | 19.47 | Low | Air acceptable |
| Islamabad | 2026-11-13 | 4 | 128.0 | 21.57 | Low | Air acceptable |
| Islamabad | 2026-11-14 | 5 | 132.0 | 21.95 | Low | Air acceptable |
| Islamabad | 2026-11-15 | 6 | 130.0 | 22.1 | Low | Air acceptable |
| Islamabad | 2026-11-16 | 7 | 130.0 | 22.11 | Low | Air acceptable |
| Karachi | 2026-11-10 | 1 | 94.0 | 22.66 | Low | Air acceptable |
| Karachi | 2026-11-11 | 2 | 90.0 | 22.61 | Low | Air acceptable |
| Karachi | 2026-11-12 | 3 | 93.0 | 22.63 | Low | Air acceptable |
| Karachi | 2026-11-13 | 4 | 94.0 | 22.64 | Low | Air acceptable |
| Karachi | 2026-11-14 | 5 | 97.0 | 22.64 | Low | Air acceptable |
| Karachi | 2026-11-15 | 6 | 95.0 | 22.67 | Low | Air acceptable |
| Karachi | 2026-11-16 | 7 | 99.0 | 22.75 | Low | Air acceptable |

## 6. Smog early-warning bulletin (latest)

```
==============================================================================
SMOG EARLY-WARNING BULLETIN (automated)
Based on data up to: 2026-11-09   |   Outlook: next 7 days
Generated automatically by the PySpark pipeline from real Open-Meteo (CAMS) air-quality and weather data
==============================================================================

SUMMARY: 1 city(ies) Hazardous, 2 city(ies) Severe, 2 city(ies) Moderate, 1 city(ies) Low

--- FAISALABAD : HAZARDOUS (max AQI 302, trend falling) ---
Outlook   : 2026-11-10: AQI 299, Hazardous
            2026-11-11: AQI 302, Hazardous
            2026-11-12: AQI 237, Hazardous
            2026-11-13: AQI 243, Hazardous
            2026-11-14: AQI 243, Hazardous
            2026-11-15: AQI 161, Severe
            2026-11-16: AQI 156, Hazardous
Visibility: lowest 21.32 km
Public    : Stay indoors. Seek medical help for breathing difficulty or chest pain.
Actions   :
  - Education department: close schools or move classes online.
  - Industries department: stop brick kilns and smoke-emitting factories.
  - Environment department: strictly enforce the ban on crop-residue burning.
  - Traffic police: restrict heavy vehicles and smoke-emitting vehicles.
  - Hospitals: activate smog emergency counters.

--- LAHORE : SEVERE (max AQI 202, trend steady) ---
Outlook   : 2026-11-10: AQI 202, Severe
            2026-11-11: AQI 196, Severe
            2026-11-12: AQI 196, Severe
            2026-11-13: AQI 182, Moderate
            2026-11-14: AQI 192, Severe
            2026-11-15: AQI 189, Moderate
            2026-11-16: AQI 194, Severe
Visibility: lowest 21.6 km
Public    : Wear an N95 mask outdoors. Avoid outdoor exercise. Keep windows closed.
Actions   :
  - Education department: cancel outdoor school activities and sports.
  - Environment department: increase patrols against crop-residue burning.
  - City administration: water sprinkling on main roads to settle dust.
  - Hospitals: prepare for more respiratory patients.

--- MULTAN : SEVERE (max AQI 183, trend steady) ---
Outlook   : 2026-11-10: AQI 183, Severe
            2026-11-11: AQI 179, Severe
            2026-11-12: AQI 178, Severe
            2026-11-13: AQI 172, Severe
            2026-11-14: AQI 169, Moderate
            2026-11-15: AQI 178, Severe
            2026-11-16: AQI 178, Severe
Visibility: lowest 21.39 km
Public    : Wear an N95 mask outdoors. Avoid outdoor exercise. Keep windows closed.
Actions   :
  - Education department: cancel outdoor school activities and sports.
  - Environment department: increase patrols against crop-residue burning.
  - City administration: water sprinkling on main roads to settle dust.
  - Hospitals: prepare for more respiratory patients.

--- PESHAWAR : MODERATE (max AQI 151, trend falling) ---
Outlook   : 2026-11-10: AQI 151, Moderate
            2026-11-11: AQI 147, Moderate
            2026-11-12: AQI 136, Moderate
            2026-11-13: AQI 135, Moderate
            2026-11-14: AQI 136, Low
            2026-11-15: AQI 128, Low
            2026-11-16: AQI 128, Low
Visibility: lowest 21.97 km
Public    : Children, elderly and heart/lung patients should limit time outdoors.
Actions   :
  - Health department: issue advisory for sensitive groups.
  - Environment department: monitor crop-burning hotspots.

--- ISLAMABAD : MODERATE (max AQI 150, trend falling) ---
Outlook   : 2026-11-10: AQI 150, Moderate
            2026-11-11: AQI 146, Moderate
            2026-11-12: AQI 126, Low
            2026-11-13: AQI 128, Low
            2026-11-14: AQI 132, Low
            2026-11-15: AQI 130, Low
            2026-11-16: AQI 130, Low
Visibility: lowest 19.47 km
Public    : Children, elderly and heart/lung patients should limit time outdoors.
Actions   :
  - Health department: issue advisory for sensitive groups.
  - Environment department: monitor crop-burning hotspots.

--- KARACHI : LOW (max AQI 99, trend steady) ---
Outlook   : 2026-11-10: AQI 94, Low
            2026-11-11: AQI 90, Low
            2026-11-12: AQI 93, Low
            2026-11-13: AQI 94, Low
            2026-11-14: AQI 97, Low
            2026-11-15: AQI 95, Low
            2026-11-16: AQI 99, Low
Visibility: lowest 22.61 km
Public    : Air is acceptable. No special precautions.
Actions   :
  - No action needed.

AQI guide: 0-150 Low | 151-200 Moderate | 201-300 Severe | 301-500 Hazardous
```

## 7. Charts

![monthly_aqi_heatmap.png](figures/monthly_aqi_heatmap.png)

![model_comparison_rmse.png](figures/model_comparison_rmse.png)

![smog_accuracy_by_horizon.png](figures/smog_accuracy_by_horizon.png)

![actual_vs_predicted_aqi.png](figures/actual_vs_predicted_aqi.png)

![feature_importance_aqi.png](figures/feature_importance_aqi.png)

![smog_confusion_matrix.png](figures/smog_confusion_matrix.png)
