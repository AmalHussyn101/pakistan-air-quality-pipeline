"""
evaluate.py - STEP 6 of the pipeline (reports).

Reads the CSV results made by the earlier steps and creates:
  PNG charts  -> reports/figures/
  A written summary report -> reports/summary_report.md

Charts:
  1. model_comparison_rmse.png       - error of every model vs the baseline (AQI and visibility)
  2. smog_accuracy_by_horizon.png    - smog-level accuracy for 1, 2 and 3 days ahead
  3. actual_vs_predicted_aqi.png     - real vs predicted AQI (next day) in the test period, by city
  4. feature_importance_aqi.png      - which features matter most for AQI
  5. smog_confusion_matrix.png       - which smog levels the model mixes up (next day)
  6. monthly_aqi_heatmap.png         - average AQI per city per month (when and where smog happens)
"""

from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")            # save charts to files, never open a window
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np               # noqa: E402
import pandas as pd              # noqa: E402

# Chart colors (colorblind-checked categorical order; grey = baseline)
MODEL_COLORS = {
    "Baseline (same as today)": "#a3a29d",
    "Linear Regression": "#2a78d6",
    "Random Forest": "#eb6834",
    "Gradient-Boosted Trees": "#1baf7a",
    "Logistic Regression": "#2a78d6",
    "Random Forest Classifier": "#eb6834",
}
CITY_ORDER = ["Lahore", "Faisalabad", "Multan", "Peshawar", "Islamabad", "Karachi"]
SMOG_LEVELS = ["Low", "Moderate", "Severe", "Hazardous"]
TEXT, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
BLUES = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

plt.rcParams.update({
    "font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.titlecolor": TEXT, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.axisbelow": True, "legend.frameon": False, "figure.dpi": 110,
})


def _save(fig, cfg, name, logger):
    out = Path(cfg["paths"]["figures_dir"]) / name
    fig.tight_layout()
    fig.savefig(out, dpi=150, facecolor="white")
    plt.close(fig)
    logger.info("Saved chart %s", out)
    return out


def _grouped_bars(ax, table, value_col, models, decimals):
    horizons = sorted(table["horizon_days"].unique())
    width = 0.8 / len(models)
    for i, m in enumerate(models):
        sub = table[table["model"] == m].set_index("horizon_days").reindex(horizons)
        x = np.arange(len(horizons)) + (i - (len(models) - 1) / 2) * width
        bars = ax.bar(x, sub[value_col], width * 0.92, color=MODEL_COLORS.get(m, "#6250d6"), label=m)
        ax.bar_label(bars, fmt=f"%.{decimals}f", fontsize=7, color=MUTED, padding=2)
    ax.set_xticks(np.arange(len(horizons)))
    ax.set_xticklabels([f"{h} day{'s' if h > 1 else ''} ahead" for h in horizons])
    ax.grid(axis="x", visible=False)


# =============================================================================
# Charts
# =============================================================================
def chart_model_comparison(reg, cfg, logger):
    targets = list(reg["target"].unique())
    models = [m for m in MODEL_COLORS if m in set(reg["model"])]
    fig, axes = plt.subplots(1, len(targets), figsize=(6.2 * len(targets), 4.4))
    axes = np.atleast_1d(axes)
    for ax, t in zip(axes, targets):
        _grouped_bars(ax, reg[reg["target"] == t], "rmse", models, 1 if t == "aqi" else 2)
        unit = "AQI points" if t == "aqi" else "km"
        ax.set_title(f"{'AQI' if t == 'aqi' else 'Visibility'}: prediction error (RMSE, lower is better)")
        ax.set_ylabel(f"RMSE ({unit})")
    axes[0].legend(loc="upper left", fontsize=8)
    return _save(fig, cfg, "model_comparison_rmse.png", logger)


def chart_smog_accuracy(cls, cfg, logger):
    models = [m for m in MODEL_COLORS if m in set(cls["model"])]
    fig, ax = plt.subplots(figsize=(7.5, 4.4))
    _grouped_bars(ax, cls, "accuracy", models, 2)
    ax.set_ylim(0, 1.08)
    ax.set_title("Smog level: accuracy (higher is better)")
    ax.set_ylabel("Accuracy")
    ax.legend(loc="lower left", fontsize=8)
    return _save(fig, cfg, "smog_accuracy_by_horizon.png", logger)


def chart_actual_vs_predicted(preds, cfg, logger):
    p = preds[(preds["target"] == "aqi") & (preds["horizon_days"] == 1)].copy()
    p["date"] = pd.to_datetime(p["date"]) + pd.Timedelta(days=1)       # the day being predicted
    cities = [c for c in CITY_ORDER if c in set(p["city"])]
    fig, axes = plt.subplots(len(cities), 1, figsize=(11, 2.0 * len(cities)), sharex=True, sharey=True)
    axes = np.atleast_1d(axes)
    for ax, city in zip(axes, cities):
        c = p[p["city"] == city].sort_values("date")
        ax.plot(c["date"], c["actual"], color="#2a78d6", linewidth=1.6, label="Actual AQI")
        ax.plot(c["date"], c["prediction"], color="#eb6834", linewidth=1.6, label="Predicted AQI (1 day ahead)")
        ax.set_ylabel(city, rotation=0, ha="right", va="center", color=TEXT)
        ax.set_ylim(0, 520)
        ax.grid(axis="x", visible=False)
    axes[0].set_title(f"Actual vs predicted AQI in the test period - best model: {p['model'].iloc[0]}",
                      loc="left", pad=22)
    axes[0].legend(loc="lower left", bbox_to_anchor=(0, 1.0), fontsize=8, ncol=2)
    return _save(fig, cfg, "actual_vs_predicted_aqi.png", logger)


def chart_feature_importance(imp, cfg, logger, top=15):
    t = imp[imp["target"] == "aqi"].nlargest(top, "importance").iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 5.5))
    bars = ax.barh(t["feature"], t["importance"], color="#2a78d6", height=0.7)
    ax.bar_label(bars, fmt="%.3f", fontsize=8, color=MUTED, padding=3)
    ax.set_title("Top features for predicting next-day AQI (Random Forest)")
    ax.set_xlabel("Importance (share of the model's decisions)")
    ax.grid(axis="y", visible=False)
    return _save(fig, cfg, "feature_importance_aqi.png", logger)


def chart_confusion(smog_preds, cfg, logger):
    p = smog_preds[smog_preds["horizon_days"] == 1]
    m = pd.crosstab(p["actual"], p["predicted"]).reindex(index=SMOG_LEVELS, columns=SMOG_LEVELS, fill_value=0)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.imshow(m.values, cmap=matplotlib.colors.LinearSegmentedColormap.from_list("b", BLUES))
    ax.grid(False)
    for i in range(4):
        for j in range(4):
            v = m.values[i, j]
            ax.text(j, i, str(v), ha="center", va="center", fontsize=11,
                    color="white" if v > m.values.max() * 0.55 else TEXT)
    ax.set_xticks(range(4), SMOG_LEVELS)
    ax.set_yticks(range(4), SMOG_LEVELS)
    ax.set_xlabel("Predicted smog level")
    ax.set_ylabel("Actual smog level")
    ax.set_title(f"Smog level next day: confusion matrix\n({p['model'].iloc[0]})")
    return _save(fig, cfg, "smog_confusion_matrix.png", logger)


def chart_monthly_heatmap(monthly, cfg, logger):
    m = monthly.pivot(index="city", columns="month", values="avg_aqi")
    m = m.reindex([c for c in CITY_ORDER if c in m.index])
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.imshow(m.values, cmap=matplotlib.colors.LinearSegmentedColormap.from_list("b", BLUES), aspect="auto")
    ax.grid(False)
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            v = m.values[i, j]
            ax.text(j, i, f"{v:.0f}", ha="center", va="center", fontsize=8,
                    color="white" if v > np.nanmax(m.values) * 0.6 else TEXT)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    ax.set_xticks(range(m.shape[1]), [months[int(c) - 1] for c in m.columns])
    ax.set_yticks(range(m.shape[0]), m.index)
    ax.set_title("Average AQI by city and month (2023-2026): smog peaks in November")
    return _save(fig, cfg, "monthly_aqi_heatmap.png", logger)


# =============================================================================
# Written report
# =============================================================================
def md_table(df):
    """Turn a small pandas table into a Markdown table (no extra library needed)."""
    cols = list(df.columns)
    lines = ["| " + " | ".join(str(c) for c in cols) + " |", "|" + "---|" * len(cols)]
    for _, r in df.iterrows():
        lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    return "\n".join(lines)


def write_report(cfg, logger, reg, cls, best, imp, quality, city, corr, forecast):
    test_start = cfg["split"]["test_start_date"]
    lines = [f"# {cfg['project']['name']}", "",
             f"_Report generated automatically by `run_pipeline.py` on {datetime.now():%Y-%m-%d %H:%M}._", "",
             "> The data is synthetic, calibrated to IQAir annual PM2.5 averages (2023-2025) and known "
             "seasonal smog, crop-burning and monsoon patterns in Pakistan.", "",
             "## 1. Data quality (cleaning step)", "", md_table(quality), "",
             "## 2. City overview", "", md_table(city), ""]

    worst = city.iloc[0]
    lines += [f"**Insight:** {worst['city']} has the worst air (average AQI {worst['avg_aqi']}, "
              f"{worst['hazardous_days']} hazardous days). Karachi and Islamabad are the cleanest.", ""]

    lines += ["## 3. What drives AQI?", "",
              "Correlation of each measured column with AQI (closer to +1 or -1 = stronger link):", "",
              md_table(corr.head(10)), ""]
    if imp is not None and len(imp):
        top = imp[imp["target"] == "aqi"].nlargest(10, "importance")[["feature", "importance"]]
        lines += ["Top 10 features used by the Random Forest for next-day AQI:", "", md_table(top), ""]

    lines += [f"## 4. Model results (test period: {test_start} onwards)", "",
              "**Baseline** = 'the value will be the same as today'. A model is useful only if it beats it.", "",
              "### Model 1: AQI and visibility (regression)", "",
              md_table(reg[["target", "horizon_days", "model", "rmse", "mae", "r2", "best_params"]]), ""]

    improvements = []
    for (t, h), g in reg.groupby(["target", "horizon_days"]):
        base = g[g["model"].str.startswith("Baseline")]["rmse"].iloc[0]
        b = g[~g["model"].str.startswith("Baseline")].nsmallest(1, "rmse").iloc[0]
        improvements.append({"target": t, "horizon_days": h, "best_model": b["model"], "best_rmse": b["rmse"],
                             "baseline_rmse": base,
                             "error_reduction_vs_baseline": f"{(1 - b['rmse'] / base) * 100:.1f}%"})
    lines += ["**Best model per target:**", "", md_table(pd.DataFrame(improvements)), "",
              "### Model 2: smog level (classification)", "",
              md_table(cls[["horizon_days", "model", "accuracy", "f1", "weightedPrecision", "weightedRecall"]]), ""]

    if forecast is not None:
        f = forecast[["city", "forecast_date", "days_ahead", "predicted_aqi", "predicted_visibility_km",
                      "predicted_smog_level", "health_advice"]]
        lines += ["## 5. Latest forecast (next 3 days)", "", md_table(f), ""]

    bulletins = sorted(Path(cfg["paths"].get("alerts_dir", "reports/alerts")).glob("smog_bulletin_*.txt"))
    if bulletins:
        lines += ["## 6. Smog early-warning bulletin (latest)", "", "```",
                  bulletins[-1].read_text(encoding="utf-8"), "```", ""]

    lines += ["## 7. Charts", ""]
    for name in ["monthly_aqi_heatmap.png", "model_comparison_rmse.png", "smog_accuracy_by_horizon.png",
                 "actual_vs_predicted_aqi.png", "feature_importance_aqi.png", "smog_confusion_matrix.png"]:
        lines += [f"![{name}](figures/{name})", ""]

    out = Path(cfg["paths"]["report_file"])
    out.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Saved report %s", out)


def run(cfg, logger):
    r = Path(cfg["paths"]["results_dir"])
    read = lambda name: pd.read_csv(r / name) if (r / name).exists() else None  # noqa: E731

    reg, cls, best = read("regression_metrics.csv"), read("classification_metrics.csv"), read("best_models.csv")
    preds, smog_preds = read("test_predictions.csv"), read("smog_test_predictions.csv")
    imp, monthly = read("feature_importance.csv"), read("city_monthly_summary.csv")
    quality, city, corr = read("data_quality_report.csv"), read("city_summary.csv"), read("correlation_with_aqi.csv")
    forecast = read("latest_forecast.csv")

    if monthly is not None:
        chart_monthly_heatmap(monthly, cfg, logger)
    if reg is None or cls is None:
        logger.warning("No model results found. Run the 'train' step first.")
        return
    chart_model_comparison(reg, cfg, logger)
    chart_smog_accuracy(cls, cfg, logger)
    chart_actual_vs_predicted(preds, cfg, logger)
    if imp is not None and len(imp):
        chart_feature_importance(imp, cfg, logger)
    chart_confusion(smog_preds, cfg, logger)
    write_report(cfg, logger, reg, cls, best, imp, quality, city, corr, forecast)
