"""
train.py - STEP 4 of the pipeline (PySpark MLlib).

MODEL 1 (regression)     : AQI and visibility, 1 to 7 days ahead.
                           Linear Regression vs Random Forest vs Gradient-Boosted Trees.
MODEL 2 (classification) : smog level (Low / Moderate / Severe / Hazardous), 1 to 7 days ahead.
                           Logistic Regression vs Random Forest Classifier.

One model per target handles all 7 horizons ("horizon" is one of its features).

  * Time-based split: train on days before the test start date, test on the days after.
  * Hyperparameters tuned with k-fold cross-validation (CrossValidator).
  * Every model is compared with a baseline: "the value will stay the same as today".
  * Results are reported for every horizon (1..7 days); the best model is saved.

Outputs (reports/results/): regression_metrics.csv, classification_metrics.csv, best_models.csv,
  test_predictions.csv, smog_test_predictions.csv, feature_importance.csv
Saved models: models/aqi, models/visibility_km, models/smog_level
"""

import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as F

from src.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES, SMOG_LEVELS
from src.utils import save_csv, to_pandas

# target name -> (label column, column with today's value for the baseline, allowed range)
REGRESSION_TARGETS = {
    "aqi": ("aqi_target", "aqi", (0.0, 500.0)),
    "visibility_km": ("visibility_target", "visibility_km", (0.05, 30.0)),
}
BASELINE = "Baseline (same as today)"


def build_pipeline(model, scale):
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL_FEATURES]
    encoder = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL_FEATURES],
                            outputCols=[f"{c}_vec" for c in CATEGORICAL_FEATURES], handleInvalid="keep")
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES + [f"{c}_vec" for c in CATEGORICAL_FEATURES],
                                outputCol="raw_features" if scale else "features", handleInvalid="keep")
    stages = indexers + [encoder, assembler]
    if scale:
        stages.append(StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True))
    return Pipeline(stages=stages + [model])


def fit_model(pipeline, grid, evaluator, train, cfg):
    t = cfg["training"]
    if t["quick_run"]:
        return pipeline.fit(train, grid[0]), grid[0]
    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator,
                        numFolds=t["cv_folds"], seed=t["seed"], parallelism=t["parallelism"])
    cv_model = cv.fit(train)
    m = cv_model.avgMetrics
    best_i = int(np.argmax(m)) if evaluator.isLargerBetter() else int(np.argmin(m))
    return cv_model.bestModel, grid[best_i]


def params_text(param_map):
    return ", ".join(f"{p.name}={v}" for p, v in param_map.items()) or "default"


def feature_names(transformed_df):
    try:
        attrs = transformed_df.schema["features"].metadata["ml_attr"]["attrs"]
        names = {a["idx"]: a["name"].replace("city_vec_", "city = ").replace("t_season_vec_", "season = ")
                 for group in attrs.values() for a in group}
        return [names[i] for i in range(len(names))]
    except Exception:
        return None


def regression_candidates(cfg, label):
    g, seed = cfg["training"]["grids"], cfg["training"]["seed"]
    lr = LinearRegression(featuresCol="features", labelCol=label, maxIter=100)
    rf = RandomForestRegressor(featuresCol="features", labelCol=label, seed=seed)
    gbt = GBTRegressor(featuresCol="features", labelCol=label, seed=seed, stepSize=0.1)
    return [
        ("Linear Regression", lr, True, ParamGridBuilder().addGrid(lr.regParam, g["lr_reg_param"])
         .addGrid(lr.elasticNetParam, g["lr_elastic_net"]).build()),
        ("Random Forest", rf, False, ParamGridBuilder().addGrid(rf.numTrees, g["rf_num_trees"])
         .addGrid(rf.maxDepth, g["rf_max_depth"]).build()),
        ("Gradient-Boosted Trees", gbt, False, ParamGridBuilder().addGrid(gbt.maxIter, g["gbt_max_iter"])
         .addGrid(gbt.maxDepth, g["gbt_max_depth"]).build()),
    ]


def classification_candidates(cfg, label):
    g, seed = cfg["training"]["grids"], cfg["training"]["seed"]
    logreg = LogisticRegression(featuresCol="features", labelCol=label, family="multinomial", maxIter=100)
    rfc = RandomForestClassifier(featuresCol="features", labelCol=label, seed=seed)
    return [
        ("Logistic Regression", logreg, True,
         ParamGridBuilder().addGrid(logreg.regParam, g["logreg_reg_param"]).build()),
        ("Random Forest Classifier", rfc, False, ParamGridBuilder().addGrid(rfc.numTrees, g["rfc_num_trees"])
         .addGrid(rfc.maxDepth, g["rfc_max_depth"]).build()),
    ]


def split(df, label, cfg):
    start = F.lit(cfg["split"]["test_start_date"]).cast("date")
    data = df.filter(F.col(label).isNotNull())
    return data.filter(F.col("target_date") < start).cache(), data.filter(F.col("date") >= start).cache()


def per_horizon(pred, evaluators, horizons):
    """Evaluate predictions for every horizon separately."""
    out = {}
    for h in horizons:
        part = pred.filter(F.col("horizon") == float(h))
        out[h] = {m: round(e.evaluate(part), 4) for m, e in evaluators.items()}
    return out


# =============================================================================
def train_regression(df, cfg, logger):
    horizons = cfg["targets"]["horizons"]
    metrics_rows, best_rows, predictions, importances = [], [], [], []

    for target, (label, today_col, (low, high)) in REGRESSION_TARGETS.items():
        train, test = split(df, label, cfg)
        logger.info("[%s] train rows = %d, test rows = %d", target, train.count(), test.count())
        evs = {m: RegressionEvaluator(labelCol=label, predictionCol="prediction", metricName=m)
               for m in ["rmse", "mae", "r2"]}

        base = test.withColumn("prediction", F.col(today_col)).cache()
        for h, m in per_horizon(base, evs, horizons).items():
            metrics_rows.append({"target": target, "horizon_days": h, "model": BASELINE, "best_params": "-", **m})
        base_rmse = evs["rmse"].evaluate(base)
        logger.info("[%s] %-24s overall RMSE=%.2f", target, BASELINE, base_rmse)

        best = None
        for name, est, scale, grid in regression_candidates(cfg, label):
            start = time.time()
            model, params = fit_model(build_pipeline(est, scale), grid, evs["rmse"], train, cfg)
            pred = model.transform(test).withColumn(
                "prediction", F.least(F.greatest(F.col("prediction"), F.lit(low)), F.lit(high))).cache()
            overall = {m: e.evaluate(pred) for m, e in evs.items()}
            for h, m in per_horizon(pred, evs, horizons).items():
                metrics_rows.append({"target": target, "horizon_days": h, "model": name,
                                     "best_params": params_text(params), **m})
            logger.info("[%s] %-24s overall RMSE=%.2f MAE=%.2f R2=%.3f (%.0f s)", target, name,
                        overall["rmse"], overall["mae"], overall["r2"], time.time() - start)
            if name == "Random Forest":
                names = feature_names(pred)
                for i, v in enumerate(model.stages[-1].featureImportances.toArray()):
                    importances.append({"target": target, "feature": names[i] if names else f"feature_{i}",
                                        "importance": round(float(v), 5)})
            if best is None or overall["rmse"] < best["rmse"]:
                best = {"name": name, "model": model, "pred": pred, "rmse": overall["rmse"],
                        "mae": overall["mae"], "r2": overall["r2"]}

        path = str(Path(cfg["paths"]["models_dir"]) / target)
        best["model"].write().overwrite().save(path)
        best_rows.append({"target": target, "type": "regression", "best_model": best["name"],
                          "rmse": round(best["rmse"], 3), "mae": round(best["mae"], 3), "r2": round(best["r2"], 3),
                          "baseline_rmse": round(base_rmse, 3),
                          "error_reduction_vs_baseline_pct": round((1 - best["rmse"] / base_rmse) * 100, 1),
                          "model_path": path})
        logger.info("[%s] BEST = %s (RMSE %.2f vs baseline %.2f) saved -> %s",
                    target, best["name"], best["rmse"], base_rmse, path)

        p = to_pandas(best["pred"].select("date", "city", "horizon", F.col(label).alias("actual"), "prediction"))
        p["horizon_days"] = p.pop("horizon").astype(int)
        p["target"], p["model"] = target, best["name"]
        predictions.append(p)
        train.unpersist()
        test.unpersist()

    return metrics_rows, best_rows, predictions, importances


def train_classification(df, cfg, logger):
    horizons = cfg["targets"]["horizons"]
    label = "smog_label_target"
    metrics_rows, best_rows = [], []
    train, test = split(df, label, cfg)
    logger.info("[smog_level] train rows = %d, test rows = %d", train.count(), test.count())
    evs = {m: MulticlassClassificationEvaluator(labelCol=label, predictionCol="prediction", metricName=m)
           for m in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]}

    base = test.withColumn("prediction", F.col("smog_label_today")).cache()
    for h, m in per_horizon(base, evs, horizons).items():
        metrics_rows.append({"target": "smog_level", "horizon_days": h, "model": BASELINE, "best_params": "-", **m})
    base_acc = evs["accuracy"].evaluate(base)

    best = None
    for name, est, scale, grid in classification_candidates(cfg, label):
        start = time.time()
        model, params = fit_model(build_pipeline(est, scale), grid, evs["f1"], train, cfg)
        pred = model.transform(test).cache()
        overall = {m: e.evaluate(pred) for m, e in evs.items()}
        for h, m in per_horizon(pred, evs, horizons).items():
            metrics_rows.append({"target": "smog_level", "horizon_days": h, "model": name,
                                 "best_params": params_text(params), **m})
        logger.info("[smog_level] %-24s overall accuracy=%.3f F1=%.3f (%.0f s)",
                    name, overall["accuracy"], overall["f1"], time.time() - start)
        if best is None or overall["f1"] > best["f1"]:
            best = {"name": name, "model": model, "pred": pred, **overall}

    path = str(Path(cfg["paths"]["models_dir"]) / "smog_level")
    best["model"].write().overwrite().save(path)
    best_rows.append({"target": "smog_level", "type": "classification", "best_model": best["name"],
                      "accuracy": round(best["accuracy"], 3), "f1": round(best["f1"], 3),
                      "baseline_accuracy": round(base_acc, 3), "model_path": path})
    logger.info("[smog_level] BEST = %s (accuracy %.3f vs baseline %.3f) saved -> %s",
                best["name"], best["accuracy"], base_acc, path)

    p = to_pandas(best["pred"].select("date", "city", "horizon", F.col(label).alias("a"), "prediction"))
    p["actual"] = p["a"].astype(int).map(dict(enumerate(SMOG_LEVELS)))
    p["predicted"] = p["prediction"].astype(int).map(dict(enumerate(SMOG_LEVELS)))
    p["horizon_days"], p["model"] = p["horizon"].astype(int), best["name"]
    train.unpersist()
    test.unpersist()
    return metrics_rows, best_rows, p[["date", "city", "actual", "predicted", "horizon_days", "model"]]


def run(spark, cfg, logger):
    df = spark.read.parquet(cfg["paths"]["features_data"]).cache()
    mode = "QUICK RUN (no cross-validation)" if cfg["training"]["quick_run"] else \
        f"{cfg['training']['cv_folds']}-fold cross-validation"
    logger.info("Training mode: %s. Test period starts %s", mode, cfg["split"]["test_start_date"])

    logger.info("---- MODEL 1: AQI and visibility (regression) ----")
    reg_metrics, reg_best, reg_preds, importances = train_regression(df, cfg, logger)
    logger.info("---- MODEL 2: smog level (classification) ----")
    cls_metrics, cls_best, cls_preds = train_classification(df, cfg, logger)

    save_csv(pd.DataFrame(reg_metrics), cfg, "regression_metrics.csv", logger)
    save_csv(pd.DataFrame(cls_metrics), cfg, "classification_metrics.csv", logger)
    save_csv(pd.DataFrame(reg_best + cls_best), cfg, "best_models.csv", logger)
    save_csv(pd.concat(reg_preds, ignore_index=True), cfg, "test_predictions.csv", logger)
    save_csv(cls_preds, cfg, "smog_test_predictions.csv", logger)
    imp = pd.DataFrame(importances).sort_values(["target", "importance"], ascending=[True, False])
    save_csv(imp, cfg, "feature_importance.csv", logger)
    df.unpersist()
