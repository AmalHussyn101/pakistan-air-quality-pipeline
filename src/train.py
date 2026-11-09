"""
train.py - STEP 4 of the pipeline (PySpark MLlib).

MODEL 1 (regression)     : predicts AQI and visibility (numbers) 1, 2 and 3 days ahead.
                           Compares Linear Regression, Random Forest and Gradient-Boosted Trees.
MODEL 2 (classification) : predicts the smog level (Low / Moderate / Severe / Hazardous).
                           Compares Logistic Regression and Random Forest Classifier.

For every target:
  * Train on data BEFORE the test start date, test on data AFTER it (time-based split).
  * Tune hyperparameters with k-fold cross-validation (CrossValidator).
  * Compare with a simple baseline: "tomorrow will be the same as today" (persistence).
    A model is only useful if it beats this baseline.
  * Keep and save the best model.

Outputs (reports/results/):
  regression_metrics.csv, classification_metrics.csv, best_models.csv,
  test_predictions.csv, smog_test_predictions.csv, feature_importance.csv
Saved models: models/<target>_h<horizon>/
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

# Allowed range of each predicted number (a prediction outside it is cut to the edge)
VALID_RANGE = {"aqi": (0.0, 500.0), "visibility_km": (0.05, 20.0)}


# =============================================================================
# Building blocks
# =============================================================================
def build_pipeline(model, scale):
    """city/season text -> numbers -> one feature vector -> (scaling) -> model"""
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
                for c in CATEGORICAL_FEATURES]
    encoder = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL_FEATURES],
                            outputCols=[f"{c}_vec" for c in CATEGORICAL_FEATURES],
                            handleInvalid="keep")
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES + [f"{c}_vec" for c in CATEGORICAL_FEATURES],
                                outputCol="raw_features" if scale else "features",
                                handleInvalid="keep")
    stages = indexers + [encoder, assembler]
    if scale:   # linear models work better when all features have a similar scale
        stages.append(StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True))
    stages.append(model)
    return Pipeline(stages=stages)


def fit_model(pipeline, grid, evaluator, train, cfg):
    """Quick run: fit once with the first settings. Normal run: cross-validation over the grid."""
    t = cfg["training"]
    if t["quick_run"]:
        return pipeline.fit(train, grid[0]), grid[0]
    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator,
                        numFolds=t["cv_folds"], seed=t["seed"], parallelism=t["parallelism"])
    cv_model = cv.fit(train)
    metrics = cv_model.avgMetrics
    best_i = int(np.argmax(metrics)) if evaluator.isLargerBetter() else int(np.argmin(metrics))
    return cv_model.bestModel, grid[best_i]


def params_text(param_map):
    return ", ".join(f"{p.name}={v}" for p, v in param_map.items()) or "default"


def feature_names(transformed_df):
    """Read the names of every position in the 'features' vector (from Spark metadata)."""
    try:
        attrs = transformed_df.schema["features"].metadata["ml_attr"]["attrs"]
        names = {}
        for group in attrs.values():
            for a in group:
                names[a["idx"]] = a["name"].replace("city_vec_", "city = ").replace("season_vec_", "season = ")
        return [names[i] for i in range(len(names))]
    except Exception:
        return None


def regression_candidates(cfg, label):
    g, seed = cfg["training"]["grids"], cfg["training"]["seed"]
    lr = LinearRegression(featuresCol="features", labelCol=label, maxIter=100)
    rf = RandomForestRegressor(featuresCol="features", labelCol=label, seed=seed)
    gbt = GBTRegressor(featuresCol="features", labelCol=label, seed=seed, stepSize=0.1)
    return [
        ("Linear Regression", lr, True,
         ParamGridBuilder().addGrid(lr.regParam, g["lr_reg_param"])
                           .addGrid(lr.elasticNetParam, g["lr_elastic_net"]).build()),
        ("Random Forest", rf, False,
         ParamGridBuilder().addGrid(rf.numTrees, g["rf_num_trees"])
                           .addGrid(rf.maxDepth, g["rf_max_depth"]).build()),
        ("Gradient-Boosted Trees", gbt, False,
         ParamGridBuilder().addGrid(gbt.maxIter, g["gbt_max_iter"])
                           .addGrid(gbt.maxDepth, g["gbt_max_depth"]).build()),
    ]


def classification_candidates(cfg, label):
    g, seed = cfg["training"]["grids"], cfg["training"]["seed"]
    logreg = LogisticRegression(featuresCol="features", labelCol=label, family="multinomial", maxIter=100)
    rfc = RandomForestClassifier(featuresCol="features", labelCol=label, seed=seed)
    return [
        ("Logistic Regression", logreg, True,
         ParamGridBuilder().addGrid(logreg.regParam, g["logreg_reg_param"]).build()),
        ("Random Forest Classifier", rfc, False,
         ParamGridBuilder().addGrid(rfc.numTrees, g["rfc_num_trees"])
                           .addGrid(rfc.maxDepth, g["rfc_max_depth"]).build()),
    ]


def split(df, label, h, cfg):
    """Time-based split. Training rows must have their target day before the test period."""
    test_start = cfg["split"]["test_start_date"]
    data = df.filter(F.col(label).isNotNull())
    train = data.filter(F.col(f"target_date_h{h}") < F.lit(test_start).cast("date")).cache()
    test = data.filter(F.col("date") >= F.lit(test_start).cast("date")).cache()
    return train, test


# =============================================================================
# MODEL 1: regression (AQI and visibility)
# =============================================================================
def train_regression(df, cfg, logger):
    horizons = cfg["targets"]["horizons"]
    models_dir = cfg["paths"]["models_dir"]
    metrics_rows, best_rows, predictions, importances = [], [], [], []

    for target in cfg["targets"]["regression"]:
        low, high = VALID_RANGE[target]
        for h in horizons:
            label = f"{target}_h{h}"
            train, test = split(df, label, h, cfg)
            logger.info("[%s] train rows = %d, test rows = %d", label, train.count(), test.count())
            evals = {m: RegressionEvaluator(labelCol=label, predictionCol="prediction", metricName=m)
                     for m in ["rmse", "mae", "r2"]}

            # Baseline: "the value will stay the same as today"
            base = test.withColumn("prediction", F.col(target).cast("double"))
            row = {"target": target, "horizon_days": h, "model": "Baseline (same as today)", "best_params": "-"}
            row.update({m: round(e.evaluate(base), 3) for m, e in evals.items()})
            metrics_rows.append(row)
            logger.info("[%s] %-24s RMSE=%.2f MAE=%.2f R2=%.3f", label, row["model"], row["rmse"], row["mae"], row["r2"])

            best = None
            for name, estimator, scale, grid in regression_candidates(cfg, label):
                start = time.time()
                model, best_params = fit_model(build_pipeline(estimator, scale), grid, evals["rmse"], train, cfg)
                pred = model.transform(test).withColumn(
                    "prediction", F.least(F.greatest(F.col("prediction"), F.lit(low)), F.lit(high)))
                row = {"target": target, "horizon_days": h, "model": name, "best_params": params_text(best_params)}
                row.update({m: round(e.evaluate(pred), 3) for m, e in evals.items()})
                metrics_rows.append(row)
                logger.info("[%s] %-24s RMSE=%.2f MAE=%.2f R2=%.3f (%.0f s)",
                            label, name, row["rmse"], row["mae"], row["r2"], time.time() - start)

                if best is None or row["rmse"] < best["rmse"]:
                    best = {"name": name, "model": model, "pred": pred, "rmse": row["rmse"]}

                # Which features matter most? (from the Random Forest, 1 day ahead)
                if name == "Random Forest" and h == horizons[0]:
                    names = feature_names(pred)
                    values = model.stages[-1].featureImportances.toArray()
                    for i, v in enumerate(values):
                        importances.append({"target": target, "feature": names[i] if names else f"feature_{i}",
                                            "importance": round(float(v), 5)})

            # Save the winner
            path = str(Path(models_dir) / label)
            best["model"].write().overwrite().save(path)
            best_rows.append({"target": target, "horizon_days": h, "type": "regression",
                              "best_model": best["name"], "rmse": best["rmse"], "model_path": path})
            logger.info("[%s] BEST = %s (RMSE %.2f) saved -> %s", label, best["name"], best["rmse"], path)

            p = to_pandas(best["pred"].select("date", "city", F.col(label).alias("actual"), "prediction"))
            p["target"], p["horizon_days"], p["model"] = target, h, best["name"]
            predictions.append(p)
            train.unpersist()
            test.unpersist()

    return metrics_rows, best_rows, predictions, importances


# =============================================================================
# MODEL 2: classification (smog level)
# =============================================================================
def train_classification(df, cfg, logger):
    horizons = cfg["targets"]["horizons"]
    models_dir = cfg["paths"]["models_dir"]
    metrics_rows, best_rows, predictions = [], [], []

    for h in horizons:
        label = f"smog_label_h{h}"
        train, test = split(df, label, h, cfg)
        logger.info("[smog_level_h%d] train rows = %d, test rows = %d", h, train.count(), test.count())
        evals = {m: MulticlassClassificationEvaluator(labelCol=label, predictionCol="prediction", metricName=m)
                 for m in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]}

        base = test.withColumn("prediction", F.col("smog_label").cast("double"))
        row = {"target": "smog_level", "horizon_days": h, "model": "Baseline (same as today)", "best_params": "-"}
        row.update({m: round(e.evaluate(base), 3) for m, e in evals.items()})
        metrics_rows.append(row)
        logger.info("[smog_level_h%d] %-24s accuracy=%.3f F1=%.3f", h, row["model"], row["accuracy"], row["f1"])

        best = None
        for name, estimator, scale, grid in classification_candidates(cfg, label):
            start = time.time()
            model, best_params = fit_model(build_pipeline(estimator, scale), grid, evals["f1"], train, cfg)
            pred = model.transform(test)
            row = {"target": "smog_level", "horizon_days": h, "model": name, "best_params": params_text(best_params)}
            row.update({m: round(e.evaluate(pred), 3) for m, e in evals.items()})
            metrics_rows.append(row)
            logger.info("[smog_level_h%d] %-24s accuracy=%.3f F1=%.3f (%.0f s)",
                        h, name, row["accuracy"], row["f1"], time.time() - start)
            if best is None or row["f1"] > best["f1"]:
                best = {"name": name, "model": model, "pred": pred, "f1": row["f1"], "accuracy": row["accuracy"]}

        path = str(Path(models_dir) / f"smog_level_h{h}")
        best["model"].write().overwrite().save(path)
        best_rows.append({"target": "smog_level", "horizon_days": h, "type": "classification",
                          "best_model": best["name"], "accuracy": best["accuracy"], "f1": best["f1"],
                          "model_path": path})
        logger.info("[smog_level_h%d] BEST = %s (F1 %.3f) saved -> %s", h, best["name"], best["f1"], path)

        p = to_pandas(best["pred"].select("date", "city", F.col(label).alias("actual_label"), "prediction"))
        p["actual"] = p["actual_label"].astype(int).map(dict(enumerate(SMOG_LEVELS)))
        p["predicted"] = p["prediction"].astype(int).map(dict(enumerate(SMOG_LEVELS)))
        p["horizon_days"], p["model"] = h, best["name"]
        predictions.append(p[["date", "city", "actual", "predicted", "horizon_days", "model"]])
        train.unpersist()
        test.unpersist()

    return metrics_rows, best_rows, predictions


# =============================================================================
# Run both
# =============================================================================
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
    save_csv(pd.concat(cls_preds, ignore_index=True), cfg, "smog_test_predictions.csv", logger)
    imp = pd.DataFrame(importances).sort_values(["target", "importance"], ascending=[True, False])
    save_csv(imp, cfg, "feature_importance.csv", logger)
    df.unpersist()
