package com.orkhangasanov.tutorial_10;

/*

Goal: Predict house prices using Linear Regression in Spark.

──────────────────────────────────────────────────────────────────────────
Steps:
1) Load dataset (CSV)
2) Encode categorical feature (location)
3) Assemble numeric + encoded features
4) Train a Linear Regression model
5) Evaluate with RMSE (Root Mean Square Error)
6) Show predictions
──────────────────────────────────────────────────────────────────────────
*/

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePricePrediction {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("HousePricePrediction")
                .master("local[*]")
                .getOrCreate();

        // STEP 1 — Load CSV data
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("house_prices.csv");

        System.out.println("Data loaded successfully!");
        data.show();

        // STEP 2 — Encode categorical column (location)
        StringIndexer locationIndexer = new StringIndexer()
                .setInputCol("location")
                .setOutputCol("locationIndex");

        // STEP 3 — Combine features into a single vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                        "bedrooms", "bathrooms", "size_sqft", "year_built", "locationIndex"
                })
                .setOutputCol("features");

        // STEP 4 — Define regression model
        LinearRegression lr = new LinearRegression()
                .setLabelCol("price")
                .setFeaturesCol("features");

        // STEP 5 — Build pipeline (feature + model)
        Pipeline pipeline = new Pipeline()
                .setStages(new org.apache.spark.ml.PipelineStage[]{
                        locationIndexer, assembler, lr
                });

        // STEP 6 — Train and transform
        Dataset<Row> predictions = pipeline.fit(data).transform(data);

        System.out.println("Model trained successfully! Showing predictions...");
        predictions.select("bedrooms", "bathrooms", "location", "price", "prediction")
                .show(10, false);

        // STEP 7 — Evaluate model accuracy
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("price")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE): " + rmse);

        spark.stop();
    }
}

