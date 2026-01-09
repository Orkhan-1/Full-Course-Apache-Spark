package com.orkhangasanov.tutorial_11;

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
import org.apache.spark.ml.PipelineStage;
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
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .config("spark.kryo.registrationRequired", "false")
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

        /*
            ┌───────┬────────────┬────────────────┐
            │ House │  Location  │ locationIndex  │
            ├───────┼────────────┼────────────────┤
            │   1   │ "New York" │       2        │
            │   2   │  "Boston"  │       0        │
            │   3   │ "Chicago"  │       1        │
            │   4   │ "New York" │       2        │
            │   5   │  "Boston"  │       0        │
            │   6   │ "Chicago"  │       1        │
            └───────┴────────────┴────────────────┘
        */


        // STEP 3 — Combine features into a single vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                        "bedrooms", "bathrooms", "size_sqft", "year_built", "locationIndex"
                })
                .setOutputCol("features");

    /*

            ┌──────────┬───────────┬──────────┬────────────┬──────────────┐
            │ bedrooms │ bathrooms │ size_sqft│ year_built │locationIndex │
            ├──────────┼───────────┼──────────┼────────────┼──────────────┤
            │    3     │    2.5    │   2000   │    2010    │      2       │ ← House 1
            │    4     │    3.0    │   2500   │    2005    │      0       │ ← House 2
            │    2     │    1.0    │   1200   │    1995    │      1       │ ← House 3
            └──────────┴───────────┴──────────┴────────────┴──────────────┘

            ┌─────────────────────────────────────────────────────────────┐
            │                         features                            │
            ├─────────────────────────────────────────────────────────────┤
            │               [3.0, 2.5, 2000.0, 2010.0, 2.0]               │
            │               [4.0, 3.0, 2500.0, 2005.0, 0.0]               │
            │               [2.0, 1.0, 1200.0, 1995.0, 1.0]               │
            └─────────────────────────────────────────────────────────────┘

            Step 1: Gather all values
            [3.0, 2.5, 2000.0, 2010.0, 2.0]

            Step 2: Create vector
            features = [3.0, 2.5, 2000.0, 2010.0, 2.0]

            Step 3: ML algorithm uses it
            prediction = dot_product(features, weights) + bias = 3.0*w1 + 2.5*w2 + 2000.0*w3
    */


        // STEP 4 — Define regression model
        LinearRegression lr = new LinearRegression()
                .setLabelCol("price")
                .setFeaturesCol("features");

        // STEP 5 — Build pipeline (feature + model)
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{
                        locationIndexer,  // Step 1: Encode text to numbers
                        assembler,        // Step 2: Combine features into vector
                        lr                // Step 3: Train linear regression model
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

