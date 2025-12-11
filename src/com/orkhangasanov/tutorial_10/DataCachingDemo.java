package com.orkhangasanov.tutorial_10;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataCachingDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DataCachingDemo")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("cities.csv");

        System.out.println("Data loaded. Starting heavy transformations...");

        // Apply expensive transformations
        Dataset<Row> highRated = df.filter("population > 1000000")
                .groupBy("country")
                .count();

        // Cache result for reuse
        highRated.cache();
        System.out.println("Data cached in memory.");

        // Trigger first action
        highRated.show();

        // Reuse cached data
        System.out.println("Using cached data again (should be faster)");
        highRated.show();

        spark.stop();
    }
}
