package com.orkhangasanov.tutorial_8;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriteDataExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("WriteDataExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("products.csv");

        // Write to Parquet
        df.write().mode("overwrite").parquet("output/products_parquet");

        // Write to JSON
        df.write().mode("overwrite").json("output/products_json");

        // Write to CSV
        df.write().mode("overwrite").option("header", "true").csv("output/products_csv");

        spark.stop();
    }
}
