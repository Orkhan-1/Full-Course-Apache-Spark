package com.orkhangasanov.tutorial_8;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ReadCSVExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ReadCSVExample")
                .master("local[*]")
                .getOrCreate();

        // Read CSV file with header and infer schema
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("products.csv");

        df.show();
        df.printSchema();

        spark.stop();
    }
}

