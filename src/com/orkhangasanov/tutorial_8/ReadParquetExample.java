package com.orkhangasanov.tutorial_8;

// File: ReadParquetExample.java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ReadParquetExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ReadParquetExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .parquet("products.parquet");

        df.show();
        df.printSchema();

        spark.stop();
    }
}
