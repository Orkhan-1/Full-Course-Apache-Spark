package com.orkhangasanov.tutorial_10;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PartitioningDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("PartitioningDemo")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("cities.csv");

        System.out.println("Initial partitions: " + df.rdd().partitions().length);

        // Repartition by continent
        Dataset<Row> repartitioned = df.repartition(df.col("continent"));

        System.out.println("Repartitioned by continent: " + repartitioned.rdd().partitions().length);

        repartitioned.groupBy("continent").count().show();

        spark.stop();
    }
}
