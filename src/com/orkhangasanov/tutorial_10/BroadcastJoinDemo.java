package com.orkhangasanov.tutorial_10;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.broadcast;

public class BroadcastJoinDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("BroadcastJoinDemo")
                .master("local[*]")
                .getOrCreate();

        // Read cities data
        Dataset<Row> cities = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("cities.csv");

        System.out.println("Cities DataFrame schema:");
        cities.printSchema();
        System.out.println("\nCities data sample:");
        cities.show(5);

        // Read countries data
        Dataset<Row> countries = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("countries.csv");

        System.out.println("\nCountries DataFrame schema:");
        countries.printSchema();
        System.out.println("\nCountries data sample:");
        countries.show(5);

        // Rename continent column in countries DataFrame to avoid ambiguity
        Dataset<Row> countriesRenamed = countries
                .withColumnRenamed("continent", "country_continent")
                .withColumnRenamed("country", "country_code");

        System.out.println("\nRenamed Countries DataFrame schema:");
        countriesRenamed.printSchema();

        // Perform broadcast join with renamed columns
        Dataset<Row> joined = cities.join(
                broadcast(countriesRenamed),
                cities.col("country").equalTo(countriesRenamed.col("country_code"))
        );

        System.out.println("\nJoined DataFrame schema:");
        joined.printSchema();

        // Select columns
        joined.select(
                "city_name",
                "country_continent",
                "population"
        ).show(10);

        // Show execution plan to verify broadcast join
        System.out.println("\nExecution plan:");
        joined.explain();

        spark.stop();
    }
}